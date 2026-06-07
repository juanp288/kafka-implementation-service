# Step by Step

---

## Paso 1 — Infraestructura base y conectividad Kafka

### Lo que se creó / modificó

| Archivo | Acción |
|---|---|
| `docker-compose.yml` | Zookeeper, Kafka, Postgres (una sola DB con dos schemas), ambos servicios |
| `order-service/prisma/schema.prisma` | Modelo `Order`, schema `orders` |
| `inventory-service/prisma/schema.prisma` | Modelos `Seat` + `ProcessedEvent`, schema `inventory` |
| `*/src/prisma/prisma.service.ts` | `PrismaService` estándar de NestJS (en ambos servicios) |
| `*/src/app.module.ts` | `ConfigModule` + `ClientsModule` (Kafka producer) en order; solo `ConfigModule` en inventory |
| `*/src/main.ts` | `inventory-service` arranca como hybrid app (HTTP + Kafka consumer) |
| `*/src/app.controller.ts` | `GET /test` en order emite `test.ping`; `@EventPattern('test.ping')` en inventory lo recibe |
| `*/Dockerfile` | Build de producción + `prisma db push` al arrancar |
| `*/.dockerignore` | Excluye `node_modules` y `dist` |

### Cómo probarlo

```bash
# Levantar toda la infraestructura
docker compose up --build

# Cuando ambos servicios estén listos
curl http://localhost:3001/test
# → { "sent": true }

# En los logs de inventory-service debe aparecer:
# [inventory-service] test.ping received: { ts: 1234567890, from: 'order-service' }
```

### Desarrollo local (solo infra en Docker)

```bash
docker compose up zookeeper kafka postgres -d

# En terminales separadas:
cd order-service && npm run start:dev
cd inventory-service && npm run start:dev
```

---

## Paso 2 — Happy path completo

### Flujo

```
POST /orders
    │
    ├─ Crea Order { status: PENDING, sagaStep: RESERVING_SEATS }
    └─ Kafka → RESERVE_SEATS ──────────────────────────► inventory-service
                                                              │
                                                   SeatService.reserveSeats()
                                                   Marca N asientos como reserved
                                                              │
                                                   Kafka → SEATS_RESERVED ──► order-service
                                                                                    │
                                                               PaymentMock.processPayment() → true
                                                               Order → CONFIRMED
```

### Archivos nuevos

| Archivo | Rol |
|---|---|
| `order-service/src/kafka/kafka.module.ts` | Configura `ClientKafka` y lo exporta para inyección |
| `order-service/src/orders/saga.orchestrator.ts` | Coordina los pasos: crea orden, emite comando, procesa respuesta |
| `order-service/src/orders/orders.controller.ts` | `POST /orders` + `GET /orders/:id` + `@EventPattern('SEATS_RESERVED')` |
| `order-service/src/orders/payment.mock.ts` | Siempre retorna `true` (se cambia en Paso 4) |
| `inventory-service/src/inventory/seat.service.ts` | Reserva asientos + auto-seed de 50 asientos al arrancar |
| `inventory-service/src/inventory/inventory.consumer.ts` | `@EventPattern('RESERVE_SEATS')` |

### Cómo probarlo

```bash
# Crear una orden (la SAGA arranca)
curl -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"eventName": "Test Event", "seatCount": 2}'
# → { "orderId": "...", "status": "PENDING" }

# Consultar el estado tras ~1 segundo
curl http://localhost:3001/orders/<orderId>
# → { "status": "CONFIRMED", "sagaStep": "COMPLETED", ... }
```

En los logs de Docker verás la secuencia: `RESERVE_SEATS` → reserva → `SEATS_RESERVED` → pago → `CONFIRMED`.

---

## Paso 3 — Idempotencia

### El patrón

```
Kafka re-entrega RESERVE_SEATS (mismo eventId)
            │
            ▼
 prisma.$transaction(async tx => {
   tx.processedEvent.create({ eventId })   ← PK única en la tabla
       │
       ├── INSERT OK (primera vez)  → reservar asientos → commit
       └── P2002 (eventId ya existe) → throw → rollback → catch → ignorar
 })
```

La clave es que el `INSERT` en `ProcessedEvent` y el `UPDATE` en `Seat` ocurren en la **misma transacción**. Si el servicio cae entre ambos, la transacción se revierte completa y el evento puede re-procesarse porque el `eventId` no quedó registrado.

### Archivos modificados

| Archivo | Cambio |
|---|---|
| `saga.orchestrator.ts` | Agrega `eventId: order.id` al payload de `RESERVE_SEATS` |
| `inventory.consumer.ts` | Incluye `eventId` en el tipo del payload |
| `seat.service.ts` | Envuelve la reserva en `$transaction`; captura `P2002` para ignorar duplicados |

### Cómo probarlo

```bash
# 1. Crear la orden y guardar el orderId
curl -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"eventName": "Test Event", "seatCount": 2}'

# 2. Re-procesar el mismo evento manualmente para simular re-entrega de Kafka
#    (ver logs de inventory-service)

# Primera vez:  "[INVENTORY] 2 asientos reservados para order ..."
# Segunda vez:  "[INVENTORY] Evento duplicado ignorado: <eventId>"
```

---

## Paso 4 — Compensación

### Flujo completo

```
POST /orders
    │
    ├─ Order PENDING → Kafka → RESERVE_SEATS
    │
    ▼ inventory-service
    Reserva asientos → Kafka → SEATS_RESERVED
    │
    ▼ order-service
    PaymentMock.processPayment()
    │
    ├─ true  (50%) → Order CONFIRMED                              ✅ Happy path
    │
    └─ false (50%) → Order FAILED → Kafka → RELEASE_SEATS        ❌ Compensación
                                                  │
                                                  ▼ inventory-service
                                                  Libera asientos (reserved=false, orderId=null)
```

### Qué cambió

| Archivo | Cambio |
|---|---|
| `payment.mock.ts` | `Math.random() >= 0.5` en vez de `true` |
| `saga.orchestrator.ts` | Rama de fallo: order → `FAILED`, emite `RELEASE_SEATS` con `eventId: orderId-release` |
| `seat.service.ts` | Nuevo `releaseSeats()` con el mismo patrón de idempotencia en transacción |
| `inventory.consumer.ts` | Nuevo `@EventPattern('RELEASE_SEATS')` |

### Cómo verificarlo

```bash
# Lanzar varias órdenes (~50% fallarán el pago)
curl -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"eventName": "Test Event", "seatCount": 2}'

# Consultar el estado de la orden
curl http://localhost:3001/orders/<orderId>

# Verificar en BD que los asientos quedaron libres tras una compensación
docker exec kafka-implementation-service-postgres-1 \
  psql -U postgres -d ticketsaga \
  -c 'SELECT id, reserved, "orderId" FROM inventory."Seat" WHERE "orderId" = '"'"'<orderId>'"'"';'
```

En los logs de Docker:
- Éxito: `✅ Order ... → CONFIRMED`
- Fallo: `❌ Pago fallido ... → emitiendo RELEASE_SEATS` seguido de `↩️ Asientos liberados`
