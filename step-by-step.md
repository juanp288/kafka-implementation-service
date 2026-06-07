# Step by Step

---

## Paso 1 — Infraestructura base y conectividad Kafka

### Lo que se creó / modificó


| Archivo                                  | Acción                                                                                       |
| ---------------------------------------- | -------------------------------------------------------------------------------------------- |
| `docker-compose.yml`                     | Zookeeper, Kafka, Postgres (una sola DB con dos schemas), ambos servicios                    |
| `order-service/prisma/schema.prisma`     | Modelo `Order`, schema `orders`                                                              |
| `inventory-service/prisma/schema.prisma` | Modelos `Seat` + `ProcessedEvent`, schema `inventory`                                        |
| `*/src/prisma/prisma.service.ts`         | `PrismaService` estándar de NestJS (en ambos servicios)                                      |
| `*/src/app.module.ts`                    | `ConfigModule` + `ClientsModule` (Kafka producer) en order; solo `ConfigModule` en inventory |
| `*/src/main.ts`                          | `inventory-service` arranca como hybrid app (HTTP + Kafka consumer)                          |
| `*/src/app.controller.ts`                | `GET /test` en order emite `test.ping`; `@EventPattern('test.ping')` en inventory lo recibe  |
| `*/Dockerfile`                           | Build de producción + `prisma db push` al arrancar                                           |
| `*/.dockerignore`                        | Excluye `node_modules` y `dist`                                                              |


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

> **Nota (vigencia actual):** este diagrama muestra el flujo tal como se implementó en el Paso 2. Desde el Paso 5, los `Kafka → TOPIC` directos se reemplazaron por escritura en `Outbox` + publicación asíncrona, y todos los eventos viajan con `correlationId` (Paso 9). El diagrama actualizado de extremo a extremo está en el Paso 5.

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


| Archivo                                                 | Rol                                                                    |
| ------------------------------------------------------- | ---------------------------------------------------------------------- |
| `order-service/src/kafka/kafka.module.ts`               | Configura `ClientKafka` y lo exporta para inyección                    |
| `order-service/src/orders/saga.orchestrator.ts`         | Coordina los pasos: crea orden, emite comando, procesa respuesta       |
| `order-service/src/orders/orders.controller.ts`         | `POST /orders` + `GET /orders/:id` + `@EventPattern('SEATS_RESERVED')` |
| `order-service/src/orders/payment.mock.ts`              | Siempre retorna `true` (se cambia en Paso 4)                           |
| `inventory-service/src/inventory/seat.service.ts`       | Reserva asientos + auto-seed de 50 asientos al arrancar                |
| `inventory-service/src/inventory/inventory.consumer.ts` | `@EventPattern('RESERVE_SEATS')`                                       |


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


| Archivo                 | Cambio                                                                         |
| ----------------------- | ------------------------------------------------------------------------------ |
| `saga.orchestrator.ts`  | Agrega `eventId: order.id` al payload de `RESERVE_SEATS`                       |
| `inventory.consumer.ts` | Incluye `eventId` en el tipo del payload                                       |
| `seat.service.ts`       | Envuelve la reserva en `$transaction`; captura `P2002` para ignorar duplicados |


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

> **Nota (vigencia actual):** igual que en el Paso 2, este diagrama es el original del Paso 4 (emisión directa a Kafka, sin rechazo de inventario ni timeout). El flujo completo y actualizado — con Outbox, rechazo de reserva y timeout de SAGA — está documentado en los Pasos 5 a 8.

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


| Archivo                 | Cambio                                                                                |
| ----------------------- | ------------------------------------------------------------------------------------- |
| `payment.mock.ts`       | `Math.random() >= 0.5` en vez de `true`                                               |
| `saga.orchestrator.ts`  | Rama de fallo: order → `FAILED`, emite `RELEASE_SEATS` con `eventId: orderId-release` |
| `seat.service.ts`       | Nuevo `releaseSeats()` con el mismo patrón de idempotencia en transacción             |
| `inventory.consumer.ts` | Nuevo `@EventPattern('RELEASE_SEATS')`                                                |


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

---

## Paso 5 — Outbox pattern

### El problema que resuelve

Hasta el Paso 4, el orquestador escribía en la BD y emitía a Kafka como dos operaciones separadas. Si el proceso caía entre ambas, la orden quedaba persistida pero el evento nunca salía — la SAGA quedaba huérfana sin que nada lo notara. El patrón Outbox lo resuelve escribiendo el evento en una tabla `Outbox` **dentro de la misma transacción** que el cambio de negocio; un publisher aparte lo entrega a Kafka.

### El patrón

```
$transaction(tx => {
  tx.order.update({ status, sagaStep, ... })          ← cambio de negocio
  tx.outbox.create({ topic, payload })                ← evento a publicar
})                                                       misma transacción ⇒ atomicidad

OutboxPublisher  (@Interval(1000ms), hasta 50 filas por ciclo)
        │
        ▼
  SELECT * FROM Outbox WHERE "publishedAt" IS NULL
        │
        ├─ kafka.emit(topic, payload)
        └─ UPDATE Outbox SET "publishedAt" = now()
```

Si el proceso cae entre el `emit` y el `UPDATE`, el siguiente ciclo reemite el mismo evento — es seguro porque el consumidor deduplica por `eventId` (patrón de idempotencia del Paso 3).

### Diagrama de extremo a extremo (estado actual, reemplaza al del Paso 2)

```
POST /orders
    │
    └─ $tx: Order{PENDING, RESERVING_SEATS, correlationId} + Outbox{RESERVE_SEATS}
            │
            ▼ OutboxPublisher (poll 1s)
            Kafka → RESERVE_SEATS ─────────────────────► inventory-service
                                                              │
                                                   SeatService.reserveSeats()
                                                   $tx: ProcessedEvent + Seat.reserved=true + Outbox{SEATS_RESERVED}
                                                              │
                                                              ▼ OutboxPublisher (poll 1s)
                                                   Kafka → SEATS_RESERVED ──► order-service
                                                                                    │
                                                               PaymentMock.processPayment() → true
                                                               Order → CONFIRMED / COMPLETED
```

### Archivos nuevos / modificados


| Archivo                                                              | Rol                                                                                           |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `*/prisma/schema.prisma`                                             | Modelo `Outbox { id, topic, payload, publishedAt, createdAt }` (en ambos servicios)           |
| `*/src/outbox/outbox.publisher.ts`                                   | `@Interval(1000)`: lee filas con `publishedAt: null`, emite a Kafka y marca `publishedAt`     |
| `saga.orchestrator.ts`, `seat.service.ts`, `saga-timeout.service.ts` | Ya no llaman `kafka.emit()` directo: escriben en `Outbox` dentro de la transacción de negocio |


### Cómo probarlo

```bash
# Ver eventos en tránsito / ya publicados
docker exec kafka-implementation-service-postgres-1 \
  psql -U postgres -d ticketsaga \
  -c 'SELECT topic, "publishedAt" FROM orders."Outbox" ORDER BY "createdAt" DESC LIMIT 10;'

# En los logs verás al publisher entregando cada segundo:
# [OUTBOX] Publicado RESERVE_SEATS (id=...)
```

---

## Paso 6 — Dead Letter Queue (DLQ)

### El problema que resuelve

Si un consumidor lanza una excepción procesando un mensaje, Kafka lo reentrega indefinidamente y puede atascar el partition. El DLQ acota los reintentos a un número fijo y aparta los mensajes "envenenados" en una tabla para inspección manual, sin bloquear el resto del tráfico.

### El patrón (a nivel aplicación, no es un `deadLetterQueue` de broker)

```
Consumer recibe mensaje
        │
        ▼
DlqService.withRetry(topic, payload, context, handler)
        │
   handler() lanza excepción
        │
        ├─ retryCount < 3 ──► espera (retryCount + 1) × 2000 ms   (2 s, 4 s, 6 s)
        │                     re-emite al MISMO topic
        │                     header x-retry-count = nextRetryCount
        │
        └─ retryCount === 3 ─► emite a `${topic}.DLQ` con headers:
                                x-original-topic, x-error, x-retry-count
                                (NO relanza ⇒ el offset original se commitea igual)
                                        │
                                        ▼
                            DlqConsumer.persist()
                            INSERT INTO DeadLetter
                              { topic, eventId, payload, error, retryCount }
```

### Topics con DLQ configurado


| Topic original           | Consumidor        | DLQ                          |
| ------------------------ | ----------------- | ---------------------------- |
| `RESERVE_SEATS`          | inventory-service | `RESERVE_SEATS.DLQ`          |
| `RELEASE_SEATS`          | inventory-service | `RELEASE_SEATS.DLQ`          |
| `SEATS_RESERVED`         | order-service     | `SEATS_RESERVED.DLQ`         |
| `RESERVE_SEATS_REJECTED` | order-service     | `RESERVE_SEATS_REJECTED.DLQ` |


### Archivos nuevos


| Archivo                     | Rol                                                                                        |
| --------------------------- | ------------------------------------------------------------------------------------------ |
| `*/prisma/schema.prisma`    | Modelo `DeadLetter { id, topic, eventId, payload, error, retryCount, createdAt }`          |
| `*/src/dlq/dlq.service.ts`  | `withRetry()` / `retryOrSendToDlq()`: reintentos con backoff exponencial (2 s / 4 s / 6 s) |
| `*/src/dlq/dlq.consumer.ts` | `@EventPattern('*.DLQ')`: persiste el mensaje muerto en `DeadLetter`                       |
| `*.consumer.ts`             | Cada handler envuelve su lógica en `dlq.withRetry(...)`                                    |


### Cómo probarlo

```bash
# Forzar fallos: tirar inventory-service mientras hay órdenes en vuelo
docker compose stop inventory-service
curl -X POST http://localhost:3001/orders -H "Content-Type: application/json" -d '{"eventName":"Test Event","seatCount":2}'
docker compose start inventory-service

# En los logs verás la secuencia de reintentos:
# [DLQ] RESERVE_SEATS → fallo. Reintento 1/3 en 2000ms
# [DLQ] RESERVE_SEATS → fallo. Reintento 2/3 en 4000ms
# [DLQ] RESERVE_SEATS → 3 reintentos agotados. Enviando a RESERVE_SEATS.DLQ: <error>
# [DLQ] 💀 Mensaje muerto persistido — topic=RESERVE_SEATS eventId=... retries=3

# Verificar lo persistido
docker exec kafka-implementation-service-postgres-1 \
  psql -U postgres -d ticketsaga \
  -c 'SELECT topic, "eventId", "retryCount", error FROM inventory."DeadLetter";'
```

---

## Paso 7 — Rechazo de reserva (`RESERVE_SEATS_REJECTED`)

### El problema que resuelve

Antes de este paso, si no había asientos suficientes para el evento, la orden se quedaba esperando `SEATS_RESERVED` indefinidamente — solo el timeout de SAGA (Paso 8) la rescataba, varios minutos después. Ahora inventory-service responde de inmediato con un **rechazo de negocio**: no es un error técnico, así que no pasa por DLQ ni se compensa (nunca llegó a reservarse nada).

### Flujo

```
RESERVE_SEATS ──► inventory-service
                       │
              SeatService.reserveSeats()
              ¿hay seatCount asientos libres para eventName?
                       │
            ┌──────────┴───────────┐
            │ sí                   │ no
            ▼                      ▼
   reserva asientos          Outbox{ RESERVE_SEATS_REJECTED,
   + Outbox{SEATS_RESERVED}          reason: 'NOT_ENOUGH_SEATS' }
                                      │
                                      ▼ order-service
                             onReservationRejected()
                             updateMany WHERE status=PENDING AND sagaStep=RESERVING_SEATS
                                       SET status=FAILED, sagaStep=CANCELLED
                             (sin RELEASE_SEATS: nunca hubo nada que liberar)
```

### Archivos modificados


| Archivo                | Cambio                                                                                                                                                                                        |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seat.service.ts`      | Si `available.length < seatCount`, escribe `RESERVE_SEATS_REJECTED` en el Outbox en lugar de `SEATS_RESERVED`                                                                                 |
| `events.ts`            | Nueva clase `SeatsReservationRejectedEvent { orderId, correlationId, reason }`                                                                                                                |
| `orders.consumer.ts`   | `@EventPattern('RESERVE_SEATS_REJECTED')` → `onReservationRejected()`                                                                                                                         |
| `saga.orchestrator.ts` | `onReservationRejected()`: usa `updateMany` con guarda de estado (`status: PENDING`, `sagaStep: RESERVING_SEATS`) para no pisar una orden que ya transicionó por otra vía (p. ej. el timeout) |


### Cómo verificarlo

```bash
# Pedir más asientos de los que hay sembrados para el evento (seed por defecto = 50)
curl -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"eventName": "Test Event", "seatCount": 999}'

curl http://localhost:3001/orders/<orderId>
# → { "status": "FAILED", "sagaStep": "CANCELLED", ... }
```

En los logs: `❌ Reserva rechazada para order ... (NOT_ENOUGH_SEATS) → Order CANCELLED (sin compensación)`

---

## Paso 8 — SAGA timeout

### El problema que resuelve

Si `SEATS_RESERVED` (ni el rechazo) nunca llega — inventory-service caído, mensaje atascado, partición de red — la orden quedaría en `PENDING` para siempre. Un job periódico detecta estas órdenes "huérfanas" y dispara la compensación por su cuenta.

### Flujo

```
@Cron(EVERY_MINUTE)
SagaTimeoutService.cancelStaleOrders()
        │
        ▼
SELECT * FROM Order
WHERE status = 'PENDING'
  AND sagaStep = 'RESERVING_SEATS'
  AND createdAt < now() - SAGA_TIMEOUT_MINUTES   (default: 5 min)
        │
        ▼ por cada orden "stale"
$transaction:
  updateMany WHERE id=orderId AND status=PENDING AND sagaStep=RESERVING_SEATS
             SET status=FAILED, sagaStep=COMPENSATING        ← guarda de estado
             (count === 0 ⇒ ya transicionó por otra vía → no hacer nada)
  Outbox{ RELEASE_SEATS, eventId: `${orderId}-timeout-release` }
        │
        ▼ inventory-service libera cualquier asiento que sí llegó a reservarse
```

### Archivos nuevos


| Archivo                                            | Rol                                                                                                    |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `order-service/src/orders/saga-timeout.service.ts` | `@Cron(CronExpression.EVERY_MINUTE)`; umbral configurable vía env `SAGA_TIMEOUT_MINUTES` (default `5`) |


### Cómo verificarlo

```bash
# Tirar inventory-service ANTES de crear la orden, para que nunca llegue SEATS_RESERVED
docker compose stop inventory-service
curl -X POST http://localhost:3001/orders -H "Content-Type: application/json" -d '{"eventName":"Test Event","seatCount":2}'

# Esperar > SAGA_TIMEOUT_MINUTES y revisar logs de order-service:
# ⏱️  Order ... sin SEATS_RESERVED tras 5min → cancelada, Outbox: RELEASE_SEATS

curl http://localhost:3001/orders/<orderId>
# → { "status": "FAILED", "sagaStep": "COMPENSATING", ... }
```

---

## Paso 9 — Correlation ID / trazabilidad

### El problema que resuelve

Una SAGA cruza dos servicios y media docena de topics distintos (incluyendo sus DLQ). Sin un identificador común, reconstruir el rastro de una orden específica entre los logs de Docker es prácticamente imposible.

### El patrón

```
POST /orders
    │
    └─ correlationId = randomUUID()        ← se genera UNA sola vez, al iniciar la SAGA
            │
            ├─ se persiste en Order.correlationId
            └─ viaja en el payload de TODOS los eventos de esa SAGA:
                 RESERVE_SEATS · SEATS_RESERVED · RESERVE_SEATS_REJECTED · RELEASE_SEATS

Todos los logs de la SAGA llevan el prefijo [CID:<correlationId>]
        │
        ▼
docker compose logs order-service inventory-service | grep "CID:<correlationId>"
        → traza completa, ordenada cronológicamente, de punta a punta
```

### Archivos modificados


| Archivo                   | Cambio                                                                                                                                                            |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*/prisma/schema.prisma`  | Campo `correlationId String` agregado a `Order`                                                                                                                   |
| `events.ts`               | Todas las clases de evento (`ReserveSeatsCommand`, `SeatsReservedEvent`, `SeatsReservationRejectedEvent`, `ReleaseSeatsCommand`) incluyen `correlationId: string` |
| `saga.orchestrator.ts`    | Genera el `correlationId` al crear la orden, lo propaga en `RESERVE_SEATS` y `RELEASE_SEATS`, y prefija sus logs con `[CID:...]`                                  |
| `seat.service.ts`         | Recibe y reenvía el `correlationId` en `SEATS_RESERVED` / `RESERVE_SEATS_REJECTED`; logs con `[CID:...]`                                                          |
| `saga-timeout.service.ts` | Reutiliza el `correlationId` de la orden original al emitir el `RELEASE_SEATS` de compensación por timeout                                                        |


### Cómo verificarlo

```bash
curl -X POST http://localhost:3001/orders -H "Content-Type: application/json" -d '{"eventName":"Test Event","seatCount":2}'
# → { "orderId": "...", "correlationId": "...", "status": "PENDING" }

# Filtrar toda la traza de esa SAGA específica, en ambos servicios:
docker compose logs order-service inventory-service | grep "CID:<correlationId>"
```

