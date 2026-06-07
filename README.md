# Ticket Reservation SAGA

Implementación de una SAGA por orquestación sobre Kafka usando dos servicios NestJS. Más allá del happy path, el proyecto cubre lo que hace falta para que una SAGA asíncrona sea confiable en producción: entrega atómica de eventos (Outbox), idempotencia en el consumidor, compensación ante fallos de negocio, manejo de mensajes envenenados (DLQ), detección de SAGAs huérfanas (timeout) y trazabilidad end-to-end (correlation ID).

El pago se simula dentro del `order-service` (falla el 50% de las veces) para mantener el scope en dos servicios sin perder el rollback real.

> Para el detalle paso a paso de cómo se construyó cada pieza (con diagramas, archivos modificados y cómo probar cada una), ver [`step-by-step.md`](./step-by-step.md).

## Arquitectura

![Arquitectura general](./ticket_saga_architecture.svg)

## Flujo de la SAGA

![Happy path y compensación](./ticket_saga_flow.svg)

La operación de negocio es comprar tickets. El orquestador la divide en pasos secuenciales. Si el pago falla, emite un comando de compensación para deshacer la reserva de asientos.

## Estructura

```
kafka-implementation-service/
├── docker-compose.yml
├── order-service/                  # Orquestador de la SAGA
│   ├── prisma/schema.prisma
│   └── src/
│       ├── kafka/                  # Cliente Kafka + topics
│       ├── outbox/                 # Publisher del patrón Outbox
│       ├── dlq/                    # Reintentos + persistencia de mensajes muertos
│       └── orders/
│           ├── orders.controller.ts    # POST /orders, GET /orders/:id
│           ├── orders.consumer.ts      # SEATS_RESERVED, RESERVE_SEATS_REJECTED
│           ├── saga.orchestrator.ts    # coordina los pasos de la SAGA
│           ├── saga-timeout.service.ts # rescata SAGAs huérfanas
│           ├── payment.mock.ts
│           └── events.ts
└── inventory-service/              # Participante de la SAGA
    ├── prisma/schema.prisma
    └── src/
        ├── kafka/
        ├── outbox/
        ├── dlq/
        └── inventory/
            ├── inventory.consumer.ts   # RESERVE_SEATS, RELEASE_SEATS
            ├── seat.service.ts         # reserva/libera asientos + idempotencia
            ├── seeder.service.ts       # siembra asientos al arrancar
            └── events.ts
```

## Infraestructura

Un solo Postgres con dos schemas (`orders` e `inventory`), Kafka y Zookeeper.

```bash
docker compose up --build
```

## Kafka topics

| Topic | Dirección | Descripción |
|---|---|---|
| `RESERVE_SEATS` | order → inventory | Comando: reservar N asientos |
| `SEATS_RESERVED` | inventory → order | Confirmación de reserva |
| `RESERVE_SEATS_REJECTED` | inventory → order | Rechazo de negocio (no hay asientos suficientes) — no requiere compensación |
| `RELEASE_SEATS` | order → inventory | Compensación: liberar asientos |
| `<topic>.DLQ` | consumidor → sí mismo | Cola de mensajes envenenados, uno por cada topic anterior, tras agotar reintentos |

Ningún servicio emite a Kafka directamente: cada cambio de negocio escribe el evento en una tabla `Outbox` dentro de la misma transacción, y un publisher periódico lo entrega (ver [`step-by-step.md` § Paso 5](./step-by-step.md#paso-5--outbox-pattern)). Todos los eventos viajan con `eventId` (idempotencia) y `correlationId` (trazabilidad).

## Base de datos

Un solo Postgres con dos schemas. Cada servicio tiene además su propia copia de `Outbox` y `DeadLetter` — son tablas de infraestructura, no de negocio, así que no se comparten entre schemas.

`order-service` — schema `orders`:

```prisma
model Order {
  id            String   @id @default(uuid())
  eventName     String
  seatCount     Int
  status        String   @default("PENDING")  // PENDING | CONFIRMED | FAILED
  sagaStep      String?                       // ver "Estados de la orden" abajo
  correlationId String                        // mismo ID en todos los eventos de esta SAGA
  createdAt     DateTime @default(now())
}
```

`inventory-service` — schema `inventory`:

```prisma
model Seat {
  id        Int     @id @default(autoincrement())
  eventName String
  reserved  Boolean @default(false)
  orderId   String?
}

model ProcessedEvent {
  eventId     String   @id   // clave de idempotencia
  processedAt DateTime @default(now())
}
```

`Outbox` y `DeadLetter` (idénticas en ambos schemas):

```prisma
model Outbox {
  id          String    @id @default(uuid())
  topic       String
  payload     Json
  publishedAt DateTime?   // null = pendiente de publicar
  createdAt   DateTime  @default(now())
}

model DeadLetter {
  id         String   @id @default(uuid())
  topic      String
  eventId    String?
  payload    Json
  error      String
  retryCount Int
  receivedAt DateTime @default(now())
}
```

### Estados de la orden

```
status:    PENDING ──► CONFIRMED
              │
              └──────► FAILED

sagaStep:  RESERVING_SEATS ──► COMPLETED       (pago aceptado)
              │
              ├────────────► COMPENSATING      (pago falló o SAGA timeout → emite RELEASE_SEATS)
              │
              └────────────► CANCELLED         (RESERVE_SEATS_REJECTED → nunca se reservó nada, sin compensar)
```

## El pasos de compensación y por qué importa el orden

Los asientos se reservan **antes** de cobrar porque liberar un asiento es trivial (un UPDATE), mientras que revertir un cobro requiere integración con el proveedor de pagos. La regla general en SAGAs: el paso más fácil de compensar va primero.
