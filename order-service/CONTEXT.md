# order-service — Contexto

Este servicio actúa como **orquestador de la SAGA**. Es el único que conoce la secuencia completa de pasos. Arranca como *hybrid app*: expone HTTP para recibir órdenes y al mismo tiempo escucha Kafka para recibir respuestas de los participantes.

## Estructura

```
src/
├── main.ts
├── app.module.ts
├── kafka/
│   └── kafka.module.ts
├── orders/
│   ├── dto/
│   │   └── create-order.dto.ts
│   ├── orders.module.ts
│   ├── orders.controller.ts
│   ├── saga.orchestrator.ts
│   └── payment.mock.ts
└── prisma/
    └── prisma.service.ts
```

## Componentes

### `main.ts`
Configura la app como *hybrid*: HTTP en el puerto 3001 y un consumer Kafka con `groupId: 'order-consumer'`. El consumer es necesario porque este servicio también **recibe** eventos (`SEATS_RESERVED`) además de emitirlos.

### `app.module.ts`
Módulo raíz. Solo importa `ConfigModule` (variables de entorno) y `OrdersModule`. No registra providers de negocio directamente.

### `kafka/kafka.module.ts`
Registra el `ClientKafka` con el token `'KAFKA_CLIENT'` y lo **exporta** para que cualquier módulo que importe `KafkaModule` pueda inyectar el productor. Centraliza la configuración del broker.

### `orders/orders.module.ts`
Ensambla todo lo del dominio de órdenes: importa `KafkaModule`, registra `OrdersController`, `SagaOrchestrator` y `PaymentMock` como providers.

### `orders/orders.controller.ts`
Tiene dos responsabilidades en una sola clase:
- **HTTP**: `POST /orders` inicia la SAGA; `GET /orders/:id` permite consultar el estado.
- **Kafka consumer**: `@EventPattern('SEATS_RESERVED')` recibe la confirmación del inventory-service y delega al orquestador.

NestJS permite mezclar handlers HTTP y Kafka en el mismo controlador gracias al setup de hybrid app en `main.ts`.

### `orders/saga.orchestrator.ts`
El cerebro de la SAGA. Contiene la lógica de coordinación:
- `startSaga()` — crea la orden en BD y emite `RESERVE_SEATS`.
- `onSeatsReserved()` — recibe la confirmación, llama al mock de pago y decide: si el pago es exitoso actualiza la orden a `CONFIRMED`; si falla, la marca como `FAILED` y emite `RELEASE_SEATS` para compensar.

### `orders/payment.mock.ts`
Simula el cobro con una probabilidad de fallo del 50% (`Math.random()`). En un sistema real este método haría la llamada al gateway de pagos. Está separado del orquestador para que pueda reemplazarse sin tocar la lógica de coordinación.

### `orders/dto/create-order.dto.ts`
Define la forma del body de `POST /orders`: `eventName` y `seatCount`. Sin validaciones por ahora (se agregarían con `class-validator` en un paso posterior).

### `prisma/prisma.service.ts`
Extiende `PrismaClient` y se conecta a la base de datos en `onModuleInit`. Apunta al schema `orders` de la base de datos `ticketsaga` vía `DATABASE_URL`.
