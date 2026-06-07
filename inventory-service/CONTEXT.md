# inventory-service — Contexto

Este servicio actúa como **participante de la SAGA**. No conoce el flujo completo, solo responde a los comandos que le llegan por Kafka: reservar asientos o liberarlos. Arranca como *hybrid app*: tiene un endpoint HTTP para health check y un consumer Kafka activo.

## Estructura

```
src/
├── main.ts
├── app.module.ts
├── kafka/
│   └── kafka.module.ts
├── inventory/
│   ├── inventory.module.ts
│   ├── inventory.consumer.ts
│   └── seat.service.ts
└── prisma/
    └── prisma.service.ts
```

## Componentes

### `main.ts`
Configura la app como *hybrid*: HTTP en el puerto 3002 y un consumer Kafka con `groupId: 'inventory-consumer'`. El `groupId` es importante: Kafka lo usa para garantizar que solo una instancia del servicio procese cada mensaje cuando hay múltiples réplicas.

### `app.module.ts`
Módulo raíz. Importa `ConfigModule` e `InventoryModule`. Sin lógica propia.

### `kafka/kafka.module.ts`
Igual al de `order-service`: registra y exporta el `ClientKafka`. Este servicio necesita producir también (`SEATS_RESERVED`) además de consumir, por eso necesita el cliente.

### `inventory/inventory.module.ts`
Ensambla el dominio de inventario: importa `KafkaModule`, registra `InventoryConsumer` como controlador y `SeatService` como provider.

### `inventory/inventory.consumer.ts`
El punto de entrada de todos los comandos Kafka que recibe este servicio:
- `@EventPattern('RESERVE_SEATS')` — delega a `SeatService.reserveSeats()`.
- `@EventPattern('RELEASE_SEATS')` — delega a `SeatService.releaseSeats()`.

Al ser un `@Controller`, NestJS lo registra automáticamente como handler del microservicio Kafka cuando está en el módulo. No hace lógica de negocio, solo enruta.

### `inventory/seat.service.ts`
Toda la lógica de negocio y la **garantía de idempotencia**:

- `onModuleInit()` — conecta el Kafka producer y ejecuta el seed inicial si no hay asientos en BD.
- `reserveSeats()` — dentro de una `$transaction` de Prisma: inserta el `eventId` en `ProcessedEvent` (falla con P2002 si ya existe), luego busca asientos libres y los marca como reservados. Si todo va bien, emite `SEATS_RESERVED`.
- `releaseSeats()` — mismo patrón de idempotencia: inserta `eventId`, luego pone `reserved=false` y `orderId=null` en los asientos del order. Compensa la reserva cuando el pago falla.

La transacción garantiza atomicidad: si el servicio cae a mitad del proceso, ningún cambio parcial queda en la BD y el evento puede re-procesarse de forma segura.

### `prisma/prisma.service.ts`
Extiende `PrismaClient` y se conecta en `onModuleInit`. Apunta al schema `inventory` de la base de datos `ticketsaga` vía `DATABASE_URL`.
