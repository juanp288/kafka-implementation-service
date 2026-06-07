Alcance claro: 2 servicios, SAGA por orquestación, con foco en idempotencia y compensación. El pago lo simulamos dentro del `orders-service` (un método que falla al azar) para no agregar un tercer servicio. Eso te da el rollback real sin inflar el proyecto.

Te mapeo todo.

## Arquitectura general
![Arquitectura general de la SAGA](./ticket_saga_architecture.svg)

## El flujo de la SAGA (happy path y compensación)

La operación de negocio es: comprar tickets. El orquestador la divide en pasos. Si el pago falla, deshace la reserva de asientos.

![Flujo de la SAGA (happy path y compensación)](./ticket_saga_flow.svg)


## Estructura de carpetas

```
ticket-saga/
├── docker-compose.yml          # Kafka + Zookeeper + Postgres Local x2 Schemas (s_orders, s_inventories)
├── orders-service/
│   ├── prisma/schema.prisma
│   └── src/
│       ├── main.ts
│       ├── orders/
│       │   ├── orders.controller.ts    # POST /orders (inicia la SAGA)
│       │   ├── saga.orchestrator.ts     # coordina los pasos
│       │   └── payment.mock.ts          # pago que falla al azar
│       └── kafka/kafka.module.ts
└── inventory-service/
    ├── prisma/schema.prisma
    └── src/
        ├── main.ts
        ├── inventory/
        │   ├── inventory.consumer.ts    # escucha comandos
        │   └── seat.service.ts          # lógica + idempotencia
        └── kafka/kafka.module.ts
```

## docker-compose.yml

Esto te levanta toda la infraestructura. Aprovechas tu Docker tal como pediste.

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: zookeeper
    ports: '9092:9092'
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  postgres-microservices:
    image: postgres:16
    ports: '5434:5432'
    environment:
      POSTGRES_DB: microservices
      POSTGRES_PASSWORD: postgres
```

## Los schemas de Prisma

`orders-service/prisma/schema.prisma` — la orden y el estado de la SAGA:

```prisma
model Order {
  id        String   @id @default(uuid())
  eventName String
  seatCount Int
  status    String   @default("PENDING")  // PENDING, CONFIRMED, FAILED
  sagaStep  String?                         // dónde va la SAGA
  createdAt DateTime @default(now())
}
```

`inventory-service/prisma/schema.prisma` — los asientos y la tabla de idempotencia:

```prisma
model Seat {
  id        Int     @id @default(autoincrement())
  eventName String
  reserved  Boolean @default(false)
  orderId   String?
}

model ProcessedEvent {
  eventId     String   @id            // clave de idempotencia
  processedAt DateTime @default(now())
}
```

## El plan

Te lo divido para que el tiempo alcance y aprendas cada concepto sin atragantarte.

* Paso 1: levantar `docker-compose`, crear los dos proyectos NestJS, configurar el `ClientKafka` en ambos y conectar Prisma a cada Postgres Schema. La meta es que un servicio emita un evento de prueba y el otro lo reciba y lo imprima en consola. Si logras eso, lo demás es lógica de negocio.

* Paso 2: el happy path completo. Endpoint `POST /orders`, el orquestador crea la orden, emite `RESERVE_SEATS`, el `inventory-service` reserva y responde, el orquestador procesa el pago (siempre éxito por ahora) y confirma.

* Paso 3: idempotencia. Agrega la tabla `ProcessedEvent` y el patrón que vimos (insert del `eventId` + lógica en la misma transacción). Pruébalo enviando el mismo evento dos veces a mano y verifica que solo se procesa una vez.

* Paso 4: compensación. Haz que `PaymentMock` falle el 50% de las veces (`Math.random()`), y cuando falle, el orquestador emite `RELEASE_SEATS`. Verifica que los asientos vuelven a quedar libres y la orden queda en `FAILED`.

## El detalle que más vas a aprender

El reto técnico real no es el código sino entender por qué los pasos van en cierto orden. Por ejemplo: reservas asientos *antes* de cobrar, porque liberar un asiento (compensación) es trivial, pero "descobrar" un pago es complejo. Siempre pon primero lo más fácil de compensar. Ese tipo de decisiones es lo que vas a interiorizar.
