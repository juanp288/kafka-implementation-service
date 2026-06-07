# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This repo implements a **ticket-purchase SAGA** using the orchestration pattern across two NestJS microservices communicating via Kafka. No code exists yet — the README and SVG diagrams define the architecture to build.

Business operation: `POST /orders` triggers a multi-step saga: reserve seats → process payment → confirm or compensate (release seats on payment failure).

## Planned folder structure

```
ticket-saga/
├── docker-compose.yml
├── orders-service/        # Orchestrator + payment mock
│   ├── prisma/schema.prisma
│   └── src/
│       ├── orders/
│       │   ├── orders.controller.ts   # POST /orders entry point
│       │   ├── saga.orchestrator.ts   # coordinates saga steps
│       │   └── payment.mock.ts        # random 50% failure
│       └── kafka/kafka.module.ts
└── inventory-service/     # Saga participant
    ├── prisma/schema.prisma
    └── src/
        ├── inventory/
        │   ├── inventory.consumer.ts  # listens for RESERVE_SEATS / RELEASE_SEATS
        │   └── seat.service.ts        # seat logic + idempotency
        └── kafka/kafka.module.ts
```

## Infrastructure

Start everything with:
```bash
docker compose up -d
```

Services: Kafka on `9092`, Zookeeper on `2181`, `postgres-orders` on `5432` (db: `orders`), `postgres-inventory` on `5433` (db: `inventory`). Password for both: `postgres`.

## NestJS service commands (once created)

```bash
# Install deps
npm install

# Run in dev
npm run start:dev

# Build
npm run build

# Tests
npm run test              # unit
npm run test:e2e          # e2e
npm run test -- --testPathPattern=saga  # single file/pattern

# Prisma
npx prisma migrate dev
npx prisma generate
npx prisma studio
```

## SAGA design constraints

**Ordering rule:** reserve seats before charging. Releasing seats (compensation) is trivial; reversing a charge is not. Always put the easiest-to-compensate step first.

**Kafka topics:**
- `RESERVE_SEATS` — orders-service → inventory-service
- `SEATS_RESERVED` — inventory-service → orders-service
- `RELEASE_SEATS` — orders-service → inventory-service (compensation)

**Idempotency:** `inventory-service` uses a `ProcessedEvent` table. Every incoming Kafka event inserts `eventId` as the PK inside the same DB transaction as the seat change. Duplicate events are silently skipped.

**Order states:** `PENDING` → `CONFIRMED` | `FAILED`

## Data models

`orders-service`:
```prisma
model Order {
  id        String   @id @default(uuid())
  eventName String
  seatCount Int
  status    String   @default("PENDING")
  sagaStep  String?
  createdAt DateTime @default(now())
}
```

`inventory-service`:
```prisma
model Seat {
  id        Int     @id @default(autoincrement())
  eventName String
  reserved  Boolean @default(false)
  orderId   String?
}

model ProcessedEvent {
  eventId     String   @id
  processedAt DateTime @default(now())
}
```
