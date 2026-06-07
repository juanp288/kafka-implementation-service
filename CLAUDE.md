# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

SAGA orchestration pattern implemented over Kafka with two NestJS microservices. The business operation is ticket purchasing: reserve seats → process payment → confirm or compensate (release seats on payment failure).

Both services run as NestJS **hybrid apps** (HTTP + Kafka consumer simultaneously). The orchestrator is `order-service`; the participant is `inventory-service`.

## Commands

```bash
# Start everything (infra + both services)
docker compose up --build

# Local dev (infra only in Docker, services outside)
docker compose up zookeeper kafka postgres -d
cd order-service && npm run start:dev      # port 3001
cd inventory-service && npm run start:dev  # port 3002

# Build
npm run build

# Tests
npm run test
npm run test:e2e
```

## Infrastructure

Single Postgres (`ticketsaga` DB) with two schemas — no separate DB per service.

| Container | Port | Notes |
|---|---|---|
| Kafka | 9093 (host) / 9092 (internal) | Two listeners configured |
| Postgres | 5432 | DB: `ticketsaga`, schemas: `orders`, `inventory` |
| order-service | 3001 | |
| inventory-service | 3002 | |

Schema sync: `prisma db push` runs on container start (Dockerfile CMD). No migrations directory — use `prisma migrate dev` if switching to a migration-based workflow.

## Kafka topics

| Topic | Producer → Consumer | Purpose |
|---|---|---|
| `RESERVE_SEATS` | order → inventory | Command: reserve N seats |
| `SEATS_RESERVED` | inventory → order | Confirmation |
| `RESERVE_SEATS_REJECTED` | inventory → order | Business rejection (not enough seats) — order compensates immediately instead of waiting on the SAGA timeout |
| `RELEASE_SEATS` | order → inventory | Compensation: undo reservation |

All events carry an `eventId` assigned by the producer for idempotency.

## Idempotency pattern

`inventory-service` uses `ProcessedEvent` table. On every incoming command, `seat.service.ts` opens a `$transaction` that first inserts the `eventId` (PK), then does the business logic. A duplicate `eventId` throws `P2002`, rolling back the entire transaction — the event is silently skipped.

## SAGA design rule

Seats are reserved **before** charging. Releasing a seat is trivial (one UPDATE); reversing a charge requires third-party coordination. Always put the easiest-to-compensate step first.

## Order states

`PENDING` → `CONFIRMED` | `FAILED`

`sagaStep` tracks position: `RESERVING_SEATS` → `COMPLETED` (success), `COMPENSATING` (payment failed or SAGA timed out, after emitting `RELEASE_SEATS`), or `CANCELLED` (rejected by inventory before any seat was reserved — `RESERVE_SEATS_REJECTED` received, no compensation needed).

## Key files

- Orchestration logic: `order-service/src/orders/saga.orchestrator.ts`
- Seat reservation + idempotency: `inventory-service/src/inventory/seat.service.ts`
- Kafka producer config: `*/src/kafka/kafka.module.ts`
- Hybrid app setup: `*/src/main.ts`

For component-level documentation of each service see `order-service/CONTEXT.md` and `inventory-service/CONTEXT.md`.
