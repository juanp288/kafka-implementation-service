import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './dto/create-order.dto';
import {
  ReleaseSeatsCommand,
  SeatsReservationRejectedEvent,
  SeatsReservedEvent,
} from './events';
import { PaymentMock } from './payment.mock';

@Injectable()
export class SagaOrchestrator {
  private readonly logger = new Logger(SagaOrchestrator.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly payment: PaymentMock,
  ) {}

  async startSaga(dto: CreateOrderDto) {
    // Transacción atómica: la orden y el evento de Outbox se crean juntos.
    // Si el proceso cae después de este punto, OutboxPublisher re-emitirá
    // RESERVE_SEATS en el siguiente ciclo de polling.
    // correlationId: identifica toda la SAGA de punta a punta. Se genera una
    // sola vez aquí y viaja en cada evento (RESERVE_SEATS, SEATS_RESERVED,
    // RESERVE_SEATS_REJECTED, RELEASE_SEATS) — permite filtrar los logs de
    // ambos servicios por un único ID, la base de cualquier setup de tracing
    // (OpenTelemetry propaga el trace context de la misma manera).
    const correlationId = randomUUID();

    const order = await this.prisma.$transaction(async (tx) => {
      const created = await tx.order.create({
        data: {
          eventName: dto.eventName,
          seatCount: dto.seatCount,
          sagaStep: 'RESERVING_SEATS',
          correlationId,
        },
      });

      await tx.outbox.create({
        data: {
          topic: TOPICS.RESERVE_SEATS,
          payload: {
            eventId: created.id,
            orderId: created.id,
            correlationId,
            eventName: created.eventName,
            seatCount: created.seatCount,
          },
        },
      });

      return created;
    });

    this.logger.log(
      `[SAGA][CID:${correlationId}] Order ${order.id} created → Outbox: ${TOPICS.RESERVE_SEATS}`,
    );
    return { orderId: order.id, status: order.status };
  }

  async onSeatsReserved({ orderId, correlationId }: SeatsReservedEvent) {
    this.logger.log(
      `[SAGA][CID:${correlationId}] ${TOPICS.SEATS_RESERVED} recibido para order ${orderId} → procesando pago`,
    );

    const paid = this.payment.processPayment();

    if (paid) {
      await this.prisma.order.update({
        where: { id: orderId },
        data: { status: 'CONFIRMED', sagaStep: 'COMPLETED' },
      });
      this.logger.log(
        `[SAGA][CID:${correlationId}] ✅ Order ${orderId} → CONFIRMED`,
      );
      return;
    }

    // Compensación: el pago falló. La orden pasa a FAILED y el evento
    // RELEASE_SEATS va al Outbox en la misma transacción — si el proceso
    // cae entre los dos, el estado queda consistente.
    await this.prisma.$transaction(async (tx) => {
      await tx.order.update({
        where: { id: orderId },
        data: { status: 'FAILED', sagaStep: 'COMPENSATING' },
      });

      const payload: ReleaseSeatsCommand = {
        eventId: `${orderId}-release`,
        orderId,
        correlationId,
      };
      await tx.outbox.create({
        data: { topic: TOPICS.RELEASE_SEATS, payload: { ...payload } },
      });
    });

    this.logger.log(
      `[SAGA][CID:${correlationId}] ❌ Pago fallido para order ${orderId} → Outbox: ${TOPICS.RELEASE_SEATS}`,
    );
  }

  async onReservationRejected({
    orderId,
    correlationId,
    reason,
  }: SeatsReservationRejectedEvent) {
    // updateMany con guarda de estado: evita carrera con SagaTimeoutService
    // si ambos intentan transicionar la misma orden al mismo tiempo.
    const { count } = await this.prisma.order.updateMany({
      where: { id: orderId, status: 'PENDING', sagaStep: 'RESERVING_SEATS' },
      data: { status: 'FAILED', sagaStep: 'CANCELLED' },
    });

    if (count === 0) {
      this.logger.warn(
        `[SAGA][CID:${correlationId}] ${TOPICS.RESERVE_SEATS_REJECTED} para order ${orderId} ignorado (la orden ya no está en RESERVING_SEATS)`,
      );
      return;
    }

    // Sin compensación: nunca se llegó a reservar nada, así que no hay
    // nada que liberar — emitir RELEASE_SEATS aquí sería semánticamente vacío.
    this.logger.warn(
      `[SAGA][CID:${correlationId}] ❌ Reserva rechazada para order ${orderId} (${reason}) → Order CANCELLED (sin compensación)`,
    );
  }
}
