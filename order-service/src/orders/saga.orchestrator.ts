import { Injectable, Logger } from '@nestjs/common';
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
    const order = await this.prisma.$transaction(async (tx) => {
      const created = await tx.order.create({
        data: {
          eventName: dto.eventName,
          seatCount: dto.seatCount,
          sagaStep: 'RESERVING_SEATS',
        },
      });

      await tx.outbox.create({
        data: {
          topic: TOPICS.RESERVE_SEATS,
          payload: {
            eventId: created.id,
            orderId: created.id,
            eventName: created.eventName,
            seatCount: created.seatCount,
          },
        },
      });

      return created;
    });

    this.logger.log(
      `[SAGA] Order ${order.id} created → Outbox: ${TOPICS.RESERVE_SEATS}`,
    );
    return { orderId: order.id, status: order.status };
  }

  async onSeatsReserved({ orderId }: SeatsReservedEvent) {
    this.logger.log(
      `[SAGA] ${TOPICS.SEATS_RESERVED} recibido para order ${orderId} → procesando pago`,
    );

    const paid = this.payment.processPayment();

    if (paid) {
      await this.prisma.order.update({
        where: { id: orderId },
        data: { status: 'CONFIRMED', sagaStep: 'COMPLETED' },
      });
      this.logger.log(`[SAGA] ✅ Order ${orderId} → CONFIRMED`);
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
      };
      await tx.outbox.create({
        data: { topic: TOPICS.RELEASE_SEATS, payload: { ...payload } },
      });
    });

    this.logger.log(
      `[SAGA] ❌ Pago fallido para order ${orderId} → Outbox: ${TOPICS.RELEASE_SEATS}`,
    );
  }

  async onReservationRejected({
    orderId,
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
        `[SAGA] ${TOPICS.RESERVE_SEATS_REJECTED} para order ${orderId} ignorado (la orden ya no está en RESERVING_SEATS)`,
      );
      return;
    }

    // Sin compensación: nunca se llegó a reservar nada, así que no hay
    // nada que liberar — emitir RELEASE_SEATS aquí sería semánticamente vacío.
    this.logger.warn(
      `[SAGA] ❌ Reserva rechazada para order ${orderId} (${reason}) → Order CANCELLED (sin compensación)`,
    );
  }
}
