import { Injectable, Logger } from '@nestjs/common';
import { TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { ReleaseSeatsCommand, SeatsReservedEvent } from './events';
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
}
