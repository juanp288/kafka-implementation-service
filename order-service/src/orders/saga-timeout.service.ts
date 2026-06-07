import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';
import { ReleaseSeatsCommand } from './events';

// Si SEATS_RESERVED nunca llega (p.ej. inventory-service cayó), la orden
// queda en PENDING/RESERVING_SEATS para siempre. Este job la detecta y la
// cancela emitiendo RELEASE_SEATS — la misma compensación que se usa cuando
// el pago falla. Es seguro emitirla aunque la reserva nunca haya ocurrido:
// seat.service.releaseSeats hace un updateMany por orderId, así que es un
// no-op si no hay asientos reservados para esa orden (idempotente).
@Injectable()
export class SagaTimeoutService {
  private readonly logger = new Logger(SagaTimeoutService.name);
  private readonly timeoutMinutes = Number(
    process.env.SAGA_TIMEOUT_MINUTES ?? 5,
  );

  constructor(private readonly prisma: PrismaService) {}

  @Cron(CronExpression.EVERY_MINUTE)
  async cancelStaleOrders() {
    const threshold = new Date(Date.now() - this.timeoutMinutes * 60_000);

    const staleOrders = await this.prisma.order.findMany({
      where: {
        status: 'PENDING',
        sagaStep: 'RESERVING_SEATS',
        createdAt: { lt: threshold },
      },
    });

    for (const order of staleOrders) {
      await this.cancel(order.id);
    }
  }

  private async cancel(orderId: string) {
    const cancelled = await this.prisma.$transaction(async (tx) => {
      // updateMany con guarda de estado: si SEATS_RESERVED llegó justo entre
      // el findMany y este punto, count será 0 y no compensamos una orden
      // que ya avanzó (CONFIRMED o FAILED por otra vía).
      const { count } = await tx.order.updateMany({
        where: { id: orderId, status: 'PENDING', sagaStep: 'RESERVING_SEATS' },
        data: { status: 'FAILED', sagaStep: 'COMPENSATING' },
      });

      if (count === 0) {
        return false;
      }

      const payload: ReleaseSeatsCommand = {
        eventId: `${orderId}-timeout-release`,
        orderId,
      };
      await tx.outbox.create({
        data: { topic: TOPICS.RELEASE_SEATS, payload: { ...payload } },
      });

      return true;
    });

    if (cancelled) {
      this.logger.warn(
        `[SAGA] ⏱️  Order ${orderId} sin ${TOPICS.SEATS_RESERVED} tras ${this.timeoutMinutes}min → cancelada, Outbox: ${TOPICS.RELEASE_SEATS}`,
      );
    }
  }
}
