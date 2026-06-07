import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';
import { ReleaseSeatsCommand, ReserveSeatsCommand } from './events';
import { NotEnoughSeatsException } from './exceptions/not-enough-seats.exception';

@Injectable()
export class SeatService {
  private readonly logger = new Logger(SeatService.name);

  constructor(private readonly prisma: PrismaService) {}

  async reserveSeats(payload: ReserveSeatsCommand) {
    const { eventId, orderId, eventName, seatCount } = payload;

    try {
      await this.prisma.$transaction(async (tx) => {
        // Intentamos insertar el eventId. Si ya existe (P2002), la transacción
        // entera se revierte y el catch lo maneja → procesamiento idempotente.
        await tx.processedEvent.create({ data: { eventId } });

        const available = await tx.seat.findMany({
          where: { eventName, reserved: false },
          take: seatCount,
        });

        if (available.length < seatCount) {
          throw new NotEnoughSeatsException(orderId);
        }

        await tx.seat.updateMany({
          where: { id: { in: available.map((s) => s.id) } },
          data: { reserved: true, orderId },
        });

        // SEATS_RESERVED va al Outbox dentro de la misma transacción:
        // la reserva de asientos y el evento son atómicos.
        await tx.outbox.create({
          data: { topic: TOPICS.SEATS_RESERVED, payload: { orderId } },
        });
      });
    } catch (e) {
      if (
        e instanceof Prisma.PrismaClientKnownRequestError &&
        e.code === 'P2002'
      ) {
        this.logger.warn(`[INVENTORY] Evento duplicado ignorado: ${eventId}`);
        return;
      }
      if (e instanceof NotEnoughSeatsException) {
        this.logger.warn(`[INVENTORY] ${e.message}`);
        return;
      }
      throw e;
    }

    this.logger.log(
      `[INVENTORY] ${seatCount} asientos reservados para order ${orderId} → Outbox: ${TOPICS.SEATS_RESERVED}`,
    );
  }

  async releaseSeats(payload: ReleaseSeatsCommand) {
    const { eventId, orderId } = payload;

    try {
      await this.prisma.$transaction(async (tx) => {
        // Mismo patrón de idempotencia.
        await tx.processedEvent.create({ data: { eventId } });

        await tx.seat.updateMany({
          where: { orderId },
          data: { reserved: false, orderId: null },
        });
      });
    } catch (e) {
      if (
        e instanceof Prisma.PrismaClientKnownRequestError &&
        e.code === 'P2002'
      ) {
        this.logger.warn(`[INVENTORY] Evento duplicado ignorado: ${eventId}`);
        return;
      }
      throw e;
    }

    this.logger.log(`[INVENTORY] ↩️  Asientos liberados para order ${orderId}`);
  }
}
