import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';
import { ReleaseSeatsCommand, ReserveSeatsCommand } from './events';

@Injectable()
export class SeatService {
  private readonly logger = new Logger(SeatService.name);

  constructor(private readonly prisma: PrismaService) {}

  async reserveSeats(payload: ReserveSeatsCommand) {
    const { eventId, orderId, correlationId, eventName, seatCount } = payload;

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
          // Rechazo de negocio, no un fallo técnico: lo sabemos al instante,
          // así que avisamos a order-service para que compense de inmediato
          // en vez de dejar la orden colgada hasta el timeout de la SAGA.
          // El eventId queda registrado igual — es un evento "procesado".
          await tx.outbox.create({
            data: {
              topic: TOPICS.RESERVE_SEATS_REJECTED,
              payload: { orderId, correlationId, reason: 'NOT_ENOUGH_SEATS' },
            },
          });
          this.logger.warn(
            `[INVENTORY][CID:${correlationId}] Asientos insuficientes para order ${orderId} (pidió ${seatCount}, hay ${available.length}) → Outbox: ${TOPICS.RESERVE_SEATS_REJECTED}`,
          );
          return;
        }

        await tx.seat.updateMany({
          where: { id: { in: available.map((s) => s.id) } },
          data: { reserved: true, orderId },
        });

        // SEATS_RESERVED va al Outbox dentro de la misma transacción:
        // la reserva de asientos y el evento son atómicos.
        await tx.outbox.create({
          data: {
            topic: TOPICS.SEATS_RESERVED,
            payload: { orderId, correlationId },
          },
        });

        this.logger.log(
          `[INVENTORY][CID:${correlationId}] ${seatCount} asientos reservados para order ${orderId} → Outbox: ${TOPICS.SEATS_RESERVED}`,
        );
      });
    } catch (e) {
      if (
        e instanceof Prisma.PrismaClientKnownRequestError &&
        e.code === 'P2002'
      ) {
        this.logger.warn(
          `[INVENTORY][CID:${correlationId}] Evento duplicado ignorado: ${eventId}`,
        );
        return;
      }
      throw e;
    }
  }

  async releaseSeats(payload: ReleaseSeatsCommand) {
    const { eventId, orderId, correlationId } = payload;

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
        this.logger.warn(
          `[INVENTORY][CID:${correlationId}] Evento duplicado ignorado: ${eventId}`,
        );
        return;
      }
      throw e;
    }

    this.logger.log(
      `[INVENTORY][CID:${correlationId}] ↩️  Asientos liberados para order ${orderId}`,
    );
  }
}
