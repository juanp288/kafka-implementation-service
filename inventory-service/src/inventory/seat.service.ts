import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { Prisma } from '@prisma/client';
import { TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';
import {
  ReleaseSeatsCommand,
  ReserveSeatsCommand,
  SeatsReservedEvent,
} from './events';
import { NotEnoughSeatsException } from './exceptions/not-enough-seats.exception';

@Injectable()
export class SeatService implements OnModuleInit {
  private readonly logger = new Logger(SeatService.name);

  constructor(
    private readonly prisma: PrismaService,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

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
          // Lanzamos para revertir la transacción y no emitir SEATS_RESERVED
          throw new NotEnoughSeatsException(orderId);
        }

        await tx.seat.updateMany({
          where: { id: { in: available.map((s) => s.id) } },
          data: { reserved: true, orderId },
        });
      });
    } catch (e) {
      if (
        e instanceof Prisma.PrismaClientKnownRequestError &&
        e.code === 'P2002'
      ) {
        // El evento ya fue procesado antes (eventId duplicado) → ignorar.
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
      `[INVENTORY] ${seatCount} asientos reservados para order ${orderId} → emitiendo ${TOPICS.SEATS_RESERVED}`,
    );

    const event: SeatsReservedEvent = { orderId };
    this.kafka.emit(TOPICS.SEATS_RESERVED, event);
  }

  async releaseSeats(payload: ReleaseSeatsCommand) {
    const { eventId, orderId } = payload;

    try {
      await this.prisma.$transaction(async (tx) => {
        // Mismo patrón de idempotencia: si RELEASE_SEATS llega dos veces,
        // solo la primera ejecución libera los asientos.
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
