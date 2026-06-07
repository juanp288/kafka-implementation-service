import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

interface ReserveSeatsPayload {
  eventId: string;
  orderId: string;
  eventName: string;
  seatCount: number;
}

@Injectable()
export class SeatService implements OnModuleInit {
  private readonly logger = new Logger(SeatService.name);

  constructor(
    private readonly prisma: PrismaService,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
    await this.seedSeatsIfEmpty();
  }

  private async seedSeatsIfEmpty() {
    const count = await this.prisma.seat.count();
    if (count === 0) {
      await this.prisma.seat.createMany({
        data: Array.from({ length: 50 }, () => ({ eventName: 'Test Event' })),
      });
      this.logger.log('Seeded 50 seats for "Test Event"');
    }
  }

  async reserveSeats(payload: ReserveSeatsPayload) {
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
          throw new Error(`NOT_ENOUGH_SEATS`);
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
      if (e.message === 'NOT_ENOUGH_SEATS') {
        this.logger.warn(
          `[INVENTORY] Asientos insuficientes para order ${orderId}`,
        );
        return;
      }
      throw e;
    }

    this.logger.log(
      `[INVENTORY] ${seatCount} asientos reservados para order ${orderId} → emitiendo SEATS_RESERVED`,
    );
    this.kafka.emit('SEATS_RESERVED', { orderId });
  }
}
