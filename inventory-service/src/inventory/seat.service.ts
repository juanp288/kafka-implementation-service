import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { PrismaService } from '../prisma/prisma.service';

interface ReserveSeatsPayload {
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
    const { orderId, eventName, seatCount } = payload;

    const available = await this.prisma.seat.findMany({
      where: { eventName, reserved: false },
      take: seatCount,
    });

    if (available.length < seatCount) {
      this.logger.warn(
        `[INVENTORY] No hay suficientes asientos para order ${orderId}`,
      );
      return;
    }

    await this.prisma.seat.updateMany({
      where: { id: { in: available.map((s) => s.id) } },
      data: { reserved: true, orderId },
    });

    this.logger.log(
      `[INVENTORY] ${seatCount} asientos reservados para order ${orderId} → emitiendo SEATS_RESERVED`,
    );
    this.kafka.emit('SEATS_RESERVED', { orderId });
  }
}
