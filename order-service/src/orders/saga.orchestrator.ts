import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { PaymentMock } from './payment.mock';

@Injectable()
export class SagaOrchestrator implements OnModuleInit {
  private readonly logger = new Logger(SagaOrchestrator.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
    private readonly prisma: PrismaService,
    private readonly payment: PaymentMock,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

  async startSaga(dto: CreateOrderDto) {
    const order = await this.prisma.order.create({
      data: {
        eventName: dto.eventName,
        seatCount: dto.seatCount,
        sagaStep: 'RESERVING_SEATS',
      },
    });

    this.logger.log(
      `[SAGA] Order ${order.id} created → emitiendo RESERVE_SEATS`,
    );

    // El eventId lo asigna el productor. El consumidor lo usa para garantizar
    // idempotencia: si recibe el mismo evento dos veces, solo lo procesa una vez.
    this.kafka.emit('RESERVE_SEATS', {
      eventId: order.id, // orderId como eventId: un order → un único RESERVE_SEATS
      orderId: order.id,
      eventName: order.eventName,
      seatCount: order.seatCount,
    });

    return { orderId: order.id, status: order.status };
  }

  async onSeatsReserved(payload: { orderId: string }) {
    this.logger.log(
      `[SAGA] SEATS_RESERVED recibido para order ${payload.orderId} → procesando pago`,
    );

    const paid = this.payment.processPayment();

    if (paid) {
      await this.prisma.order.update({
        where: { id: payload.orderId },
        data: { status: 'CONFIRMED', sagaStep: 'COMPLETED' },
      });
      this.logger.log(`[SAGA] ✅ Order ${payload.orderId} → CONFIRMED`);
    }
  }
}
