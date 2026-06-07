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
    const { orderId } = payload;
    this.logger.log(
      `[SAGA] SEATS_RESERVED recibido para order ${orderId} → procesando pago`,
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

    // Compensación: el pago falló, hay que deshacer la reserva de asientos.
    // Primero marcamos la orden como FAILED y luego emitimos el comando de rollback.
    // El orden importa: si el servicio cae entre estos dos pasos, la orden queda
    // en FAILED pero los asientos reservados — un estado inconsistente aceptable
    // porque RELEASE_SEATS puede re-emitirse manualmente o con un job de limpieza.
    await this.prisma.order.update({
      where: { id: orderId },
      data: { status: 'FAILED', sagaStep: 'COMPENSATING' },
    });

    this.logger.log(
      `[SAGA] ❌ Pago fallido para order ${orderId} → emitiendo RELEASE_SEATS`,
    );

    // El eventId distingue este evento del RESERVE_SEATS del mismo orderId.
    this.kafka.emit('RELEASE_SEATS', {
      eventId: `${orderId}-release`,
      orderId,
    });
  }
}
