import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { DlqService } from '../dlq/dlq.service';
import { TOPICS } from '../kafka/topics';
import { ReleaseSeatsCommand, ReserveSeatsCommand } from './events';
import { SeatService } from './seat.service';

@Controller()
export class InventoryConsumer {
  constructor(
    private readonly seatService: SeatService,
    private readonly dlq: DlqService,
  ) {}

  @EventPattern(TOPICS.RESERVE_SEATS)
  async handleReserveSeats(
    @Payload() data: ReserveSeatsCommand,
    @Ctx() context: KafkaContext,
  ) {
    await this.dlq.withRetry(TOPICS.RESERVE_SEATS, data, context, () =>
      this.seatService.reserveSeats(data),
    );
  }

  // Compensación: el pago falló en order-service, hay que deshacer la reserva.
  @EventPattern(TOPICS.RELEASE_SEATS)
  async handleReleaseSeats(
    @Payload() data: ReleaseSeatsCommand,
    @Ctx() context: KafkaContext,
  ) {
    await this.dlq.withRetry(TOPICS.RELEASE_SEATS, data, context, () =>
      this.seatService.releaseSeats(data),
    );
  }
}
