import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { DlqService } from '../dlq/dlq.service';
import { TOPICS } from '../kafka/topics';
import { SeatsReservationRejectedEvent, SeatsReservedEvent } from './events';
import { SagaOrchestrator } from './saga.orchestrator';

@Controller()
export class OrdersConsumer {
  constructor(
    private readonly saga: SagaOrchestrator,
    private readonly dlq: DlqService,
  ) {}

  @EventPattern(TOPICS.SEATS_RESERVED)
  async handleSeatsReserved(
    @Payload() data: SeatsReservedEvent,
    @Ctx() context: KafkaContext,
  ) {
    await this.dlq.withRetry(TOPICS.SEATS_RESERVED, data, context, () =>
      this.saga.onSeatsReserved(data),
    );
  }

  @EventPattern(TOPICS.RESERVE_SEATS_REJECTED)
  async handleReservationRejected(
    @Payload() data: SeatsReservationRejectedEvent,
    @Ctx() context: KafkaContext,
  ) {
    await this.dlq.withRetry(TOPICS.RESERVE_SEATS_REJECTED, data, context, () =>
      this.saga.onReservationRejected(data),
    );
  }
}
