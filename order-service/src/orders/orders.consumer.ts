import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { DlqService } from '../dlq/dlq.service';
import { TOPICS } from '../kafka/topics';
import { SeatsReservedEvent } from './events';
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
}
