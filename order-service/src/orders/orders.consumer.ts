import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '../kafka/topics';
import { SeatsReservedEvent } from './events';
import { SagaOrchestrator } from './saga.orchestrator';

@Controller()
export class OrdersConsumer {
  constructor(private readonly saga: SagaOrchestrator) {}

  @EventPattern(TOPICS.SEATS_RESERVED)
  async handleSeatsReserved(@Payload() data: SeatsReservedEvent) {
    await this.saga.onSeatsReserved(data);
  }
}
