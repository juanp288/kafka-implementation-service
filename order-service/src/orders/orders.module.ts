import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PrismaModule } from '../prisma/prisma.module';
import { OrdersConsumer } from './orders.consumer';
import { OrdersController } from './orders.controller';
import { PaymentMock } from './payment.mock';
import { SagaOrchestrator } from './saga.orchestrator';

@Module({
  imports: [KafkaModule, PrismaModule],
  controllers: [OrdersController, OrdersConsumer],
  providers: [SagaOrchestrator, PaymentMock],
})
export class OrdersModule {}
