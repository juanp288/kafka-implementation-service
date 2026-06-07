import { Module } from '@nestjs/common';
import { DlqModule } from '../dlq/dlq.module';
import { OutboxModule } from '../outbox/outbox.module';
import { PrismaModule } from '../prisma/prisma.module';
import { OrdersConsumer } from './orders.consumer';
import { OrdersController } from './orders.controller';
import { PaymentMock } from './payment.mock';
import { SagaOrchestrator } from './saga.orchestrator';
import { SagaTimeoutService } from './saga-timeout.service';

@Module({
  imports: [PrismaModule, OutboxModule, DlqModule],
  controllers: [OrdersController, OrdersConsumer],
  providers: [SagaOrchestrator, PaymentMock, SagaTimeoutService],
})
export class OrdersModule {}
