import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PrismaService } from '../prisma/prisma.service';
import { OrdersController } from './orders.controller';
import { PaymentMock } from './payment.mock';
import { SagaOrchestrator } from './saga.orchestrator';

@Module({
  imports: [KafkaModule],
  controllers: [OrdersController],
  providers: [SagaOrchestrator, PaymentMock, PrismaService],
})
export class OrdersModule {}
