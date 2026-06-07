import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PrismaModule } from '../prisma/prisma.module';
import { DlqConsumer } from './dlq.consumer';
import { DlqService } from './dlq.service';

@Module({
  imports: [KafkaModule, PrismaModule],
  controllers: [DlqConsumer],
  providers: [DlqService],
  exports: [DlqService],
})
export class DlqModule {}
