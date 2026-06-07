import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PrismaModule } from '../prisma/prisma.module';
import { OutboxPublisher } from './outbox.publisher';

// KafkaModule vive aquí: es el único punto del servicio que habla con Kafka.
@Module({
  imports: [KafkaModule, PrismaModule],
  providers: [OutboxPublisher],
})
export class OutboxModule {}
