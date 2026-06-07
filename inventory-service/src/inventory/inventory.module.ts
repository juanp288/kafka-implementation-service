import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PrismaService } from '../prisma/prisma.service';
import { InventoryConsumer } from './inventory.consumer';
import { SeatService } from './seat.service';

@Module({
  imports: [KafkaModule],
  controllers: [InventoryConsumer],
  providers: [SeatService, PrismaService],
})
export class InventoryModule {}
