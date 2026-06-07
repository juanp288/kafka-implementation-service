import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PrismaModule } from '../prisma/prisma.module';
import { InventoryConsumer } from './inventory.consumer';
import { SeederService } from './seeder.service';
import { SeatService } from './seat.service';

@Module({
  imports: [KafkaModule, PrismaModule],
  controllers: [InventoryConsumer],
  providers: [SeatService, SeederService],
})
export class InventoryModule {}
