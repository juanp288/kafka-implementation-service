import { Module } from '@nestjs/common';
import { OutboxModule } from '../outbox/outbox.module';
import { PrismaModule } from '../prisma/prisma.module';
import { InventoryConsumer } from './inventory.consumer';
import { SeederService } from './seeder.service';
import { SeatService } from './seat.service';

@Module({
  imports: [PrismaModule, OutboxModule],
  controllers: [InventoryConsumer],
  providers: [SeatService, SeederService],
})
export class InventoryModule {}
