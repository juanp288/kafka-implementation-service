import { Module } from '@nestjs/common';
import { DlqModule } from '../dlq/dlq.module';
import { OutboxModule } from '../outbox/outbox.module';
import { PrismaModule } from '../prisma/prisma.module';
import { InventoryConsumer } from './inventory.consumer';
import { SeederService } from './seeder.service';
import { SeatService } from './seat.service';

@Module({
  imports: [PrismaModule, OutboxModule, DlqModule],
  controllers: [InventoryConsumer],
  providers: [SeatService, SeederService],
})
export class InventoryModule {}
