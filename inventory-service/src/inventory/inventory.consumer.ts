import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { SeatService } from './seat.service';

@Controller()
export class InventoryConsumer {
  constructor(private readonly seatService: SeatService) {}

  @EventPattern('RESERVE_SEATS')
  async handleReserveSeats(
    @Payload()
    data: {
      eventId: string;
      orderId: string;
      eventName: string;
      seatCount: number;
    },
  ) {
    await this.seatService.reserveSeats(data);
  }
}
