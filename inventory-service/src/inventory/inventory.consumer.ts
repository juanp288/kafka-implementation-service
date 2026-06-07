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

  // Compensación: el pago falló en order-service, hay que deshacer la reserva.
  @EventPattern('RELEASE_SEATS')
  async handleReleaseSeats(
    @Payload() data: { eventId: string; orderId: string },
  ) {
    await this.seatService.releaseSeats(data);
  }
}
