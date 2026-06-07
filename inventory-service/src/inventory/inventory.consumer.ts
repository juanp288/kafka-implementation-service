import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '../kafka/topics';
import { ReleaseSeatsCommand, ReserveSeatsCommand } from './events';
import { SeatService } from './seat.service';

@Controller()
export class InventoryConsumer {
  constructor(private readonly seatService: SeatService) {}

  @EventPattern(TOPICS.RESERVE_SEATS)
  async handleReserveSeats(@Payload() data: ReserveSeatsCommand) {
    await this.seatService.reserveSeats(data);
  }

  // Compensación: el pago falló en order-service, hay que deshacer la reserva.
  @EventPattern(TOPICS.RELEASE_SEATS)
  async handleReleaseSeats(@Payload() data: ReleaseSeatsCommand) {
    await this.seatService.releaseSeats(data);
  }
}
