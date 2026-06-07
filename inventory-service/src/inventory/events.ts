export class ReserveSeatsCommand {
  eventId: string;
  orderId: string;
  eventName: string;
  seatCount: number;
}

export class SeatsReservedEvent {
  orderId: string;
}

export class ReleaseSeatsCommand {
  eventId: string;
  orderId: string;
}
