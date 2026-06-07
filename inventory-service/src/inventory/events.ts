export class ReserveSeatsCommand {
  eventId: string;
  orderId: string;
  correlationId: string;
  eventName: string;
  seatCount: number;
}

export class SeatsReservedEvent {
  orderId: string;
  correlationId: string;
}

export class SeatsReservationRejectedEvent {
  orderId: string;
  correlationId: string;
  reason: string;
}

export class ReleaseSeatsCommand {
  eventId: string;
  orderId: string;
  correlationId: string;
}
