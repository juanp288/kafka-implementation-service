export class NotEnoughSeatsException extends Error {
  constructor(orderId: string) {
    super(`Not enough seats for order ${orderId}`);
    this.name = 'NotEnoughSeatsException';
  }
}
