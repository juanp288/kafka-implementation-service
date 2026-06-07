import { Injectable } from '@nestjs/common';

@Injectable()
export class PaymentMock {
  processPayment(): boolean {
    // Falla el 50% de las veces para poder observar la compensación en acción.
    return Math.random() >= 0.5;
  }
}
