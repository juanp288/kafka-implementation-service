import { Injectable } from '@nestjs/common';

@Injectable()
export class PaymentMock {
  // Paso 2: el pago siempre es exitoso.
  // Paso 4: cambiar a Math.random() < 0.5 para simular fallos y disparar compensación.
  processPayment(): boolean {
    return true;
  }
}
