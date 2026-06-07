import { Controller, Get } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

@Controller()
export class AppController {
  @Get()
  getHello(): string {
    return 'inventory-service is running';
  }

  @EventPattern('test.ping')
  handleTestPing(@Payload() data: unknown) {
    console.log('[inventory-service] test.ping received:', data);
  }
}
