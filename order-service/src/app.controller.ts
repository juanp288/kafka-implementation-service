import { Controller, Get, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {
  constructor(@Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

  @Get()
  getHello(): string {
    return 'order-service is running';
  }

  @Get('test')
  sendTestEvent() {
    this.kafka.emit('test.ping', { ts: Date.now(), from: 'order-service' });
    return { sent: true };
  }
}
