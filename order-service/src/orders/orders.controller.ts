import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { SagaOrchestrator } from './saga.orchestrator';

@Controller('orders')
export class OrdersController {
  constructor(
    private readonly saga: SagaOrchestrator,
    private readonly prisma: PrismaService,
  ) {}

  @Post()
  create(@Body() dto: CreateOrderDto) {
    return this.saga.startSaga(dto);
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.prisma.order.findUniqueOrThrow({ where: { id } });
  }

  // Kafka: inventory-service responde aquí cuando los asientos están reservados
  @EventPattern('SEATS_RESERVED')
  async handleSeatsReserved(@Payload() data: { orderId: string }) {
    await this.saga.onSeatsReserved(data);
  }
}
