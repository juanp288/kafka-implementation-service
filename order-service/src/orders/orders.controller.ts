import { Body, Controller, Get, Param, Post } from '@nestjs/common';
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
}
