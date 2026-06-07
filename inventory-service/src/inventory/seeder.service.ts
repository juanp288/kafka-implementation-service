import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class SeederService implements OnModuleInit {
  private readonly logger = new Logger(SeederService.name);

  constructor(private readonly prisma: PrismaService) {}

  async onModuleInit() {
    const count = await this.prisma.seat.count();
    if (count === 0) {
      await this.prisma.seat.createMany({
        data: Array.from({ length: 50 }, () => ({ eventName: 'Test Event' })),
      });
      this.logger.log('Seeded 50 seats for "Test Event"');
    }
  }
}
