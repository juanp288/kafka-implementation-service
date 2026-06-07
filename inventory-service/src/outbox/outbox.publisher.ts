import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { Interval } from '@nestjs/schedule';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class OutboxPublisher implements OnModuleInit {
  private readonly logger = new Logger(OutboxPublisher.name);

  constructor(
    private readonly prisma: PrismaService,
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

  // Polling cada segundo: busca eventos pendientes y los publica en Kafka.
  // Si el proceso cae entre el emit y el update de publishedAt, el siguiente
  // ciclo re-emitirá el evento — los consumidores lo manejan con ProcessedEvent.
  @Interval(1000)
  async publishPending() {
    const events = await this.prisma.outbox.findMany({
      where: { publishedAt: null },
      orderBy: { createdAt: 'asc' },
      take: 50,
    });

    for (const event of events) {
      try {
        this.kafka.emit(event.topic, event.payload as object);
        await this.prisma.outbox.update({
          where: { id: event.id },
          data: { publishedAt: new Date() },
        });
        this.logger.log(`[OUTBOX] Publicado: ${event.topic} (${event.id})`);
      } catch (e) {
        this.logger.error(
          `[OUTBOX] Error publicando ${event.id}: ${e.message}`,
        );
      }
    }
  }
}
