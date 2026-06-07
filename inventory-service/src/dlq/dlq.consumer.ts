import { Controller, Logger } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { dlqTopic, TOPICS } from '../kafka/topics';
import { PrismaService } from '../prisma/prisma.service';

// Consumer de limpieza: persiste en DeadLetter los mensajes que agotaron sus
// reintentos, para que puedan inspeccionarse o reprocesarse manualmente.
@Controller()
export class DlqConsumer {
  private readonly logger = new Logger(DlqConsumer.name);

  constructor(private readonly prisma: PrismaService) {}

  @EventPattern(dlqTopic(TOPICS.RESERVE_SEATS))
  handleReserveSeatsDlq(@Payload() data: object, @Ctx() context: KafkaContext) {
    return this.persist(TOPICS.RESERVE_SEATS, data, context);
  }

  @EventPattern(dlqTopic(TOPICS.RELEASE_SEATS))
  handleReleaseSeatsDlq(@Payload() data: object, @Ctx() context: KafkaContext) {
    return this.persist(TOPICS.RELEASE_SEATS, data, context);
  }

  private async persist(
    originalTopic: string,
    payload: object,
    context: KafkaContext,
  ) {
    const headers = context.getMessage().headers ?? {};
    const error = headers['x-error']?.toString() ?? 'unknown';
    const retryCount = Number(headers['x-retry-count']?.toString() ?? '0');
    const eventId = (payload as { eventId?: string }).eventId;

    await this.prisma.deadLetter.create({
      data: { topic: originalTopic, eventId, payload, error, retryCount },
    });

    this.logger.error(
      `[DLQ] 💀 Mensaje muerto persistido — topic=${originalTopic} eventId=${eventId ?? 'n/a'} retries=${retryCount}: ${error}`,
    );
  }
}
