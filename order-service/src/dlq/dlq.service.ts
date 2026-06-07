import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientKafka, KafkaContext } from '@nestjs/microservices';
import { dlqTopic } from '../kafka/topics';

const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 2000;
const RETRY_COUNT_HEADER = 'x-retry-count';

// Reintentos controlados a nivel de aplicación: si el handler falla, el mensaje
// se re-emite al mismo topic con un contador en los headers (con backoff). Al
// superar MAX_RETRIES se enruta a `<topic>.DLQ` para inspección manual.
//
// Esto evita el reenvío indefinido de Kafka: dejamos que el offset original
// se confirme normalmente (no relanzamos la excepción) y el control de
// reintentos pasa a estar en nuestras manos.
@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);

  constructor(@Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka) {}

  async withRetry(
    topic: string,
    payload: object,
    context: KafkaContext,
    handler: () => Promise<void>,
  ): Promise<void> {
    try {
      await handler();
    } catch (error) {
      await this.retryOrSendToDlq(topic, payload, context, error as Error);
    }
  }

  private async retryOrSendToDlq(
    topic: string,
    payload: object,
    context: KafkaContext,
    error: Error,
  ) {
    const headers = this.readHeaders(context);
    const retryCount = Number(headers[RETRY_COUNT_HEADER] ?? '0');
    const correlationId =
      (payload as { correlationId?: string }).correlationId ?? 'n/a';

    if (retryCount < MAX_RETRIES) {
      const nextRetryCount = retryCount + 1;
      const backoffMs = nextRetryCount * BACKOFF_BASE_MS;

      this.logger.warn(
        `[DLQ][CID:${correlationId}] ${topic} → fallo (${error.message}). Reintento ${nextRetryCount}/${MAX_RETRIES} en ${backoffMs}ms`,
      );

      await this.sleep(backoffMs);
      this.kafka.emit(topic, {
        value: payload,
        headers: { ...headers, [RETRY_COUNT_HEADER]: String(nextRetryCount) },
      });
      return;
    }

    const target = dlqTopic(topic);
    this.logger.error(
      `[DLQ][CID:${correlationId}] ${topic} → ${MAX_RETRIES} reintentos agotados. Enviando a ${target}: ${error.message}`,
    );
    this.kafka.emit(target, {
      value: payload,
      headers: {
        ...headers,
        'x-original-topic': topic,
        'x-error': error.message,
        [RETRY_COUNT_HEADER]: String(retryCount),
      },
    });
  }

  private readHeaders(context: KafkaContext): Record<string, string> {
    const raw = context.getMessage().headers ?? {};
    return Object.fromEntries(
      Object.entries(raw)
        .filter((entry): entry is [string, Buffer | string] => entry[1] != null)
        .map(([key, value]) => [key, value.toString()]),
    );
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
