import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            brokers: [process.env.KAFKA_BROKERS ?? 'localhost:9093'],
          },
          producer: { allowAutoTopicCreation: true },
        },
      },
    ]),
  ],
  exports: [ClientsModule],
})
export class KafkaModule {}
