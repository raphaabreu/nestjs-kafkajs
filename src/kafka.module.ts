import { DynamicModule, Global, Module } from '@nestjs/common';
import { Kafka, KafkaConfig } from 'kafkajs';
import { KafkaConsumerFactory } from './kafka-consumer.factory';
import { logCreator } from './log-creator';

@Global()
@Module({})
export class KafkaModule {
  static forRoot(config?: Partial<KafkaConfig>): DynamicModule {
    const guessSsl = (process.env.KAFKA || 'localhost:9092').indexOf('localhost') === -1;

    const final = {
      brokers: (process.env.KAFKA || 'localhost:9092').split(','),
      ssl: guessSsl,
      logCreator,
      ...config,
    };

    return {
      module: KafkaModule,
      providers: [
        {
          provide: Kafka,
          useFactory: () => new Kafka(final),
        },
        KafkaConsumerFactory,
      ],
      exports: [Kafka, KafkaConsumerFactory],
    };
  }
}
