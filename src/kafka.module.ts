import { DynamicModule, Global, Module } from '@nestjs/common';
import { Kafka, KafkaConfig } from 'kafkajs';
import { logCreator } from './log-creator';

@Global()
@Module({})
export class KafkaModule {
  static forRoot(config?: KafkaConfig): DynamicModule {
    config = {
      brokers: (process.env.KAFKA || 'localhost:9092').split(','),
      ssl: true,
      logCreator,
      ...config,
    };

    return {
      module: KafkaModule,
      providers: [
        {
          provide: Kafka,
          useFactory: () => new Kafka(config),
        },
      ],
      exports: [Kafka],
    };
  }
}
