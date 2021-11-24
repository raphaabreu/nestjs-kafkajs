import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { Injectable } from '@nestjs/common';
import { Consumer, ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopic, Kafka } from 'kafkajs';

const HEARTBEAT_CHECK_INTERVAL = 1 * 60 * 1000; // 1 minute

@Injectable()
export class KafkaConsumerFactory {
  constructor(private kafka: Kafka) {}

  create(subscribeTopic: ConsumerSubscribeTopic, config: ConsumerConfig, runConfig: ConsumerRunConfig): Consumer {
    const consumer = this.kafka.consumer(config);

    this.start(consumer, subscribeTopic, config, runConfig);

    return consumer;
  }

  private async start(
    consumer: Consumer,
    subscribeTopic: ConsumerSubscribeTopic,
    config: ConsumerConfig,
    runConfig: ConsumerRunConfig,
  ): Promise<void> {
    try {
      await consumer.connect();
      await consumer.subscribe(subscribeTopic);
      await consumer.run(runConfig);
    } catch (error) {
      logger.error('Consumer failed to start, terminating process', error);
      setTimeout(() => process.exit(1), 10000);
      return;
    }

    let lastHeartbeat = new Date();
    consumer.on('consumer.heartbeat', () => {
      lastHeartbeat = new Date();
    });

    const intervalHandler = setInterval(async () => {
      const now = new Date();
      if (lastHeartbeat.getTime() < now.getTime() - HEARTBEAT_CHECK_INTERVAL) {
        logger.warn('Missing heartbeat, restarting consumer');

        clearInterval(intervalHandler);

        await this.stop(consumer);
        await this.start(consumer, subscribeTopic, config, runConfig);
      }
    }, HEARTBEAT_CHECK_INTERVAL);
  }

  private async stop(consumer: Consumer) {
    await consumer.stop();
    await consumer.disconnect();
  }
}

const logger = new StructuredLogger(KafkaConsumerFactory.name);
