import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { Injectable } from '@nestjs/common';
import { Consumer, ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopic, Kafka } from 'kafkajs';

let consumers: Consumer[] = [];
const HEARTBEAT_CHECK_INTERVAL = 1 * 60 * 1000; // 1 minute

@Injectable()
export class KafkaConsumerFactory {
  constructor(private kafka: Kafka) {}

  async start(
    subscribeTopic: ConsumerSubscribeTopic,
    config: ConsumerConfig,
    runConfig: ConsumerRunConfig,
  ): Promise<void> {
    const consumer = this.kafka.consumer(config);

    try {
      await consumer.connect();
      await consumer.subscribe(subscribeTopic);
      await consumer.run(runConfig);
      consumers.push(consumer);
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
        await this.start(subscribeTopic, config, runConfig);
      }
    }, HEARTBEAT_CHECK_INTERVAL);
  }

  private async stop(consumer: Consumer) {
    await consumer.stop();
    await consumer.disconnect();
    consumers = consumers.filter((c) => c !== consumer);
  }
}

const logger = new StructuredLogger(KafkaConsumerFactory.name);
