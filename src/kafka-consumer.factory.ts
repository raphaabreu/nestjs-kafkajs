import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { Injectable } from '@nestjs/common';
import { Consumer, ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopic, Kafka } from 'kafkajs';
import promClient from 'prom-client';

const HEARTBEAT_CHECK_INTERVAL = 1 * 60 * 1000; // 1 minute

@Injectable()
export class KafkaConsumerFactory {
  private readonly histogram = new promClient.Histogram({
    name: 'kafkajs_single_consumption_duration',
    help: 'KafkaJs single message consumption duration in seconds',
    labelNames: ['topic', 'groupId'],
    buckets: [0.0001, 0.001, 0.01, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  });
  private readonly failureCounter = new promClient.Counter({
    name: 'kafkajs_batch_consumption_failed_count',
    help: 'KafkaJs batch message consumption failures',
    labelNames: ['topic', 'groupId', 'error'],
  });
  private readonly batchSize = new promClient.Summary({
    name: 'kafkajs_batch_consumption_size',
    help: 'KafkaJs batch message consumption size',
    labelNames: ['topic', 'groupId'],
  });

  constructor(private kafka: Kafka) {}

  create(subscribeTopic: ConsumerSubscribeTopic, config: ConsumerConfig, runConfig: ConsumerRunConfig): Consumer {
    const consumer = this.kafka.consumer(config);

    if (runConfig.eachMessage) {
      const prev = runConfig.eachMessage;

      runConfig.eachMessage = async (payload) => {
        const labels = {
          topic: subscribeTopic.topic.toString(),
          groupId: config.groupId,
        };
        const stopTimer = this.histogram.startTimer({
          topic: subscribeTopic.topic.toString(),
          groupId: config.groupId,
        });

        try {
          const result = await prev(payload);

          stopTimer();

          return result;
        } catch (error) {
          stopTimer();

          this.failureCounter.inc({ ...labels, error: error.message || error });

          throw error;
        }
      };
    }

    if (runConfig.eachBatch) {
      const prev = runConfig.eachBatch;

      runConfig.eachBatch = async (payload) => {
        const labels = {
          topic: subscribeTopic.topic.toString(),
          groupId: config.groupId,
        };
        const stopTimer = this.histogram.startTimer({
          topic: subscribeTopic.topic.toString(),
          groupId: config.groupId,
        });

        this.batchSize.observe(labels, payload.batch.messages.length);

        try {
          const result = await prev(payload);

          stopTimer();

          return result;
        } catch (error) {
          stopTimer();

          this.failureCounter.inc({ ...labels, error: error.message || error });

          throw error;
        }
      };
    }
    this.start(consumer, subscribeTopic, config, runConfig);

    return consumer;
  }

  private async start(
    consumer: Consumer,
    subscribeTopic: ConsumerSubscribeTopic,
    config: ConsumerConfig,
    runConfig: ConsumerRunConfig,
  ): Promise<boolean> {
    try {
      await consumer.connect();
      await consumer.subscribe(subscribeTopic);
      await consumer.run(runConfig);
    } catch (error) {
      logger.error('Consumer failed to start, terminating process', error);
      setTimeout(() => process.exit(1), 10000);
      return false;
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

        try {
          await Promise.race<void>([
            this.restart(consumer, subscribeTopic, config, runConfig),
            new Promise<void>((resolve, reject) => setTimeout(() => reject(), 10000)),
          ]);
        } catch (error) {
          logger.error('Consumer failed to restart, terminating process', error);
          setTimeout(() => process.exit(1), 10000);
        }
      }
    }, HEARTBEAT_CHECK_INTERVAL);

    logger.log('Started consumer');
    return true;
  }

  private async restart(
    consumer: Consumer,
    subscribeTopic: ConsumerSubscribeTopic,
    config: ConsumerConfig,
    runConfig: ConsumerRunConfig,
  ): Promise<void> {
    await consumer.stop();
    await consumer.disconnect();
    const started = await this.start(consumer, subscribeTopic, config, runConfig);

    if (!started) {
      throw new Error('Consumer failed to start');
    }
  }
}

const logger = new StructuredLogger(KafkaConsumerFactory.name);
