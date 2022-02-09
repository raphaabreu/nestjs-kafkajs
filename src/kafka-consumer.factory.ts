import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';
import { Injectable } from '@nestjs/common';
import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  EachBatchPayload,
  EachMessagePayload,
  Kafka,
} from 'kafkajs';
import promClient from 'prom-client';

const HEARTBEAT_CHECK_INTERVAL = 1 * 60 * 1000; // 1 minute

export type ExtendedConsumerRunConfig = {
  skipAwaitEach: boolean;
  errorHandler: (error: any) => any;
} & ConsumerRunConfig;

@Injectable()
export class KafkaConsumerFactory {
  private readonly histogram = new promClient.Histogram({
    name: 'kafkajs_consumption_duration',
    help: 'KafkaJs message consumption duration in seconds',
    labelNames: ['topic', 'groupId'],
    buckets: [0.0001, 0.001, 0.01, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  });
  private readonly failureCounter = new promClient.Counter({
    name: 'kafkajs_consumption_failed_count',
    help: 'KafkaJs message consumption failures',
    labelNames: ['topic', 'groupId', 'error'],
  });
  private readonly batchSize = new promClient.Summary({
    name: 'kafkajs_consumption_batch_size',
    help: 'KafkaJs message consumption batch size',
    labelNames: ['topic', 'groupId'],
  });

  constructor(private kafka: Kafka) {}

  create(
    subscribeTopic: ConsumerSubscribeTopic,
    config: ConsumerConfig,
    runConfig: ExtendedConsumerRunConfig,
  ): Consumer {
    const consumer = this.kafka.consumer(config);

    const labels = {
      topic: subscribeTopic.topic.toString(),
      groupId: config.groupId,
    };

    const consumerLogger = new StructuredLogger(`${subscribeTopic.topic.toString()}`);

    consumerLogger.appendScope(labels);

    if (runConfig.eachMessage) {
      const prev = runConfig.eachMessage;

      runConfig.eachMessage = async (payload) => {
        const promise = this.handleEach(labels, prev, consumerLogger, runConfig.errorHandler, payload);

        if (runConfig.skipAwaitEach !== true) {
          await promise;
        }
      };
    }

    if (runConfig.eachBatch) {
      const prev = runConfig.eachBatch;

      runConfig.eachBatch = async (payload) => {
        const promise = this.handleEach(labels, prev, consumerLogger, runConfig.errorHandler, payload);

        if (runConfig.skipAwaitEach !== true) {
          await promise;
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

  private async handleEach<T extends EachBatchPayload | EachMessagePayload>(
    labels: Partial<Record<'topic' | 'groupId', string | number>>,
    prev: (payload: T) => Promise<void>,
    consumerLogger: StructuredLogger,
    errorHandler: (error: any) => any,
    payload: T,
  ): Promise<void> {
    const stopTimer = this.histogram.startTimer(labels);

    const messageCount =
      ((payload as EachBatchPayload).batch && (payload as EachBatchPayload).batch.messages.length) || 1;

    this.batchSize.observe(labels, messageCount);

    try {
      const result = await prev(payload);

      stopTimer();

      return result;
    } catch (error) {
      stopTimer();

      this.failureCounter.inc({ ...labels, error: error.message || error }, messageCount);

      if (errorHandler) {
        const final = errorHandler(error);
        if (final) {
          consumerLogger.error('Failed to consume', final);
          throw final;
        }
      } else {
        consumerLogger.error('Failed to consume', error);
        throw error;
      }

      consumerLogger.error('Ignoring consumption failure', error);
    }
  }
}

const logger = new StructuredLogger(KafkaConsumerFactory.name);
