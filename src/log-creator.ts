import { logLevel } from 'kafkajs';
import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';

const messagesAsInfo = [
  'The group is rebalancing, so a rejoin is needed',
  'Connection timeout',
  'Connection error: Client network socket disconnected before secure TLS connection was established',
  'Failed to connect to seed broker, trying another broker from the list: Connection timeout',
];

export function logCreator() {
  const rootLogger = new StructuredLogger('Kafka');

  return ({ level, log }) => {
    const { message, timestamp, logger, ...extra } = log;

    if (extra.data) {
      extra.data = JSON.stringify(extra.data);
    }

    const scopedLogger = rootLogger.createScope(extra);

    if (messagesAsInfo.includes(extra.error)) {
      level = logLevel.INFO;
    }

    switch (level) {
      case logLevel.ERROR:
        scopedLogger.error(message, '');
        break;
      case logLevel.WARN:
        scopedLogger.warn(message);
        break;
      case logLevel.NOTHING:
      case logLevel.INFO:
        scopedLogger.log(message);
        break;
      case logLevel.DEBUG:
        scopedLogger.debug(message);
        break;
    }
  };
}
