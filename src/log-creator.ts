import { logLevel } from 'kafkajs';
import { StructuredLogger } from '@raphaabreu/nestjs-opensearch-structured-logger';

export function logCreator() {
  const rootLogger = new StructuredLogger('Kafka');

  return ({ level, log }) => {
    const { message, timestamp, logger, ...extra } = log;

    if (extra.data) {
      extra.data = JSON.stringify(extra.data);
    }

    const scoppedLogger = rootLogger.createScope(extra);

    if (extra.error === 'The group is rebalancing, so a rejoin is needed') {
      level = logLevel.INFO;
    }

    switch (level) {
      case logLevel.ERROR:
      case logLevel.NOTHING:
        scoppedLogger.error(message, '');
        break;
      case logLevel.WARN:
        scoppedLogger.warn(message);
        break;
      case logLevel.INFO:
        scoppedLogger.log(message);
        break;
      case logLevel.DEBUG:
        scoppedLogger.debug(message);
        break;
    }
  };
}
