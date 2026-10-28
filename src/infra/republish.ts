import { v7 as uuidv7 } from 'uuid';
import { createDatabase } from './database/postgres.js';
import { KyselyOrderBookRepository } from './database/kyselyOrderBookRepository.js';
import { KyselyTransactionRunner } from './database/kyselyTransactionRunner.js';
import { HistoryRepublisher } from './messaging/historyRepublisher.js';
import { matchingEventsExchange } from './messaging/outboxEventPublisher.js';
import { connectRabbitMq, rabbitMqUrlFromEnv } from './platform/messaging/connection.js';
import { postgresSettings } from './settings.js';

/*
 * npm run history:republish (make history-republish): publishes every trade, the current snapshot of every
 * book and a replayCompleted marker on matching.replay.* keys, for market-data to rebuild its projections.
 */
const db = createDatabase(postgresSettings());
const connection = await connectRabbitMq(rabbitMqUrlFromEnv());
try {
  const channel = await connection.createConfirmChannel();
  await channel.checkExchange(matchingEventsExchange);
  const republisher = new HistoryRepublisher(db, new KyselyOrderBookRepository(new KyselyTransactionRunner(db)), channel, {
    exchange: matchingEventsExchange,
    correlationId: uuidv7(),
  });
  const result = await republisher.republish();
  console.log(`republished ${result.trades} trades and ${result.snapshots} book snapshots`);
} finally {
  await connection.close();
  await db.destroy();
}
