import Fastify from 'fastify';
import { activeMarkets } from './catalog/activeMarkets.js';
import { buildMatchingCore, serviceName } from './composition.js';
import { createDatabase, PostgresProbe } from './database/postgres.js';
import { registerHealthRoutes } from './http/health.js';
import { classifyMatchingFailure } from './messaging/consumers/consumedEvent.js';
import { marketOrdersHandler, marketOrdersQueue } from './messaging/consumers/marketOrdersConsumer.js';
import { matchingEventsExchange } from './messaging/outboxEventPublisher.js';
import type { AmqpConnection } from './platform/messaging/connection.js';
import { connectRabbitMq, rabbitMqUrlFromEnv } from './platform/messaging/connection.js';
import { Consumer } from './platform/messaging/consumer.js';
import { loadEnvelopeValidator } from './platform/messaging/envelope.js';
import { OutboxRelay } from './platform/messaging/outbox.js';
import { declareConsumerQueue } from './platform/messaging/topology.js';
import { healthPortSetting, postgresSettings } from './settings.js';

const outboxLockKey = 7_004;

const db = createDatabase(postgresSettings());
const core = buildMatchingCore({ db });
const validate = loadEnvelopeValidator();
const markets = activeMarkets();
const log = Fastify({ logger: true }).log;

const startMarketConsumer = async (connection: AmqpConnection, marketId: string): Promise<Consumer> => {
  const channel = await connection.createChannel();
  const definition = marketOrdersQueue(marketId);
  for (const binding of definition.bindings) await channel.checkExchange(binding.exchange);
  await declareConsumerQueue(channel, definition);
  const consumer = new Consumer(core.runner, channel, { definition, handler: marketOrdersHandler(core), validate, classify: classifyMatchingFailure, prefetch: 1 });
  await consumer.start();
  return consumer;
};

const connection = await connectRabbitMq(rabbitMqUrlFromEnv());
let brokerUp = true;
const relay = new OutboxRelay(db, await connection.createConfirmChannel(), { exchange: matchingEventsExchange, lockKey: outboxLockKey });
relay.start();
const consumers = await Promise.all(markets.map((marketId) => startMarketConsumer(connection, marketId)));

const healthServer = Fastify({ logger: false });
registerHealthRoutes(healthServer, serviceName, [new PostgresProbe(db), { dependency: 'rabbitmq', isHealthy: async () => brokerUp }]);
await healthServer.listen({ host: '0.0.0.0', port: healthPortSetting() });
log.info({ markets }, 'matching markets');

let stopping = false;
const shutdown = async (): Promise<void> => {
  stopping = true;
  relay.stop();
  await Promise.all(consumers.map((consumer) => consumer.stop().catch(() => undefined)));
  await connection.close().catch(() => undefined);
  await healthServer.close();
  await db.destroy();
};

/** No in-process reconnect: on a lost broker connection the worker exits and the container restarts it (outbox + inbox make it lossless). */
const exitOnBrokerLoss = (reason: unknown): void => {
  if (stopping) return;
  brokerUp = false;
  log.error({ detail: reason instanceof Error ? reason.message : reason }, 'rabbitmq connection lost, exiting');
  // Exit at once: closing channels on a dead connection can hang, and the outbox + inbox make a hard exit safe.
  process.exit(1);
};
connection.on('error', (error: unknown) => exitOnBrokerLoss(error));
connection.on('close', () => exitOnBrokerLoss('closed'));

process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
