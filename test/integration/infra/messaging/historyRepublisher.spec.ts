import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import type { TestBroker } from '../../../support/rabbitmq.js';
import { createTestBroker } from '../../../support/rabbitmq.js';
import type { TestDatabase } from '../../../support/database.js';
import { createTestDatabase } from '../../../support/database.js';
import { OrderBook } from '../../../../src/domain/entities/orderBook.js';
import { AccountId } from '../../../../src/domain/valueObjects/accountId.js';
import { MarketId } from '../../../../src/domain/valueObjects/marketId.js';
import { OrderId } from '../../../../src/domain/valueObjects/orderId.js';
import { Price } from '../../../../src/domain/valueObjects/price.js';
import { Quantity } from '../../../../src/domain/valueObjects/quantity.js';
import { Side } from '../../../../src/domain/valueObjects/side.js';
import { KyselyOrderBookRepository } from '../../../../src/infra/database/kyselyOrderBookRepository.js';
import { KyselyTransactionRunner } from '../../../../src/infra/database/kyselyTransactionRunner.js';
import { HistoryRepublisher } from '../../../../src/infra/messaging/historyRepublisher.js';
import { loadEnvelopeValidator } from '../../../../src/infra/platform/messaging/envelope.js';
import { loadSchemaValidator } from '../../../../src/infra/platform/messaging/schemas.js';

let database: TestDatabase;
let broker: TestBroker;

beforeAll(async () => {
  database = await createTestDatabase();
  broker = await createTestBroker();
});

afterAll(async () => {
  await broker.close();
  await database.destroy();
});

const incoming = (side: string, quantity: string, price: string) => ({
  orderId: OrderId.from(randomUUID()),
  accountId: AccountId.from(randomUUID()),
  side: Side.from(side),
  quantity: Quantity.from(quantity),
  price: Price.from(price),
  acceptedAt: new Date('2026-10-28T12:00:00Z'),
});

let tradeNumber = 0;
const nextTradeId = () => `0199d5f4-2b9a-7c3e-9a51-${String((tradeNumber += 1)).padStart(12, '0')}`;

const bindQueue = async (pattern: string): Promise<string> => {
  const queue = broker.uniqueName('test.replay');
  await broker.channel.assertQueue(queue, { exclusive: true });
  await broker.channel.bindQueue(queue, broker.exchange, pattern);
  return queue;
};

const drain = async (queue: string, count: number) => {
  const validate = loadEnvelopeValidator();
  const messages = [];
  for (let index = 0; index < count; index += 1) {
    const message = await broker.nextMessage(queue);
    messages.push({ routingKey: message.fields.routingKey, envelope: validate(JSON.parse(message.content.toString())) });
  }
  return messages;
};

describe('HistoryRepublisher', () => {
  test('Should republish every trade in book order, the current book and a final marker, only on matching.replay.* keys', async () => {
    // Given: three trades (two in one cascade), paged two by two, and 2 @ 100 left resting
    const runner = new KyselyTransactionRunner(database.db);
    const repository = new KyselyOrderBookRepository(runner);
    const book = OrderBook.empty(MarketId.from('BTC-USD'));
    const at = (second: number) => new Date(`2026-10-28T12:00:0${second}Z`);
    book.place(incoming('sell', '1', '100'), at(1), nextTradeId);
    book.place(incoming('sell', '1', '101'), at(2), nextTradeId);
    book.place(incoming('buy', '2', '101'), at(3), nextTradeId);
    book.place(incoming('sell', '3', '100'), at(4), nextTradeId);
    book.place(incoming('buy', '1', '100'), at(5), nextTradeId);
    await runner.run(() => repository.save(book));
    const replay = await bindQueue('matching.replay.#');
    const regular = await bindQueue('matching.tradeExecuted.*');
    const republisher = new HistoryRepublisher(database.db, repository, await broker.connection.createConfirmChannel(), {
      exchange: broker.exchange,
      correlationId: randomUUID(),
      pageSize: 2,
    });

    // When
    const result = await republisher.republish();

    // Then
    expect(result).toEqual({ trades: 3, snapshots: 1 });
    const messages = await drain(replay, 5);
    expect(messages.map((message) => message.routingKey)).toEqual([
      'matching.replay.tradeExecuted.BTC-USD',
      'matching.replay.tradeExecuted.BTC-USD',
      'matching.replay.tradeExecuted.BTC-USD',
      'matching.replay.bookSnapshot.BTC-USD',
      'matching.replay.completed',
    ]);
    const validateTrade = loadSchemaValidator('src/infra/schemas/published/events/matching.tradeExecuted.v1.schema.json');
    expect(messages.slice(0, 3).map((message) => validateTrade(message.envelope.payload))).toMatchObject([
      { price: '100.00000000', bookSequence: 3, executedAt: '2026-10-28T12:00:03.000Z' },
      { price: '101.00000000', bookSequence: 3 },
      { price: '100.00000000', quantity: '1.00000000', bookSequence: 5 },
    ]);
    expect(messages[3]?.envelope.payload).toEqual({ marketId: 'BTC-USD', sequence: 5, bids: [], asks: [{ price: '100.00000000', quantity: '2.00000000' }] });
    expect(messages[4]?.envelope).toMatchObject({ type: 'matching.replayCompleted', payload: { trades: 3, snapshots: 1 } });
    expect(await broker.channel.checkQueue(regular)).toMatchObject({ messageCount: 0 });
  });
});
