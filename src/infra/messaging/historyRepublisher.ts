import type { ConfirmChannel } from 'amqplib';
import type { Kysely, Selectable } from 'kysely';
import { MarketId } from '../../domain/valueObjects/marketId.js';
import type { Database, TradeTable } from '../database/database.js';
import type { KyselyOrderBookRepository } from '../database/kyselyOrderBookRepository.js';
import { createEnvelope } from '../platform/messaging/envelope.js';
import { loadSchemaValidator } from '../platform/messaging/schemas.js';

export const replayRoutingPrefix = 'matching.replay';

export interface HistoryRepublisherOptions {
  readonly exchange: string;
  readonly correlationId: string;
  readonly pageSize?: number;
}

export interface RepublishResult {
  readonly trades: number;
  readonly snapshots: number;
}

const validatorOf = (type: string) => loadSchemaValidator<Record<string, unknown>>(`src/infra/schemas/published/events/${type}.v1.schema.json`);

const tradePayload = (row: Selectable<TradeTable>) => ({
  tradeId: row.trade_id,
  marketId: row.market_id,
  buyOrderId: row.buy_order_id,
  sellOrderId: row.sell_order_id,
  buyAccountId: row.buy_account_id,
  sellAccountId: row.sell_account_id,
  side: row.side,
  quantity: String(row.quantity),
  price: String(row.price),
  executedAt: row.executed_at.toISOString(),
  bookSequence: Number(row.book_sequence),
});

/**
 * Republishes the whole trade history and the current state of every book, so a consumer can rebuild its
 * projections from the source of truth (the broker keeps no history). Routing keys start with
 * matching.replay., which no regular binding matches, and the last message is a replayCompleted marker.
 * Publishes straight to the exchange (no outbox): it changes nothing, so it can simply run again.
 */
export class HistoryRepublisher {
  private readonly validateTrade = validatorOf('matching.tradeExecuted');
  private readonly validateSnapshot = validatorOf('matching.bookSnapshot');
  private readonly validateCompleted = validatorOf('matching.replayCompleted');

  constructor(
    private readonly db: Kysely<Database>,
    private readonly books: KyselyOrderBookRepository,
    private readonly channel: ConfirmChannel,
    private readonly options: HistoryRepublisherOptions,
  ) {}

  async republish(): Promise<RepublishResult> {
    const markets = (await this.db.selectFrom('book').select('market_id').orderBy('market_id').execute()).map((row) => MarketId.from(row.market_id));
    let trades = 0;
    for (const market of markets) trades += await this.republishTrades(market);
    let snapshots = 0;
    for (const market of markets) snapshots += await this.republishSnapshot(market);
    const result = { trades, snapshots };
    this.publish('matching.replayCompleted', `${replayRoutingPrefix}.completed`, this.validateCompleted({ ...result }), new Date());
    await this.channel.waitForConfirms();
    return result;
  }

  /** Pages through the market's trades in book order (keyset on book_sequence, trade_id). */
  private async republishTrades(market: MarketId): Promise<number> {
    let count = 0;
    let after: readonly [string, string] = ['0', '00000000-0000-0000-0000-000000000000'];
    for (;;) {
      const page = await this.tradesAfter(market, after);
      for (const row of page) {
        this.publish('matching.tradeExecuted', `${replayRoutingPrefix}.tradeExecuted.${row.market_id}`, this.validateTrade(tradePayload(row)), row.executed_at);
      }
      await this.channel.waitForConfirms();
      count += page.length;
      const last = page.at(-1);
      if (!last || page.length < this.pageSize()) return count;
      after = [String(last.book_sequence), last.trade_id];
    }
  }

  private tradesAfter(market: MarketId, [sequence, tradeId]: readonly [string, string]) {
    return this.db
      .selectFrom('trade')
      .selectAll()
      .where('market_id', '=', market.value)
      .where((eb) => eb(eb.refTuple('book_sequence', 'trade_id'), '>', eb.tuple(sequence, tradeId)))
      .orderBy('book_sequence')
      .orderBy('trade_id')
      .limit(this.pageSize())
      .execute();
  }

  private async republishSnapshot(market: MarketId): Promise<number> {
    const book = await this.books.load(market);
    if (book.sequence === 0) return 0;
    const payload = this.validateSnapshot({ marketId: market.value, sequence: book.sequence, ...book.levels() });
    this.publish('matching.bookSnapshot', `${replayRoutingPrefix}.bookSnapshot.${market.value}`, payload, new Date());
    await this.channel.waitForConfirms();
    return 1;
  }

  private publish(type: string, routingKey: string, payload: Record<string, unknown>, occurredAt: Date): void {
    const envelope = createEnvelope({ type, version: 1, producer: 'matching', correlationId: this.options.correlationId, payload }, () => occurredAt);
    this.channel.publish(this.options.exchange, routingKey, Buffer.from(JSON.stringify(envelope)), {
      persistent: true,
      contentType: 'application/json',
      messageId: envelope.eventId,
    });
  }

  private pageSize(): number {
    return this.options.pageSize ?? 500;
  }
}
