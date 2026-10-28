import type { EventPublisher } from '../../application/contracts/eventPublisher.js';
import type { BookChanged } from '../../domain/events/bookChanged.js';
import type { DomainEvent } from '../../domain/events/domainEvent.js';
import type { OrderCancelRejected } from '../../domain/events/orderCancelRejected.js';
import type { OrderRemoved } from '../../domain/events/orderRemoved.js';
import type { TradeExecuted } from '../../domain/events/tradeExecuted.js';
import type { Database } from '../database/database.js';
import type { KyselyTransactionRunner } from '../database/kyselyTransactionRunner.js';
import { createEnvelope } from '../platform/messaging/envelope.js';
import { appendToOutbox } from '../platform/messaging/outbox.js';
import type { SchemaValidator } from '../platform/messaging/schemas.js';
import { loadSchemaValidator } from '../platform/messaging/schemas.js';
import { currentCorrelation } from '../platform/observability/correlation.js';

export const matchingEventsExchange = 'matching.events';

type MatchingEvent = TradeExecuted | OrderRemoved | OrderCancelRejected | BookChanged;

const publishedTypes = ['matching.tradeExecuted', 'matching.orderRemoved', 'matching.orderCancelRejected', 'matching.bookSnapshot'] as const;
type PublishedType = (typeof publishedTypes)[number];

interface IntegrationEvent {
  readonly type: PublishedType;
  readonly payload: Record<string, unknown>;
}

const loadValidators = () =>
  Object.fromEntries(
    publishedTypes.map((type) => [type, loadSchemaValidator<Record<string, unknown>>(`src/infra/schemas/published/events/${type}.v1.schema.json`)]),
  ) as Record<PublishedType, SchemaValidator<Record<string, unknown>>>;

/** Maps the domain event to its published contract (same type, payload and routing as the schema). */
const integrationEventOf = (event: MatchingEvent): IntegrationEvent => {
  switch (event.type) {
    case 'TradeExecuted': {
      const { type: _type, occurredAt, ...trade } = event;
      return { type: 'matching.tradeExecuted', payload: { ...trade, executedAt: occurredAt.toISOString() } };
    }
    case 'OrderRemoved': {
      const { type: _type, occurredAt: _at, ...removed } = event;
      return { type: 'matching.orderRemoved', payload: removed };
    }
    case 'OrderCancelRejected': {
      const { type: _type, occurredAt: _at, ...rejected } = event;
      return { type: 'matching.orderCancelRejected', payload: rejected };
    }
    case 'BookChanged':
      return { type: 'matching.bookSnapshot', payload: { marketId: event.marketId, sequence: event.sequence, bids: event.bids, asks: event.asks } };
    default:
      throw new Error(`No integration event for the domain event ${(event as DomainEvent).type}`);
  }
};

/** Writes each domain event as its integration event to the outbox, in the current transaction, routed by market. */
export class OutboxEventPublisher implements EventPublisher {
  private readonly validators = loadValidators();

  constructor(private readonly runner: KyselyTransactionRunner<Database>) {}

  async publish(events: readonly DomainEvent[]): Promise<void> {
    for (const event of events) await this.append(event as MatchingEvent);
  }

  private async append(event: MatchingEvent): Promise<void> {
    const { type, payload } = integrationEventOf(event);
    const correlation = currentCorrelation();
    const envelope = createEnvelope(
      { type, version: 1, producer: 'matching', correlationId: correlation.correlationId, causationId: correlation.causationId, payload: this.validators[type](payload) },
      () => event.occurredAt,
    );
    await appendToOutbox(this.runner.transaction(), { routingKey: `${type}.${event.marketId}`, envelope });
  }
}
