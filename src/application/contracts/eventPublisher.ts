import type { DomainEvent } from '../../domain/events/domainEvent.js';

/** Hands the aggregate's events to the outbox inside the current transaction. */
export interface EventPublisher {
  publish(events: readonly DomainEvent[]): Promise<void>;
}
