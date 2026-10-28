import { describe, expect, test } from 'vitest';
import type { Database } from '../../../../src/infra/database/database.js';
import type { KyselyTransactionRunner } from '../../../../src/infra/database/kyselyTransactionRunner.js';
import { OutboxEventPublisher } from '../../../../src/infra/messaging/outboxEventPublisher.js';

describe('OutboxEventPublisher', () => {
  test('Should refuse a domain event that has no integration event', async () => {
    // Given
    const publisher = new OutboxEventPublisher({} as KyselyTransactionRunner<Database>);

    // When & Then
    await expect(publisher.publish([{ type: 'SomethingElse', occurredAt: new Date() }])).rejects.toThrowError('No integration event for the domain event SomethingElse');
  });

  test('Should publish nothing for an empty list', async () => {
    await expect(new OutboxEventPublisher({} as KyselyTransactionRunner<Database>).publish([])).resolves.toBeUndefined();
  });
});
