import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import type { TestDatabase } from '../../../support/database.js';
import { createTestDatabase } from '../../../support/database.js';
import type { Database } from '../../../../src/infra/database/database.js';
import { KyselyTransactionRunner } from '../../../../src/infra/database/kyselyTransactionRunner.js';

let database: TestDatabase;
let runner: KyselyTransactionRunner<Database>;

beforeAll(async () => {
  database = await createTestDatabase();
  runner = new KyselyTransactionRunner(database.db);
});

afterAll(() => database.destroy());

/** Like the OutboxEventPublisher: a second writer that only reaches the database through the current transaction. */
const writeThroughTransaction = () => runner.transaction().insertInto('inbox').values({ consumer: 'publisher', message_key: 'from-publisher' }).execute();

const writeAndPublish = (key: string, fail: boolean) =>
  runner.run(async () => {
    await runner.executor().insertInto('inbox').values({ consumer: 'repository', message_key: key }).execute();
    await writeThroughTransaction();
    if (fail) throw new Error('use case failed after publishing');
  });

const keys = async () =>
  (await database.db.selectFrom('inbox').select('message_key').orderBy('message_key').execute()).map((row) => row.message_key);

describe('KyselyTransactionRunner', () => {
  test('Should roll back the repository write and the publisher write together when the use case fails', async () => {
    // When
    await expect(writeAndPublish('failed', true)).rejects.toThrowError('use case failed after publishing');

    // Then
    expect(await keys()).toEqual([]);
  });

  test('Should commit both writes in one transaction when the use case succeeds', async () => {
    // When
    await writeAndPublish('committed', false);

    // Then
    expect(await keys()).toEqual(['committed', 'from-publisher']);
  });

  test('Should refuse transaction() outside run', () => {
    expect(() => runner.transaction()).toThrowError('No active transaction');
  });
});
