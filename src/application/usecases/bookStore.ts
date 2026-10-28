import type { OrderBook } from '../../domain/entities/orderBook.js';
import type { MarketId } from '../../domain/valueObjects/marketId.js';
import type { EventPublisher } from '../contracts/eventPublisher.js';
import type { OrderBookRepository } from '../contracts/orderBookRepository.js';
import type { TransactionRunner } from '../contracts/transactionRunner.js';

/** Loads the book, applies one change and saves it with its events in one transaction. */
export class BookStore {
  constructor(
    private readonly orderBookRepository: OrderBookRepository,
    private readonly transactionRunner: TransactionRunner,
    private readonly eventPublisher: EventPublisher,
  ) {}

  change(marketId: MarketId, work: (book: OrderBook) => void): Promise<void> {
    return this.transactionRunner.run(async () => {
      const book = await this.orderBookRepository.load(marketId);
      work(book);
      if (book.hasChanges()) await this.orderBookRepository.save(book);
      await this.eventPublisher.publish(book.pullEvents());
    });
  }
}
