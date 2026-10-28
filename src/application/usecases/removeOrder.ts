import { MarketId } from '../../domain/valueObjects/marketId.js';
import { OrderId } from '../../domain/valueObjects/orderId.js';
import type { Clock } from '../contracts/clock.js';
import type { BookStore } from './bookStore.js';

/** Saga S4 pivot: removes what is left of the order, or answers that it is no longer in the book. */
export class RemoveOrder {
  constructor(
    private readonly bookStore: BookStore,
    private readonly clock: Clock,
  ) {}

  async execute(rawOrderId: string, rawMarketId: string): Promise<void> {
    const orderId = OrderId.from(rawOrderId);
    await this.bookStore.change(MarketId.from(rawMarketId), (book) => book.cancel(orderId, this.clock.now()));
  }
}
