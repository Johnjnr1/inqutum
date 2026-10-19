/**
 * In-memory keyed mutex lock manager to prevent race conditions during
 * async operations (e.g., awaiting Horizon verification) on the same invoice ID.
 */

export class InvoiceLockManager {
  private locks = new Map<string, Promise<void>>();

  /**
   * Acquire a lock for a specific invoice ID.
   * Returns a release function that MUST be invoked when the operation completes.
   *
   * Time Complexity: O(1)
   * Space Complexity: O(1) per active locked invoice
   */
  async acquire(invoiceId: string): Promise<() => void> {
    while (this.locks.has(invoiceId)) {
      await this.locks.get(invoiceId);
    }

    let releaseLock!: () => void;
    const lockPromise = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });

    this.locks.set(invoiceId, lockPromise);

    return () => {
      if (this.locks.get(invoiceId) === lockPromise) {
        this.locks.delete(invoiceId);
      }
      releaseLock();
    };
  }

  isLocked(invoiceId: string): boolean {
    return this.locks.has(invoiceId);
  }

  get activeLocks(): number {
    return this.locks.size;
  }
}

export const invoiceLockManager = new InvoiceLockManager();
