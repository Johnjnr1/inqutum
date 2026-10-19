import { generateInvoiceMemo } from '../utils/memo';
import { CreateInvoiceInput } from '../utils/validation';
import memoryStorage, { MemoryStorage } from '../storage/memory-storage';
import { calculateInvoiceExpiry } from '../domain/invoice-expiry';
import type { StoredInvoice } from '../storage/invoice-storage';
import { compareNewestFirst, isAfterCursor, type InvoiceCursor } from '../storage/invoice-cursor';
import type { InvoiceStats } from '../storage/invoice-stats';
import type { PayerInfo } from '../storage/invoice-storage';
import { checkVersion } from '../concurrency/optimistic-lock';

export class InvoiceMemoryService {
  constructor(private readonly storage: MemoryStorage = memoryStorage) {}

  async createInvoice(input: CreateInvoiceInput): Promise<StoredInvoice> {
    if (!input.sellerPublicKey) {
      throw new Error('Seller public key is required');
    }

    const memo = generateInvoiceMemo();
    const expiresAt = calculateInvoiceExpiry(input.expiresInDays);

    const invoice = this.storage.createInvoice({
      sellerPublicKey: input.sellerPublicKey,
      sellerName: input.sellerName,
      sellerEmail: input.sellerEmail,
      amount: input.amount,
      assetCode: input.assetCode || 'XLM',
      assetIssuer: input.assetIssuer,
      memo,
      description: input.description,
      customerName: input.customerName,
      customerEmail: input.customerEmail,
      expiresAt,
    });

    if (process.env.NODE_ENV !== 'test') {
      console.log('✅ Invoice created:', invoice.id);
    }
    return invoice;
  }

  async getInvoiceById(id: string): Promise<StoredInvoice | null> {
    const invoice = this.storage.getInvoiceById(id);
    return invoice ?? null;
  }

  async getInvoiceByMemo(memo: string): Promise<StoredInvoice | null> {
    const invoice = this.storage.getInvoiceByMemo(memo);
    return invoice ?? null;
  }

  async markAsPaid(
    invoiceId: string,
    txHash: string,
    payerPublicKey: string,
    payerInfo?: PayerInfo,
    expectedVersion?: number
  ): Promise<StoredInvoice> {
    const existing = this.storage.getInvoiceById(invoiceId);
    if (existing && expectedVersion !== undefined) {
      checkVersion(expectedVersion, existing.version ?? 1);
    }

    const invoice = this.storage.markAsPaid(invoiceId, txHash, payerPublicKey, payerInfo, expectedVersion);

    if (!invoice) {
      throw new Error('Invoice not found, expired, or already processed');
    }

    console.log('✅ Invoice marked as paid:', invoiceId);
    return invoice;
  }

  async getInvoicesBySeller(
    sellerPublicKey: string,
    status?: string,
    limit: number = 50,
    offset: number = 0,
    after?: InvoiceCursor
  ): Promise<StoredInvoice[]> {
    let invoices = this.storage.getAllInvoices(status ? { status } : undefined);

    if (sellerPublicKey) {
      invoices = invoices.filter((inv) => inv.sellerPublicKey === sellerPublicKey);
    }

    invoices.sort(compareNewestFirst);
    if (after) invoices = invoices.filter((inv) => isAfterCursor(inv, after));

    return invoices.slice(offset, offset + limit);
  }

  async cancelInvoice(invoiceId: string, expectedVersion?: number): Promise<StoredInvoice> {
    const invoice = this.storage.getInvoiceById(invoiceId);

    if (!invoice) {
      throw new Error('Invoice not found or already processed');
    }

    if (expectedVersion !== undefined) {
      checkVersion(expectedVersion, invoice.version ?? 1);
    }

    if (invoice.status !== 'PENDING') {
      throw new Error('Invoice not found or already processed');
    }

    const updated = this.storage.updateInvoice(invoiceId, { status: 'CANCELLED' }, expectedVersion);
    if (!updated) {
      throw new Error('Invoice not found or already processed');
    }
    return updated;
  }

  async findOverduePendingInvoices(cutoff: Date, limit: number) {
    const overdue = this.storage.findOverduePending(cutoff);
    return { total: overdue.length, invoices: overdue.slice(0, limit) };
  }

  async markExpiredInvoices(now?: Date): Promise<number> {
    return this.storage.markExpiredInvoices(now);
  }

  async getInvoiceStats(sellerPublicKey: string): Promise<InvoiceStats[]> {
    return [this.storage.getStats(sellerPublicKey)];
  }

  async getAllInvoices(filter?: { status?: string }): Promise<StoredInvoice[]> {
    return this.storage.getAllInvoices(filter);
  }
}

export default new InvoiceMemoryService();
