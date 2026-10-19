import { v4 as uuidv4 } from 'uuid';
import { pool } from '../config/database';
import { generateInvoiceMemo } from '../utils/memo';
import { CreateInvoiceInput } from '../utils/validation';
import type { InvoiceStats } from '../storage/invoice-stats';
import { calculateInvoiceExpiry } from '../domain/invoice-expiry';
import type { InvoiceCursor } from '../storage/invoice-cursor';
import { ConflictError } from '../concurrency/optimistic-lock';

// PostgreSQL invoice service. Kept behaviourally identical to
// InvoiceMemoryService so callers that go through the shared InvoiceStorage
// interface cannot tell which backend is running. Invariants mirrored on both
// sides: (1) markAsPaid only succeeds when status is PENDING AND expires_at
// is strictly after now(), (2) cancelInvoice only succeeds when status is
// PENDING, (3) every read path calls markExpiredInvoices first so expired
// rows transition before being reported, (4) list + stats are scoped to the
// caller's seller_public_key, (5) credit assets always carry their
// asset_issuer because createInvoiceSchema already rejected anything less.
/** Minimal database surface used by this service (pg Pool or a test double). */
export interface Queryable {
  query(text: string, params?: any[]): Promise<{ rows: any[]; rowCount?: number | null }>;
}

export interface Invoice {
  id: string;
  sellerPublicKey: string;
  sellerName?: string;
  sellerEmail?: string;
  amount: number;
  assetCode: string;
  assetIssuer?: string;
  memo: string;
  description?: string;
  customerName?: string;
  customerEmail?: string;
  status: 'PENDING' | 'PAID' | 'EXPIRED' | 'CANCELLED';
  paymentTxHash?: string;
  payerPublicKey?: string;
  payerName?: string;
  payerEmail?: string;
  createdAt: Date;
  paidAt?: Date;
  expiresAt: Date;
  metadata?: any;
  version?: number;
}

export class InvoiceService {
  constructor(private readonly db: Queryable = pool) {}

  /**
   * Create a new invoice for the seller wallet supplied by the request
   */
  async createInvoice(input: CreateInvoiceInput): Promise<Invoice> {
    if (!input.sellerPublicKey) {
      throw new Error('Seller public key is required');
    }

    const id = uuidv4();
    const memo = generateInvoiceMemo();
    const expiresAt = calculateInvoiceExpiry(input.expiresInDays);

    const query = `
      INSERT INTO invoices (
        id, seller_public_key, seller_name, seller_email, amount,
        asset_code, asset_issuer, memo, description, customer_name,
        customer_email, status, expires_at, version
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 1)
      RETURNING *
    `;

    const values = [
      id,
      input.sellerPublicKey,
      input.sellerName || null,
      input.sellerEmail || null,
      input.amount,
      input.assetCode || 'XLM',
      input.assetIssuer || null,
      memo,
      input.description || null,
      input.customerName || null,
      input.customerEmail || null,
      'PENDING',
      expiresAt,
    ];

    try {
      const result = await this.db.query(query, values);
      console.log('✅ Invoice created:', result.rows[0].id);
      return this.mapRowToInvoice(result.rows[0]);
    } catch (error: any) {
      console.error('Error creating invoice:', error);
      throw new Error(`Failed to create invoice: ${error.message}`);
    }
  }

  /**
   * Get invoice by ID
   */
  async getInvoiceById(id: string): Promise<Invoice | null> {
    await this.markExpiredInvoices();
    const query = 'SELECT * FROM invoices WHERE id = $1';
    const result = await this.db.query(query, [id]);

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToInvoice(result.rows[0]);
  }

  /**
   * Get invoice by memo
   */
  async getInvoiceByMemo(memo: string): Promise<Invoice | null> {
    await this.markExpiredInvoices();
    const query = 'SELECT * FROM invoices WHERE memo = $1';
    const result = await this.db.query(query, [memo]);

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToInvoice(result.rows[0]);
  }

  /**
   * Update invoice status to PAID
   */
  async markAsPaid(
    invoiceId: string,
    txHash: string,
    payerPublicKey: string,
    payerInfo?: { payerName?: string; payerEmail?: string },
    expectedVersion?: number
  ): Promise<Invoice> {
    let query: string;
    let params: any[];

    if (expectedVersion !== undefined) {
      query = `
        UPDATE invoices
        SET status = 'PAID', payment_tx_hash = $2, payer_public_key = $3, paid_at = NOW(),
            payer_name = $4, payer_email = $5, version = version + 1
        WHERE id = $1 AND status = 'PENDING' AND expires_at > NOW() AND version = $6
        RETURNING *
      `;
      params = [invoiceId, txHash, payerPublicKey, payerInfo?.payerName || null, payerInfo?.payerEmail || null, expectedVersion];
    } else {
      query = `
        UPDATE invoices
        SET status = 'PAID', payment_tx_hash = $2, payer_public_key = $3, paid_at = NOW(),
            payer_name = $4, payer_email = $5, version = version + 1
        WHERE id = $1 AND status = 'PENDING' AND expires_at > NOW()
        RETURNING *
      `;
      params = [invoiceId, txHash, payerPublicKey, payerInfo?.payerName || null, payerInfo?.payerEmail || null];
    }

    try {
      const result = await this.db.query(query, params);

      if (result.rows.length === 0) {
        if (expectedVersion !== undefined) {
          const check = await this.db.query('SELECT version FROM invoices WHERE id = $1', [invoiceId]);
          if (check.rows.length > 0) {
            throw new ConflictError(check.rows[0].version, expectedVersion);
          }
        }
        throw new Error('Invoice not found, expired, or already processed');
      }

      console.log('✅ Invoice marked as paid:', invoiceId);

      await this.logPaymentEvent(invoiceId, 'PAYMENT_CONFIRMED', {
        txHash,
        payerPublicKey,
      });

      return this.mapRowToInvoice(result.rows[0]);
    } catch (error: any) {
      if (error instanceof ConflictError) {
        throw error;
      }
      console.error('Error marking invoice as paid:', error);
      throw new Error(`Failed to update invoice: ${error.message}`);
    }
  }

  /**
   * Get all invoices for a seller
   */
  async getInvoicesBySeller(
    sellerPublicKey: string,
    status?: string,
    limit: number = 50,
    offset: number = 0,
    after?: InvoiceCursor
  ): Promise<Invoice[]> {
    if (!sellerPublicKey) {
      throw new Error('Seller public key is required');
    }

    await this.markExpiredInvoices();

    let query = 'SELECT * FROM invoices WHERE seller_public_key = $1';
    const params: any[] = [sellerPublicKey];

    if (status) {
      query += ' AND status = $2';
      params.push(status);
    }

    // created_at is truncated to ms so the cursor (a JS Date) compares exactly.
    // ponytail: truncation defeats idx_invoices_seller_created_at for the sort; per-seller
    // lists are small. Add an expression index on date_trunc if a seller reaches ~100k rows.
    const sortKey = "date_trunc('milliseconds', created_at)";
    if (after) {
      query += ` AND (${sortKey}, id) < ($${params.length + 1}::timestamptz, $${params.length + 2}::uuid)`;
      params.push(after.createdAt.toISOString(), after.id);
    }

    query += ` ORDER BY ${sortKey} DESC, id DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);

    const result = await this.db.query(query, params);
    return result.rows.map((row) => this.mapRowToInvoice(row));
  }

  /**
   * Cancel an invoice
   */
  async cancelInvoice(invoiceId: string, expectedVersion?: number): Promise<Invoice> {
    await this.markExpiredInvoices();

    let query: string;
    let params: any[];

    if (expectedVersion !== undefined) {
      query = `
        UPDATE invoices
        SET status = 'CANCELLED', version = version + 1
        WHERE id = $1 AND status = 'PENDING' AND version = $2
        RETURNING *
      `;
      params = [invoiceId, expectedVersion];
    } else {
      query = `
        UPDATE invoices
        SET status = 'CANCELLED', version = version + 1
        WHERE id = $1 AND status = 'PENDING'
        RETURNING *
      `;
      params = [invoiceId];
    }

    const result = await this.db.query(query, params);

    if (result.rows.length === 0) {
      if (expectedVersion !== undefined) {
        const check = await this.db.query('SELECT version FROM invoices WHERE id = $1', [invoiceId]);
        if (check.rows.length > 0) {
          throw new ConflictError(check.rows[0].version, expectedVersion);
        }
      }
      throw new Error('Invoice not found or already processed');
    }

    return this.mapRowToInvoice(result.rows[0]);
  }

  /**
   * Mark expired invoices
   */
  /** Read-only: PENDING rows past expiry that no sweep has transitioned yet. */
  async findOverduePendingInvoices(
    cutoff: Date,
    limit: number
  ): Promise<{ total: number; invoices: Array<Pick<Invoice, 'id' | 'sellerPublicKey' | 'expiresAt'>> }> {
    const result = await this.db.query(
      `SELECT id, seller_public_key, expires_at, COUNT(*) OVER () AS total
       FROM invoices
       WHERE status = 'PENDING' AND expires_at <= $1
       ORDER BY expires_at ASC
       LIMIT $2`,
      [cutoff, limit]
    );
    return {
      total: Number(result.rows[0]?.total ?? 0),
      invoices: result.rows.map((row) => ({
        id: row.id,
        sellerPublicKey: row.seller_public_key,
        expiresAt: new Date(row.expires_at),
      })),
    };
  }

  async markExpiredInvoices(now: Date = new Date()): Promise<number> {
    const query = `
      UPDATE invoices 
      SET status = 'EXPIRED'
      WHERE status = 'PENDING' AND expires_at <= $1
      RETURNING id
    `;

    const result = await this.db.query(query, [now]);
    console.log(`⏰ Marked ${result.rowCount} invoices as expired`);
    return result.rowCount || 0;
  }

  /**
   * Log payment event
   */
  async logPaymentEvent(invoiceId: string, eventType: string, eventData: any): Promise<void> {
    const query = `
      INSERT INTO payment_events (invoice_id, event_type, event_data)
      VALUES ($1, $2, $3)
    `;

    await this.db.query(query, [invoiceId, eventType, JSON.stringify(eventData)]);
  }

  async getAllInvoices(filter?: { status?: string }): Promise<Invoice[]> {
    let query = 'SELECT * FROM invoices';
    const params: any[] = [];
    if (filter?.status) {
      query += ' WHERE status = $1';
      params.push(filter.status);
    }
    query += ' ORDER BY created_at DESC';
    const result = await this.db.query(query, params);
    return result.rows.map((row) => this.mapRowToInvoice(row));
  }

  /**
   * Get invoice statistics
   */
  async getInvoiceStats(sellerPublicKey: string): Promise<InvoiceStats[]> {
    if (!sellerPublicKey) {
      throw new Error('Seller public key is required');
    }

    await this.markExpiredInvoices();

    const query = `
      SELECT 
        COUNT(*) as total_invoices,
        COALESCE(SUM(CASE WHEN status = 'PAID' THEN 1 ELSE 0 END), 0) as paid_invoices,
        COALESCE(SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END), 0) as pending_invoices,
        COALESCE(SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END), 0) as actionable_invoices,
        COALESCE(SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END), 0) as expired_invoices,
        COALESCE(
          (
            SELECT jsonb_object_agg(asset_code, total_revenue)
            FROM (
              SELECT COALESCE(asset_code, 'XLM') as asset_code, SUM(amount) as total_revenue
              FROM invoices
              WHERE seller_public_key = $1 AND status = 'PAID'
              GROUP BY COALESCE(asset_code, 'XLM')
            ) paid_revenue
          ),
          '{}'::jsonb
        ) as revenue_by_asset
      FROM invoices
      WHERE seller_public_key = $1
    `;

    const result = await this.db.query(query, [sellerPublicKey]);
    return result.rows.map((row) => this.mapRowToStats(row));
  }

  /**
   * Map an aggregate row to stats. Postgres returns COUNT/SUM as strings, so the
   * numbers are normalised to match the in-memory backend.
   */
  private mapRowToStats(row: any): InvoiceStats {
    const revenueByAsset: Record<string, number> = {};

    Object.entries(row.revenue_by_asset || {}).forEach(([assetCode, revenue]) => {
      revenueByAsset[assetCode] = Number(revenue);
    });

    return {
      total_invoices: Number(row.total_invoices),
      paid_invoices: Number(row.paid_invoices),
      pending_invoices: Number(row.pending_invoices),
      actionable_invoices: Number(row.actionable_invoices ?? row.pending_invoices),
      expired_invoices: Number(row.expired_invoices),
      revenue_by_asset: revenueByAsset,
    };
  }

  /**
   * Map database row to Invoice object
   */
  private mapRowToInvoice(row: any): Invoice {
    return {
      id: row.id,
      sellerPublicKey: row.seller_public_key,
      sellerName: row.seller_name,
      sellerEmail: row.seller_email,
      amount: parseFloat(row.amount),
      assetCode: row.asset_code,
      assetIssuer: row.asset_issuer,
      memo: row.memo,
      description: row.description,
      customerName: row.customer_name,
      customerEmail: row.customer_email,
      status: row.status,
      paymentTxHash: row.payment_tx_hash,
      payerPublicKey: row.payer_public_key,
      payerName: row.payer_name,
      payerEmail: row.payer_email,
      createdAt: row.created_at,
      paidAt: row.paid_at,
      expiresAt: row.expires_at,
      metadata: row.metadata,
      version: row.version ?? 1,
    };
  }
}

export default new InvoiceService();
