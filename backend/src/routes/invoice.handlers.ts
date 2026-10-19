// Handlers for both backends (in-memory and PostgreSQL). They take an
// InvoiceStorage implementation and do not branch on the storage mode, so a
// bug in one backend is a bug in both. All seller/payer/asset/customer/
// metadata/expiry fields pass through unchanged from the storage layer. The
// shared suite in invoice-handlers.test.ts runs these same handlers against
// both adapters with the same assertions to guarantee field parity.
import { Request, Response } from 'express';
import { performance } from 'perf_hooks';
import stellarService from '../services/stellar.service';
import { createInvoiceSchema } from '../utils/validation';
import { generatePaymentQR, generateStellarPaymentQR } from '../utils/qrcode';
import { sendFailure, sendSuccess, sendVerificationFailure } from '../types/api';
import { INVOICE_STATUSES, type InvoiceStatus, type InvoiceStorage, type StoredInvoice } from '../storage/invoice-storage';
import { decodeInvoiceCursor, encodeInvoiceCursor } from '../storage/invoice-cursor';
import { STELLAR_NETWORK } from '../config/stellar';
import {
  VERIFICATION_CONTRACT_VERSION,
  VERIFICATION_MESSAGES,
  checkInvoiceIsPayable,
  checkPayerInfo,
  checkTxHash,
  verifyHorizonPayment,
} from '../services/payment-verification';
import { simulationAllowed } from '../config/runtime';
import { metrics } from '../observability/telemetry';
import { exportAuditEvents, AuditAction } from '../audit/audit-service';
import { classifyError } from '../errors/error-taxonomy';
import { ConflictError } from '../concurrency/optimistic-lock';
import { invoiceLockManager } from '../concurrency/lock-manager';
import { safeFrontendOrigin } from '../security/content-safety';
import {
  CheckpointInDoubtError,
  CheckpointInProgressError,
  CheckpointKeyReusedError,
} from '../recovery/side-effect-checkpoint';
import { NotificationService, notificationService } from '../notifications/notification-service';
import { EmailService, emailService, EmailTemplateType } from '../services/email.service';
import type { VerificationCode } from '../services/payment-verification';

/** Kept explicit so clients can tune polling without duplicating backend policy. */
export const PAYMENT_STATUS_POLL_INTERVAL_MS = 3000;

/** Invoice list page size bounds; the effective value is echoed in `pagination.limit`. */
export const DEFAULT_INVOICE_PAGE_LIMIT = 50;
export const MAX_INVOICE_PAGE_LIMIT = 200;

/** Only the part of the Stellar service the verify handler needs. */
export interface TransactionLookup {
  getTransaction(txHash: string): Promise<any>;
}

export interface InvoiceHandlerOptions {
  storage: InvoiceStorage;
  /** Defaults to FRONTEND_URL, read per request so tests and dev reloads see changes. */
  frontendUrl?: string;
  /** Optional local-test override. Production always forces simulation off. */
  allowSimulate?: boolean;
  stellar?: TransactionLookup;
  /** Defaults to the process-wide notification service. */
  notifications?: NotificationService;
  /** Defaults to the process-wide email service. */
  emailService?: EmailService;
}

export interface InvoiceHandlers {
  createInvoice(req: Request, res: Response): Promise<void>;
  getInvoice(req: Request, res: Response): Promise<void>;
  getInvoices(req: Request, res: Response): Promise<void>;
  getPaymentInfo(req: Request, res: Response): Promise<void>;
  cancelInvoice(req: Request, res: Response): Promise<void>;
  verifyPayment(req: Request, res: Response): Promise<void>;
  getStats(req: Request, res: Response): Promise<void>;
  simulatePayment(req: Request, res: Response): Promise<void>;
  getInvoiceAuditTrail(req: Request, res: Response): Promise<void>;
  getAuditEvents(req: Request, res: Response): Promise<void>;
  exportAuditTrail(req: Request, res: Response): Promise<void>;
  getObservabilityMetrics(req: Request, res: Response): Promise<void>;
  getPrometheusMetrics(req: Request, res: Response): Promise<void>;
  getEmailPreview(req: Request, res: Response): Promise<void>;
  sendInvoiceEmail(req: Request, res: Response): Promise<void>;
}

/**
 * Log the stack/message rather than the error object: some validation errors
 * (zod) cannot be inspected by `console` on newer Node versions, and the throw
 * would escape the catch block and leave the request hanging.
 */
function logError(label: string, error: any): void {
  console.error(label, error?.stack || error?.message || error);
}

function toPositiveInt(value: unknown, fallback: number): number {
  const parsed = parseInt(String(value), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function getUserAgent(req: Request): string | undefined {
  if (typeof req.get === 'function') {
    return req.get('user-agent');
  }
  return (req.headers && req.headers['user-agent']) as string | undefined;
}

/**
 * Invoice route handlers shared by the MVP (in-memory) and Postgres servers.
 * The only difference between the two entrypoints is the storage adapter.
 */
export function createInvoiceHandlers(options: InvoiceHandlerOptions): InvoiceHandlers {
  const { storage } = options;
  const stellar: TransactionLookup = options.stellar || stellarService;
  const notifications = options.notifications ?? notificationService;
  const emails = options.emailService ?? emailService;

  // Notifications are a side effect: a failure here must never fail the request.
  const safely = (label: string, fn: () => unknown) => {
    try {
      fn();
    } catch (err) {
      logError(`Notification error (${label}):`, err);
    }
  };
  const syncNotifications = (invoice: StoredInvoice) =>
    safely('sync', () => notifications.syncInvoiceNotifications(invoice));

  const frontendUrl = () =>
    safeFrontendOrigin(options.frontendUrl || process.env.FRONTEND_URL);

  const simulateAllowed = () =>
    process.env.NODE_ENV !== 'production' && (
      options.allowSimulate !== undefined
        ? options.allowSimulate
        : simulationAllowed()
    );

  const buildPaymentPayload = async (invoice: StoredInvoice) => {
    const paymentUrl = `${frontendUrl()}/pay/${invoice.id}`;

    if (invoice.status !== 'PENDING') {
      return {
        paymentAvailable: false,
        paymentUrl,
        qrCode: null,
        stellarQrCode: null,
      };
    }

    return {
      paymentAvailable: true,
      paymentUrl,
      statusPollingIntervalMs: PAYMENT_STATUS_POLL_INTERVAL_MS,
      qrCode: await generatePaymentQR(paymentUrl),
      stellarQrCode: await generateStellarPaymentQR(
        invoice.sellerPublicKey,
        invoice.amount.toString(),
        invoice.assetCode,
        invoice.memo,
        invoice.assetIssuer
      ),
    };
  };

  return {
    async createInvoice(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const validatedData = createInvoiceSchema.parse(req.body);
        const invoice = await storage.createInvoice(validatedData);
        const payment = await buildPaymentPayload(invoice);

        // Record audit trail event
        if (storage.recordAuditEvent) {
          try {
            await storage.recordAuditEvent({
              action: 'INVOICE_CREATED',
              actor: {
                type: 'seller',
                id: invoice.sellerPublicKey,
                ip: req.ip,
                userAgent: getUserAgent(req),
              },
              scope: {
                entityType: 'invoice',
                entityId: invoice.id,
              },
              reason: 'Seller created invoice',
              afterState: invoice,
              metadata: {
                amount: invoice.amount,
                assetCode: invoice.assetCode,
                assetIssuer: invoice.assetIssuer,
                memo: invoice.memo,
                expiresAt: invoice.expiresAt,
              },
              correlationId: corrId,
            });
          } catch (auditErr) {
            console.error('Audit log error on invoice creation:', auditErr);
          }
        }

        // Telemetry & Funnel
        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.create',
          actor_type: 'seller',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 201,
          metadata: { invoiceId: invoice.id, amount: invoice.amount, assetCode: invoice.assetCode },
        });
        metrics.recordFunnelStage('invoice_created');

        sendSuccess(res, 201, {
          invoice,
          paymentAvailable: payment.paymentAvailable,
          paymentUrl: payment.paymentUrl,
          statusPollingIntervalMs: payment.statusPollingIntervalMs,
          qrCode: payment.qrCode,
          stellarQrCode: payment.stellarQrCode,
        });
      } catch (error: any) {
        const duration = performance.now() - start;
        logError('Create invoice error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.create',
          actor_type: 'seller',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 400,
          error_code: classified.code,
        });

        sendFailure(res, 400, error.message || 'Failed to create invoice', {
          code: classified.code,
          correlationId: corrId,
        });
      }
    },

    async getInvoice(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const invoice = await storage.getInvoiceById(req.params.id);

        if (!invoice) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.get',
            actor_type: 'anonymous',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 404,
            error_code: 'INVOICE_NOT_FOUND',
          });
          return sendFailure(res, 404, 'Invoice not found', {
            code: 'INVOICE_NOT_FOUND',
            correlationId: corrId,
          });
        }

        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.get',
          actor_type: 'anonymous',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { invoiceId: invoice.id, status: invoice.status },
        });

        syncNotifications(invoice);
        if (invoice.version !== undefined && typeof res.setHeader === 'function') {
          res.setHeader('ETag', String(invoice.version));
        }
        sendSuccess(res, 200, invoice);
      } catch (error: any) {
        const duration = performance.now() - start;
        logError('Get invoice error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.get',
          actor_type: 'anonymous',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 500,
          error_code: classified.code,
        });

        sendFailure(res, 500, error.message || 'Failed to get invoice', {
          code: classified.code,
          correlationId: corrId,
        });
      }
    },

    async getInvoices(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const { sellerPublicKey } = req.query;

        if (!sellerPublicKey) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.list',
            actor_type: 'seller',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 400,
            error_code: 'SELLER_REQUIRED',
          });
          return sendFailure(res, 400, 'sellerPublicKey query parameter is required', {
            code: 'SELLER_REQUIRED',
            correlationId: corrId,
          });
        }

        // Status is matched case-insensitively so API and UI filters agree on both backends.
        const status = req.query.status ? String(req.query.status).toUpperCase() : undefined;
        const after = req.query.cursor ? decodeInvoiceCursor(String(req.query.cursor)) : undefined;
        const invalid =
          status && !INVOICE_STATUSES.includes(status as InvoiceStatus)
            ? { code: 'INVALID_STATUS', message: `status must be one of ${INVOICE_STATUSES.join(', ')}` }
            : after === null
              ? { code: 'INVALID_CURSOR', message: 'cursor is not valid; restart from the first page' }
              : null;
        if (invalid) {
          metrics.recordOperation({
            operation: 'invoice.list',
            actor_type: 'seller',
            result: 'failure',
            latency_ms: performance.now() - start,
            correlation_id: corrId,
            http_status: 400,
            error_code: invalid.code,
          });
          return sendFailure(res, 400, invalid.message, { code: invalid.code, correlationId: corrId });
        }

        // Clamped so a page is never empty-but-"hasMore" (limit=0) or unbounded.
        const limit = Math.min(
          Math.max(toPositiveInt(req.query.limit, DEFAULT_INVOICE_PAGE_LIMIT), 1),
          MAX_INVOICE_PAGE_LIMIT
        );
        // A cursor fully positions the page; offset only applies to legacy offset paging.
        const offset = after ? 0 : toPositiveInt(req.query.offset, 0);

        // Fetch one extra row to learn whether another page exists.
        const rows = (await storage.getInvoicesBySeller(
          sellerPublicKey as string,
          status,
          limit + 1,
          offset,
          after ?? undefined
        )).filter((row) => row.sellerPublicKey === sellerPublicKey);
        const hasMore = rows.length > limit;
        const invoices = rows.slice(0, limit);
        const last = invoices[invoices.length - 1];
        const nextCursor = hasMore && last ? encodeInvoiceCursor(last) : null;

        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.list',
          actor_type: 'seller',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { sellerPublicKey, count: invoices.length, limit, offset },
        });

        invoices.forEach(syncNotifications);
        sendSuccess(res, 200, invoices, {
          pagination: { limit, offset, total: invoices.length, nextCursor, hasMore },
        });
      } catch (error: any) {
        const duration = performance.now() - start;
        logError('Get invoices error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.list',
          actor_type: 'seller',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 500,
          error_code: classified.code,
        });

        sendFailure(res, 500, error.message || 'Failed to get invoices', {
          code: classified.code,
          correlationId: corrId,
        });
      }
    },

    async getPaymentInfo(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const invoice = await storage.getInvoiceById(req.params.id);

        if (!invoice) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.get_payment_info',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 404,
            error_code: 'INVOICE_NOT_FOUND',
          });
          return sendFailure(res, 404, 'Invoice not found', {
            code: 'INVOICE_NOT_FOUND',
            correlationId: corrId,
          });
        }

        const payment = await buildPaymentPayload(invoice);
        const duration = performance.now() - start;

        metrics.recordOperation({
          operation: 'invoice.get_payment_info',
          actor_type: 'payer',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { invoiceId: invoice.id, status: invoice.status },
        });
        metrics.recordFunnelStage('payment_page_viewed');

        sendSuccess(res, 200, { ...payment, invoice });
      } catch (error: any) {
        const duration = performance.now() - start;
        logError('Get payment info error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.get_payment_info',
          actor_type: 'payer',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 500,
          error_code: classified.code,
        });

        sendFailure(res, 500, error.message || 'Failed to get payment info', {
          code: classified.code,
          correlationId: corrId,
        });
      }
    },

    async cancelInvoice(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const expectedVersion = req.body?.version ?? (req.headers && req.headers['if-match'] ? Number(req.headers['if-match']) : undefined);
        const originalInvoice = await storage.getInvoiceById(req.params.id);
        const invoice = await storage.cancelInvoice(req.params.id, expectedVersion);

        // Record audit trail event
        if (storage.recordAuditEvent) {
          try {
            await storage.recordAuditEvent({
              action: 'INVOICE_CANCELLED',
              actor: {
                type: 'seller',
                id: invoice.sellerPublicKey,
                ip: req.ip,
                userAgent: getUserAgent(req),
              },
              scope: {
                entityType: 'invoice',
                entityId: invoice.id,
              },
              reason: 'Seller cancelled invoice',
              beforeState: originalInvoice,
              afterState: invoice,
              correlationId: corrId,
            });
          } catch (auditErr) {
            console.error('Audit log error on invoice cancel:', auditErr);
          }
        }

        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.cancel',
          actor_type: 'seller',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { invoiceId: invoice.id },
        });

        syncNotifications(invoice);
        sendSuccess(res, 200, invoice);
      } catch (error: any) {
        const duration = performance.now() - start;

        if (error instanceof ConflictError) {
          metrics.recordOperation({
            operation: 'invoice.cancel',
            actor_type: 'seller',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 409,
            error_code: 'CONFLICT',
          });
          return sendFailure(res, 409, error.message, {
            code: 'CONFLICT',
            correlationId: corrId,
          });
        }

        logError('Cancel invoice error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.cancel',
          actor_type: 'seller',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 400,
          error_code: classified.code,
        });

        sendFailure(res, 400, error.message || 'Failed to cancel invoice', {
          code: classified.code,
          correlationId: corrId,
        });
      }
    },

    async verifyPayment(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      const { id } = req.params;
      const releaseLock = await invoiceLockManager.acquire(id);

      try {
        const { network } = req.body || {};

        metrics.recordFunnelStage('payment_initiated');

        const hashCheck = checkTxHash(req.body?.txHash);
        if (!hashCheck.ok) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 400,
            error_code: hashCheck.code,
          });
          return sendVerificationFailure(res, 400, hashCheck.code, hashCheck.error, corrId);
        }

        const payerCheck = checkPayerInfo(req.body);
        if (!payerCheck.ok) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 400,
            error_code: payerCheck.code,
          });
          return sendVerificationFailure(res, 400, payerCheck.code, payerCheck.error, corrId);
        }

        const invoice = await storage.getInvoiceById(id);

        if (!invoice) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 404,
            error_code: 'INVOICE_NOT_FOUND',
          });
          return sendFailure(res, 404, 'Invoice not found', {
            code: 'INVOICE_NOT_FOUND',
            correlationId: corrId,
          });
        }

        const statusCheck = checkInvoiceIsPayable(invoice.status);
        if (!statusCheck.ok) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 400,
            error_code: statusCheck.code,
          });
          return sendVerificationFailure(res, 400, statusCheck.code, statusCheck.error, corrId);
        }

        let txDetails;
        try {
          txDetails = await stellar.getTransaction(hashCheck.value);
        } catch (error: any) {
          logError('Verify payment lookup error:', error);
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 404,
            error_code: 'TRANSACTION_NOT_FOUND',
          });
          return sendVerificationFailure(
            res,
            404,
            'TRANSACTION_NOT_FOUND',
            VERIFICATION_MESSAGES.TRANSACTION_NOT_FOUND,
            corrId
          );
        }

        const verification = verifyHorizonPayment({
          txHash: hashCheck.value,
          expected: {
            memo: invoice.memo,
            amount: invoice.amount,
            destination: invoice.sellerPublicKey,
            assetCode: invoice.assetCode,
            assetIssuer: invoice.assetIssuer,
            network: STELLAR_NETWORK,
          },
          transaction: txDetails.transaction,
          operations: txDetails.operations,
          network,
        });

        if (!verification.ok) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 400,
            error_code: verification.code,
          });
          safely('rejected', () =>
            notifications.notifyPaymentRejected(invoice, verification.code as VerificationCode, hashCheck.value)
          );
          return sendVerificationFailure(res, 400, verification.code, verification.error, corrId);
        }

        const paymentExpectedVersion = req.body?.version ?? (req.headers && req.headers['if-match'] ? Number(req.headers['if-match']) : undefined);
        let updatedInvoice: StoredInvoice;
        try {
          updatedInvoice = await storage.markAsPaid(
            id,
            verification.value.txHash,
            verification.value.from,
            payerCheck.value,
            paymentExpectedVersion
          );
        } catch (error) {
          // The payment lookup can cross expiresAt after the first status read.
          // Re-read so that race still returns the public expiry contract.
          const latest = await storage.getInvoiceById(id);
          const latestStatus = latest && checkInvoiceIsPayable(latest.status);
          if (latestStatus && !latestStatus.ok) {
            const duration = performance.now() - start;
            metrics.recordOperation({
              operation: 'invoice.verify_payment',
              actor_type: 'payer',
              result: 'failure',
              latency_ms: duration,
              correlation_id: corrId,
              http_status: 400,
              error_code: latestStatus.code,
            });
            return sendVerificationFailure(
              res,
              400,
              latestStatus.code,
              latestStatus.error,
              corrId
            );
          }
          throw error;
        }

        // Record audit trail event for verified settlement
        if (storage.recordAuditEvent) {
          try {
            await storage.recordAuditEvent({
              action: 'PAYMENT_VERIFIED',
              actor: {
                type: 'payer',
                id: verification.value.from,
                ip: req.ip,
                userAgent: getUserAgent(req),
              },
              scope: {
                entityType: 'invoice',
                entityId: invoice.id,
              },
              reason: 'Payment verified on Horizon against invoice requirements',
              beforeState: invoice,
              afterState: updatedInvoice,
              metadata: {
                txHash: verification.value.txHash,
                payerPublicKey: verification.value.from,
                amount: invoice.amount,
                assetCode: invoice.assetCode,
                assetIssuer: invoice.assetIssuer,
                memo: invoice.memo,
                settledAt: updatedInvoice.paidAt,
              },
              correlationId: corrId,
            });
          } catch (auditErr) {
            console.error('Audit log error on payment verify:', auditErr);
          }
        }

        // Telemetry & Funnel Success
        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.verify_payment',
          actor_type: 'payer',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: {
            invoiceId: updatedInvoice.id,
            txHash: verification.value.txHash,
            payer: verification.value.from,
          },
        });
        metrics.recordFunnelStage('payment_verified');

        syncNotifications(updatedInvoice);
        sendSuccess(res, 200, updatedInvoice, {
          message: 'Payment verified on Stellar',
          correlationId: corrId,
        });
      } catch (error: any) {
        const duration = performance.now() - start;

        if (error instanceof ConflictError) {
          metrics.recordOperation({
            operation: 'invoice.verify_payment',
            actor_type: 'payer',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 409,
            error_code: 'CONFLICT',
          });
          return sendFailure(res, 409, error.message, {
            code: 'CONFLICT',
            correlationId: corrId,
          });
        }

        logError('Verify payment error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.verify_payment',
          actor_type: 'payer',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 500,
          error_code: classified.code,
        });

        sendFailure(res, 500, error.message || 'Failed to verify payment', {
          code: classified.code,
          correlationId: corrId,
        });
      } finally {
        releaseLock();
      }
    },

    async getStats(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const { sellerPublicKey } = req.query;

        if (!sellerPublicKey) {
          const duration = performance.now() - start;
          metrics.recordOperation({
            operation: 'invoice.get_stats',
            actor_type: 'seller',
            result: 'failure',
            latency_ms: duration,
            correlation_id: corrId,
            http_status: 400,
            error_code: 'SELLER_REQUIRED',
          });
          return sendFailure(res, 400, 'sellerPublicKey query parameter is required', {
            code: 'SELLER_REQUIRED',
            correlationId: corrId,
          });
        }

        const stats = await storage.getInvoiceStats(sellerPublicKey as string);
        const duration = performance.now() - start;

        metrics.recordOperation({
          operation: 'invoice.get_stats',
          actor_type: 'seller',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { sellerPublicKey },
        });

        sendSuccess(res, 200, stats);
      } catch (error: any) {
        const duration = performance.now() - start;
        logError('Get stats error:', error);
        const classified = classifyError(error);

        metrics.recordOperation({
          operation: 'invoice.get_stats',
          actor_type: 'seller',
          result: 'failure',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 500,
          error_code: classified.code,
        });

        sendFailure(res, 500, error.message || 'Failed to get statistics', {
          code: classified.code,
          correlationId: corrId,
        });
      }
    },

    // Local testing only — hidden unless ALLOW_SIMULATE=true.
    async simulatePayment(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        if (!simulateAllowed()) {
          return sendFailure(res, 404, 'Endpoint not found', {
            code: 'NOT_FOUND',
            correlationId: corrId,
          });
        }

        const { id } = req.params;
        const invoice = await storage.getInvoiceById(id);

        if (!invoice) {
          return sendFailure(res, 404, 'Invoice not found', {
            code: 'INVOICE_NOT_FOUND',
            correlationId: corrId,
          });
        }

        const statusCheck = checkInvoiceIsPayable(invoice.status);
        if (!statusCheck.ok) {
          return sendVerificationFailure(res, 400, statusCheck.code, statusCheck.error, corrId);
        }

        const mockTxHash = `MOCK_TX_${Date.now().toString(36).toUpperCase()}_${Math.random()
          .toString(36)
          .substring(2, 10)
          .toUpperCase()}`;
        const mockPayerKey = 'GXXXSIMULATEDPAYERXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';

        const updatedInvoice = await storage.markAsPaid(id, mockTxHash, mockPayerKey);

        // Audit simulation
        if (storage.recordAuditEvent) {
          try {
            await storage.recordAuditEvent({
              action: 'PAYMENT_SIMULATED',
              actor: {
                type: 'maintainer',
                id: 'dev-simulator',
                ip: req.ip,
                userAgent: getUserAgent(req),
              },
              scope: {
                entityType: 'invoice',
                entityId: invoice.id,
              },
              reason: 'Payment simulated via dev API endpoint',
              beforeState: invoice,
              afterState: updatedInvoice,
              metadata: { mockTxHash, mockPayerKey },
              correlationId: corrId,
            });
          } catch (auditErr) {
            console.error('Audit log error on payment simulation:', auditErr);
          }
        }

        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.simulate_payment',
          actor_type: 'maintainer',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { invoiceId: updatedInvoice.id, mockTxHash },
        });

        syncNotifications(updatedInvoice);
        sendSuccess(res, 200, updatedInvoice, {
          message: 'Payment simulated successfully',
          correlationId: corrId,
        });
      } catch (error: any) {
        logError('Simulate payment error:', error);
        sendFailure(res, 500, error.message || 'Failed to simulate payment', {
          correlationId: corrId,
        });
      }
    },

    // Audit Trail for a specific invoice
    async getInvoiceAuditTrail(req: Request, res: Response) {
      try {
        const { id } = req.params;
        const events = storage.getAuditEventsByInvoice
          ? await storage.getAuditEventsByInvoice(id)
          : [];

        sendSuccess(res, 200, events, {
          pagination: { limit: events.length, offset: 0, total: events.length },
        });
      } catch (error: any) {
        logError('Get invoice audit trail error:', error);
        sendFailure(res, 500, error.message || 'Failed to get audit trail');
      }
    },

    // Maintainer query for audit events
    async getAuditEvents(req: Request, res: Response) {
      try {
        const { action, entityId, actorId, actorType, fromTimestamp, toTimestamp, limit, offset } =
          req.query;

        const filter = {
          action: action as AuditAction | undefined,
          entityId: entityId as string | undefined,
          actorId: actorId as string | undefined,
          actorType: actorType as any,
          fromTimestamp: fromTimestamp as string | undefined,
          toTimestamp: toTimestamp as string | undefined,
          limit: limit ? toPositiveInt(limit, 50) : undefined,
          offset: offset ? toPositiveInt(offset, 0) : undefined,
        };

        const result = storage.getAuditEvents
          ? await storage.getAuditEvents(filter)
          : { events: [], total: 0, limit: 50, offset: 0 };

        sendSuccess(res, 200, result.events, {
          pagination: { limit: result.limit, offset: result.offset, total: result.total },
        });
      } catch (error: any) {
        logError('Query audit events error:', error);
        sendFailure(res, 500, error.message || 'Failed to query audit events');
      }
    },

    // Maintainer export for audit events (JSON, NDJSON, CSV)
    async exportAuditTrail(req: Request, res: Response) {
      try {
        const { format = 'json', action, entityId, actorId } = req.query;
        const result = storage.getAuditEvents
          ? await storage.getAuditEvents({
              action: action as AuditAction | undefined,
              entityId: entityId as string | undefined,
              actorId: actorId as string | undefined,
              limit: 500,
            })
          : { events: [], total: 0, limit: 500, offset: 0 };

        const exportFormat = (String(format).toLowerCase() === 'csv'
          ? 'csv'
          : String(format).toLowerCase() === 'ndjson'
          ? 'ndjson'
          : 'json') as 'json' | 'ndjson' | 'csv';

        const output = exportAuditEvents(result.events, exportFormat);

        if (exportFormat === 'csv') {
          res.setHeader('Content-Type', 'text/csv');
          res.setHeader('Content-Disposition', 'attachment; filename="quittance-audit-trail.csv"');
          res.status(200).send(output);
          return;
        }

        if (exportFormat === 'ndjson') {
          res.setHeader('Content-Type', 'application/x-ndjson');
          res.status(200).send(output);
          return;
        }

        res.setHeader('Content-Type', 'application/json');
        res.status(200).send(output);
      } catch (error: any) {
        logError('Export audit trail error:', error);
        sendFailure(res, 500, error.message || 'Failed to export audit trail');
      }
    },

    // Observability JSON metrics summary
    async getObservabilityMetrics(_req: Request, res: Response) {
      try {
        const summary = metrics.getMetricsSummary();
        sendSuccess(res, 200, summary);
      } catch (error: any) {
        logError('Observability metrics error:', error);
        sendFailure(res, 500, error.message || 'Failed to get metrics');
      }
    },

    // Prometheus plain text metrics
    async getPrometheusMetrics(_req: Request, res: Response) {
      try {
        const prometheusOutput = metrics.exportPrometheusMetrics();
        res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
        res.status(200).send(prometheusOutput);
      } catch (error: any) {
        logError('Prometheus metrics error:', error);
        res.status(500).send('# Error exporting metrics');
      }
    },

    // Email Template Preview for freelancer verification before sending (Issue #36)
    async getEmailPreview(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const { id } = req.params;
        const invoice = await storage.getInvoiceById(id);

        if (!invoice) {
          return sendFailure(res, 404, 'Invoice not found', {
            code: 'INVOICE_NOT_FOUND',
            correlationId: corrId,
          });
        }

        const templateType =
          (req.query.type as EmailTemplateType) ||
          (invoice.status === 'PAID' ? 'payment_proof' : 'payment_request');

        if (templateType === 'payment_proof' && invoice.status !== 'PAID') {
          return sendFailure(res, 400, 'Payment proof preview is only available for PAID invoices', {
            code: 'INVOICE_NOT_PAID',
            correlationId: corrId,
          });
        }

        const preview = emails.getPreview(invoice, templateType, { frontendUrl: frontendUrl() });
        const duration = performance.now() - start;

        metrics.recordOperation({
          operation: 'invoice.email_preview',
          actor_type: 'seller',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { invoiceId: invoice.id, templateType },
        });

        sendSuccess(res, 200, preview);
      } catch (error: any) {
        logError('Get email preview error:', error);
        sendFailure(res, 500, error.message || 'Failed to generate email preview', {
          correlationId: corrId,
        });
      }
    },

    // Secure Email Dispatch using Contextual Escaping (Issue #36)
    async sendInvoiceEmail(req: Request, res: Response) {
      const start = performance.now();
      const corrId = req.correlationId || `req-${Date.now().toString(36)}`;

      try {
        const { id } = req.params;
        const invoice = await storage.getInvoiceById(id);

        if (!invoice) {
          return sendFailure(res, 404, 'Invoice not found', {
            code: 'INVOICE_NOT_FOUND',
            correlationId: corrId,
          });
        }

        const templateType =
          (req.body?.templateType as EmailTemplateType) ||
          (invoice.status === 'PAID' ? 'payment_proof' : 'payment_request');
        const recipientEmail = req.body?.recipientEmail || invoice.customerEmail;

        if (!recipientEmail) {
          return sendFailure(res, 400, 'Recipient email address is required', {
            code: 'RECIPIENT_EMAIL_REQUIRED',
            correlationId: corrId,
          });
        }

        if (templateType === 'payment_proof' && invoice.status !== 'PAID') {
          return sendFailure(res, 400, 'Payment proof can only be sent for PAID invoices', {
            code: 'INVOICE_NOT_PAID',
            correlationId: corrId,
          });
        }

        // Optional retry key (#289); bounded so the checkpoint key fits its column.
        const idempotencyKey = req.headers?.['idempotency-key'];
        if (idempotencyKey !== undefined && (typeof idempotencyKey !== 'string' || !idempotencyKey || idempotencyKey.length > 200)) {
          return sendFailure(res, 400, 'Idempotency-Key must be 1-200 characters', {
            code: 'INVALID_IDEMPOTENCY_KEY',
            correlationId: corrId,
          });
        }

        const result = await emails.sendInvoiceEmail(invoice, templateType, {
          frontendUrl: frontendUrl(),
          recipientEmail,
          idempotencyKey,
        });

        // Record audit trail event for sent email
        if (storage.recordAuditEvent) {
          try {
            await storage.recordAuditEvent({
              action: 'EMAIL_SENT' as any,
              actor: {
                type: 'seller',
                id: invoice.sellerPublicKey,
                ip: req.ip,
                userAgent: getUserAgent(req),
              },
              scope: {
                entityType: 'invoice',
                entityId: invoice.id,
              },
              reason: `Invoice ${templateType} email dispatched to ${recipientEmail}`,
              metadata: {
                messageId: result.messageId,
                recipient: recipientEmail,
                templateType,
                subject: result.subject,
              },
              correlationId: corrId,
            });
          } catch (auditErr) {
            console.error('Audit log error on email dispatch:', auditErr);
          }
        }

        const duration = performance.now() - start;
        metrics.recordOperation({
          operation: 'invoice.send_email',
          actor_type: 'seller',
          result: 'success',
          latency_ms: duration,
          correlation_id: corrId,
          http_status: 200,
          metadata: { invoiceId: invoice.id, templateType, messageId: result.messageId },
        });

        sendSuccess(res, 200, {
          messageId: result.messageId,
          recipient: result.recipient,
          templateType,
          sentAt: new Date().toISOString(),
        });
      } catch (error: any) {
        if (error instanceof CheckpointKeyReusedError) {
          return sendFailure(res, 422, error.message, {
            code: 'IDEMPOTENCY_KEY_REUSED',
            category: 'VALIDATION',
            retryable: false,
            correlationId: corrId,
          });
        }
        if (error instanceof CheckpointInProgressError || error instanceof CheckpointInDoubtError) {
          return sendFailure(res, 409, error.message, {
            code: error instanceof CheckpointInDoubtError ? 'EMAIL_OUTCOME_IN_DOUBT' : 'EMAIL_SEND_IN_PROGRESS',
            category: 'LIFECYCLE',
            retryable: error instanceof CheckpointInProgressError,
            correlationId: corrId,
          });
        }
        logError('Send invoice email error:', error);
        sendFailure(res, 500, error.message || 'Failed to send invoice email', {
          correlationId: corrId,
        });
      }
    },
  };
}

export default createInvoiceHandlers;
