// Shopify Webhook endpoint
import { shopifyAdapter } from '../../../src/lib/adapters/shopify/index.js';
import { callBitrix, getBitrixWebhookBase } from '../../../src/lib/bitrix/client.js';
import { mapShopifyOrderToBitrixDeal } from '../../../src/lib/bitrix/orderMapper.js';
import { upsertBitrixContact } from '../../../src/lib/bitrix/contact.js';
import { BITRIX_CONFIG, financialStatusToStageId, financialStatusToPaymentStatus } from '../../../src/lib/bitrix/config.js';

// Configure body parser to accept raw JSON
export const config = {
  api: {
    bodyParser: {
      sizeLimit: '1mb',
    },
  },
};

/**
 * Unified handler for order created/updated events
 * @param {Object} order - Shopify order object
 * @param {boolean} isCreate - true if this is a create event, false if update
 */
async function handleOrderCreateOrUpdate(order, isCreate) {
  const action = isCreate ? 'created' : 'updated';
  console.log(`[SHOPIFY WEBHOOK] Handling order ${action}: ${order.name || order.id}`);
  console.log(`[SHOPIFY WEBHOOK] Order data:`, {
    id: order.id,
    name: order.name,
    total_price: order.total_price,
    current_total_price: order.current_total_price,
    financial_status: order.financial_status,
    line_items_count: order.line_items?.length || 0,
    tags: order.tags
  });

  const shopifyOrderId = String(order.id);

  // Try to find existing deal (for both create and update)
  let deal = null;
  let dealId = null;
  
  try {
    const listResp = await callBitrix('/crm.deal.list.json', {
      filter: { 'UF_SHOPIFY_ORDER_ID': shopifyOrderId },
      select: ['ID', 'OPPORTUNITY', 'STAGE_ID', 'CATEGORY_ID'],
    });
    deal = listResp.result?.[0];
    dealId = deal?.ID;
  } catch (listError) {
    console.error(`[SHOPIFY WEBHOOK] Error finding deal:`, listError);
  }

  // Map order to Bitrix deal
  const { dealFields, productRows } = mapShopifyOrderToBitrixDeal(order);
  
  console.log(`[SHOPIFY WEBHOOK] Mapped dealFields:`, JSON.stringify(dealFields, null, 2));
  console.log(`[SHOPIFY WEBHOOK] Mapped productRows count: ${productRows.length}`);

  if (isCreate && dealId) {
    console.log(`[SHOPIFY WEBHOOK] ⚠️ Deal already exists for order ${shopifyOrderId}, treating as update`);
    isCreate = false;
  }

  if (isCreate) {
  console.log(`[SHOPIFY WEBHOOK] Handling order created: ${order.name || order.id}`);
  console.log(`[SHOPIFY WEBHOOK] Order data:`, {
    id: order.id,
    name: order.name,
    total_price: order.total_price,
    current_total_price: order.current_total_price,
    financial_status: order.financial_status,
    line_items_count: order.line_items?.length || 0
  });

  // Map order to Bitrix deal
  const { dealFields, productRows } = mapShopifyOrderToBitrixDeal(order);
  
  console.log(`[SHOPIFY WEBHOOK] Mapped dealFields:`, JSON.stringify(dealFields, null, 2));
  console.log(`[SHOPIFY WEBHOOK] Mapped productRows count:`, productRows.length);
  if (productRows.length > 0) {
    console.log(`[SHOPIFY WEBHOOK] First product row:`, JSON.stringify(productRows[0], null, 2));
  }

    // CREATE: Create new deal
    // Upsert contact (non-blocking)
    let contactId = null;
    try {
      const bitrixBase = getBitrixWebhookBase();
      contactId = await upsertBitrixContact(bitrixBase, order);
      if (contactId) {
        dealFields.CONTACT_ID = contactId;
      }
    } catch (contactError) {
      console.error('[SHOPIFY WEBHOOK] Contact upsert failed (non-blocking):', contactError);
    }

    // Create deal
    console.log(`[SHOPIFY WEBHOOK] Creating deal in Bitrix with fields:`, Object.keys(dealFields));
    const dealAddResp = await callBitrix('/crm.deal.add.json', {
      fields: dealFields,
    });

    console.log(`[SHOPIFY WEBHOOK] Bitrix create response:`, JSON.stringify(dealAddResp, null, 2));

    if (!dealAddResp.result) {
      console.error(`[SHOPIFY WEBHOOK] ❌ Failed to create deal. Response:`, dealAddResp);
      throw new Error(`Failed to create deal: ${JSON.stringify(dealAddResp)}`);
    }

    dealId = dealAddResp.result;
    console.log(`[SHOPIFY WEBHOOK] ✅ Deal created: ${dealId}`);

  } else {
    // UPDATE: Update existing deal
    if (!dealId) {
      console.log(`[SHOPIFY WEBHOOK] Deal not found for Shopify order ${shopifyOrderId}, will create new one`);
      // Fallback to create if deal not found
      return await handleOrderCreateOrUpdate(order, true);
    }

    const currentCategoryId = Number(deal.CATEGORY_ID) || 2;
    console.log(`[SHOPIFY WEBHOOK] Found deal ${dealId} for order ${shopifyOrderId}, category: ${currentCategoryId}`);

    // Determine category based on order tags
    const orderTags = Array.isArray(order.tags) 
      ? order.tags 
      : (order.tags ? String(order.tags).split(',').map(t => t.trim()) : []);
    
    const preorderTags = ['pre-order', 'preorder-product-added'];
    const hasPreorderTag = orderTags.some(tag => 
      preorderTags.some(preorderTag => tag.toLowerCase() === preorderTag.toLowerCase())
    );
    
    const categoryId = hasPreorderTag ? BITRIX_CONFIG.CATEGORY_PREORDER : BITRIX_CONFIG.CATEGORY_STOCK;
    
    // Prepare update fields
    const fields = {};

    // Update category if changed
    if (categoryId !== currentCategoryId) {
      fields.CATEGORY_ID = categoryId;
      console.log(`[SHOPIFY WEBHOOK] Category changed from ${currentCategoryId} to ${categoryId}`);
    }

    // Update amount if changed
    const newAmount = Number(order.current_total_price || order.total_price || 0);
    if (newAmount !== Number(deal.OPPORTUNITY)) {
      fields.OPPORTUNITY = newAmount;
      console.log(`[SHOPIFY WEBHOOK] Amount changed: ${deal.OPPORTUNITY} → ${newAmount}`);
    }

    // Map financial status to stage ID (based on category)
    const stageId = financialStatusToStageId(order.financial_status, categoryId);
    if (stageId !== deal.STAGE_ID) {
      fields.STAGE_ID = stageId;
      console.log(`[SHOPIFY WEBHOOK] Stage updated: "${deal.STAGE_ID}" → "${stageId}"`);
    }

    // Payment status synchronization - always update
    const paymentStatusEnumId = financialStatusToPaymentStatus(order.financial_status);
    fields.UF_CRM_1739183959976 = paymentStatusEnumId;
    console.log(`[SHOPIFY WEBHOOK] Payment status: "${paymentStatusEnumId}" (${order.financial_status})`);

    // Update other fields
    if (order.current_total_discounts !== undefined) {
      fields.UF_SHOPIFY_TOTAL_DISCOUNT = Number(order.current_total_discounts);
    }
    if (order.current_total_tax !== undefined) {
      fields.UF_SHOPIFY_TOTAL_TAX = Number(order.current_total_tax);
    }
    if (order.shipping_lines?.[0]?.price !== undefined) {
      const shippingPrice = Number(
        order.current_total_shipping_price_set?.shop_money?.amount ||
        order.total_shipping_price_set?.shop_money?.amount ||
        order.shipping_price ||
        order.shipping_lines?.[0]?.price ||
        0
      );
      fields.UF_SHOPIFY_SHIPPING_PRICE = shippingPrice;
    }

    // Update deal if there are changes
    if (Object.keys(fields).length > 0) {
      console.log(`[SHOPIFY WEBHOOK] Updating deal ${dealId} with fields:`, Object.keys(fields));
      await callBitrix('/crm.deal.update.json', {
        id: dealId,
        fields,
      });
      console.log(`[SHOPIFY WEBHOOK] ✅ Deal ${dealId} updated`);
    } else {
      console.log(`[SHOPIFY WEBHOOK] No fields to update for deal ${dealId}`);
    }
  }

  // Always update product rows (for both create and update)
  if (productRows && productRows.length > 0) {
    try {
      console.log(`[SHOPIFY WEBHOOK] Updating product rows for deal ${dealId}: ${productRows.length} rows`);
      await callBitrix('/crm.deal.productrows.set.json', {
        id: dealId,
        rows: productRows,
      });
      console.log(`[SHOPIFY WEBHOOK] ✅ Product rows updated for deal ${dealId}: ${productRows.length} rows`);
    } catch (productRowsError) {
      console.error(`[SHOPIFY WEBHOOK] ❌ Product rows error:`, productRowsError);
      // Don't throw - deal is already created/updated
    }
  } else {
    // If no product rows (e.g., all items removed), clear rows to keep Bitrix in sync
    try {
      await callBitrix('/crm.deal.productrows.set.json', {
        id: dealId,
        rows: [],
      });
      console.log(`[SHOPIFY WEBHOOK] ✅ Product rows cleared for deal ${dealId}`);
    } catch (clearError) {
      console.error(`[SHOPIFY WEBHOOK] ❌ Error clearing product rows:`, clearError);
    }
  }

  return dealId;
}

/**
 * Handle order created event - create deal in Bitrix
 * @deprecated Use handleOrderCreateOrUpdate instead
 */
async function handleOrderCreated(order) {
  return await handleOrderCreateOrUpdate(order, true);
}

/**
 * Handle order updated event - update deal in Bitrix
 * @deprecated Use handleOrderCreateOrUpdate instead
 */
async function handleOrderUpdated(order) {
  return await handleOrderCreateOrUpdate(order, false);
}

/**
 * Handle refund created event - update deal in Bitrix
 * Note: Shopify refund webhook sends refund object, not order object
 */
async function handleRefundCreated(refundData) {
  console.log(`[SHOPIFY WEBHOOK] Handling refund created`);
  console.log(`[SHOPIFY WEBHOOK] Refund data:`, {
    order_id: refundData.order_id,
    refund_id: refundData.id,
    amount: refundData.amount,
    currency: refundData.currency,
    refund_line_items: refundData.refund_line_items?.length || 0
  });

  const shopifyOrderId = String(refundData.order_id);
  
  // 1. Find deal by UF_SHOPIFY_ORDER_ID
  const listResp = await callBitrix('/crm.deal.list.json', {
    filter: { 'UF_SHOPIFY_ORDER_ID': shopifyOrderId },
    select: ['ID', 'OPPORTUNITY', 'STAGE_ID', 'CATEGORY_ID'],
  });

  const deal = listResp.result?.[0];
  if (!deal) {
    console.log(`[SHOPIFY WEBHOOK] Deal not found for Shopify order ${shopifyOrderId}`);
    return;
  }

  const dealId = deal.ID;
  const currentCategoryId = Number(deal.CATEGORY_ID) || 2;
  console.log(`[SHOPIFY WEBHOOK] Found deal ${dealId} for refund on order ${shopifyOrderId}`);

  // 2. Get full order from Shopify to recalculate totals
  try {
    const { getOrder } = await import('../../../src/lib/shopify/adminClient.js');
    const shopifyOrder = await getOrder(shopifyOrderId);
    
    if (!shopifyOrder) {
      console.error(`[SHOPIFY WEBHOOK] Order ${shopifyOrderId} not found in Shopify`);
      return;
    }

    // 3. Recalculate deal amount and product rows based on current order state (after refund)
    const { dealFields, productRows } = mapShopifyOrderToBitrixDeal(shopifyOrder);
    
    // 4. Update deal with new amount and product rows
    const fields = {
      OPPORTUNITY: dealFields.OPPORTUNITY,
      UF_SHOPIFY_TOTAL_DISCOUNT: dealFields.UF_SHOPIFY_TOTAL_DISCOUNT,
      UF_SHOPIFY_SHIPPING_PRICE: dealFields.UF_SHOPIFY_SHIPPING_PRICE,
      UF_SHOPIFY_TOTAL_TAX: dealFields.UF_SHOPIFY_TOTAL_TAX,
    };

    // Update payment status based on refund
    const refundAmount = Number(refundData.amount || 0);
    const orderTotal = Number(shopifyOrder.total_price || 0);
    const remainingAmount = orderTotal - refundAmount;
    
    if (remainingAmount <= 0) {
      // Full refund
      fields.UF_CRM_1739183959976 = '58'; // Unpaid
      fields.STAGE_ID = financialStatusToStageId('refunded', currentCategoryId);
    } else if (refundAmount > 0) {
      // Partial refund
      fields.UF_CRM_1739183959976 = '58'; // Unpaid (or could be '60' for partial)
    }

    await callBitrix('/crm.deal.update.json', {
      id: dealId,
      fields,
    });
    console.log(`[SHOPIFY WEBHOOK] Deal ${dealId} updated after refund`);

    // 5. Update product rows to reflect refunded quantities
    if (productRows && productRows.length > 0) {
      await callBitrix('/crm.deal.productrows.set.json', {
        id: dealId,
        rows: productRows,
      });
      console.log(`[SHOPIFY WEBHOOK] Product rows updated for deal ${dealId} after refund: ${productRows.length} rows`);
    }

  } catch (error) {
    console.error(`[SHOPIFY WEBHOOK] Error processing refund:`, error);
    // Don't throw - refund is already processed in Shopify
  }

  return dealId;
}

export default async function handler(req, res) {
  // Enhanced logging - log ALL incoming requests immediately
  const requestId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  console.log(`[SHOPIFY WEBHOOK] ===== INCOMING REQUEST [${requestId}] =====`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Timestamp: ${new Date().toISOString()}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Method: ${req.method}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] URL: ${req.url}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] All headers:`, JSON.stringify(req.headers, null, 2));
  
  if (req.method !== 'POST') {
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Method not allowed: ${req.method}`);
    res.status(405).end('Method not allowed');
    return;
  }

  // Log raw body size
  const bodyString = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Body size: ${bodyString?.length || 0} bytes`);
  
  const topic = req.headers['x-shopify-topic'] || req.headers['X-Shopify-Topic'];
  const shopifyShopDomain = req.headers['x-shopify-shop-domain'] || req.headers['X-Shopify-Shop-Domain'];
  const shopifyHmac = req.headers['x-shopify-hmac-sha256'] || req.headers['X-Shopify-Hmac-Sha256'];
  
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Topic: ${topic || 'MISSING!'}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Shop Domain: ${shopifyShopDomain || 'MISSING!'}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] HMAC Present: ${!!shopifyHmac}`);
  
  const order = req.body;
  
  // Try to extract order info even if structure is different
  const orderId = order?.id || order?.order_id || order?.order?.id || 'N/A';
  const orderName = order?.name || order?.order_name || order?.order?.name || 'N/A';
  
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Order ID: ${orderId}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Order Name: ${orderName}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Body keys: ${Object.keys(order || {}).join(', ')}`);

  // If no topic, log full body for debugging
  if (!topic) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ NO TOPIC HEADER! Full body:`, JSON.stringify(order, null, 2));
  }

  try {
    // Store event for monitoring (non-blocking)
    try {
      const storedEvent = shopifyAdapter.storeEvent(order);
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Event stored. Topic: ${topic}, Order: ${orderName || orderId}`);
    } catch (storeError) {
      console.error(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ Failed to store event:`, storeError);
    }

    // Handle different topics
    if (topic === 'orders/create' || topic === 'orders/updated') {
      // Unified handler for both create and update
      // Create will create deal if not exists, update will update existing deal
      await handleOrderCreateOrUpdate(order, topic === 'orders/create');
    } else if (topic === 'refunds/create') {
      await handleRefundCreated(order);
    } else {
      // For other topics just log and return 200
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] Unhandled topic: ${topic || 'null/undefined'}`);
    }

    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Request processed successfully`);
    res.status(200).json({ success: true, requestId, topic });
  } catch (e) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Error:`, e);
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] Error stack:`, e.stack);
    res.status(500).json({ 
      error: 'Internal server error', 
      message: e.message,
      requestId 
    });
  }
}

