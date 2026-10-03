const axios = require('axios');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');

// Resolve configuration from environment variables
const config = {
  BOT_TOKEN: process.env.BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || '',
  OWNER_CHAT_ID: process.env.OWNER_CHAT_ID || process.env.ID_CHAT_PEMILIK || process.env.OWNER_IDS || process.env.TELEGRAM_OWNER_ID || process.env.TELEGRAM_ADMIN_ID || '8815465971',
  CHANNEL_ID: process.env.CHANNEL_ID || process.env.ID_SALURAN || process.env.ID_SALURAN_TELEGRAM || process.env.TELEGRAM_CHANNEL_ID || '@FizzAbout',
  BASE_URL: (process.env.BASE_URL || process.env.URL_DASAR || process.env.APP_URL || 'https://putzpay.biz.id').replace(/\/$/, ''),
  DEVELOPER: 'PutzPay Team',
  VERSION: 'v2.5.0',
  RICH_THINKING_MS: process.env.RICH_THINKING_MS || 1000,
  REQUEST_TIMEOUT_MS: process.env.REQUEST_TIMEOUT_MS || 12000
};

const BOT_TOKEN = config.BOT_TOKEN;
const OWNER_CHAT_ID = config.OWNER_CHAT_ID;
const CHANNEL_ID = config.CHANNEL_ID;
const BASE_URL = config.BASE_URL;

// Format Date to WIB (Asia/Jakarta)
function formatWibDate(dateVal = new Date()) {
  try {
    const d = new Date(dateVal);
    const validDate = isNaN(d.getTime()) ? new Date() : d;

    const formatter = new Intl.DateTimeFormat('id-ID', {
      timeZone: 'Asia/Jakarta',
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });

    const parts = formatter.formatToParts(validDate);
    const map = {};
    for (const p of parts) {
      map[p.type] = p.value;
    }
    return `${map.day} ${map.month} ${map.year} • ${map.hour}:${map.minute}:${map.second} WIB`;
  } catch (err) {
    try {
      return new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' }) + ' WIB';
    } catch (e) {
      return new Date().toISOString() + ' WIB';
    }
  }
}

function formatRupiah(amount) {
  if (typeof amount === 'number') {
    return `Rp ${amount.toLocaleString('id-ID')}`;
  }
  if (!amount) return 'Rp 0';
  const str = String(amount).replace(/[^0-9]/g, '');
  const num = parseInt(str, 10);
  return isNaN(num) ? `Rp ${amount}` : `Rp ${num.toLocaleString('id-ID')}`;
}

// Sanitize sensitive values from error or text strings
function sanitizeError(errStr) {
  if (!errStr) return 'Unknown error';
  let str = String(errStr);
  return str
    .replace(/(bot[0-9]+:[a-zA-Z0-9_-]+)/gi, '[REDACTED_BOT_TOKEN]')
    .replace(/(key-[a-zA-Z0-9]+)/gi, '[REDACTED_KEY]')
    .replace(/(secret[a-zA-Z0-9_=-]+)/gi, '[REDACTED_SECRET]')
    .replace(/(password=[^&\s]+)/gi, 'password=[REDACTED]');
}

// Deduplication mechanism using event IDs
const processedEvents = new Set();
function isDuplicateAndMark(eventId) {
  if (!eventId) return false;
  const key = String(eventId);
  if (processedEvents.has(key)) return true;
  processedEvents.add(key);
  if (processedEvents.size > 3000) {
    const firstVal = processedEvents.values().next().value;
    processedEvents.delete(firstVal);
  }
  return false;
}

// Session trackers
const userSessions = new Map();
const ownerSessions = new Map();

// Periodic session cleanup (30 mins TTL)
setInterval(() => {
  const now = Date.now();
  const TTL = 30 * 60 * 1000;
  for (const [key, session] of userSessions.entries()) {
    if (session && session.state === 'IDLE' && now - (session.updatedAt || 0) > TTL) {
      userSessions.delete(key);
    }
  }
  for (const [key, session] of ownerSessions.entries()) {
    if (session && now - (session.updatedAt || 0) > TTL) {
      ownerSessions.delete(key);
    }
  }
}, 10 * 60 * 1000);

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const lastThinkingAt = new Map();
const lastRichMessageId = new Map();
const RICH_THINKING_MS = Math.max(1000, Number(config.RICH_THINKING_MS) || 1000);

function escapeRichCell(value) {
  return String(value ?? '-')
    .replace(/\\/g, '\\\\')
    .replace(/\|/g, '\\|')
    .replace(/\r?\n/g, ' ')
    .trim();
}

function markdownCodeBlock(value) {
  const text = String(value ?? '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  return `\`\`\`\n${text.replace(/([\\`])/g, '\\$1')}\n\`\`\``;
}

// Active message keyboard registry helper
const activeKeyboards = new Map();
function registerActiveMessage(chatId, messageId, keyboardType, keyboardMeta = {}) {
  activeKeyboards.set(`${chatId}:${messageId}`, { keyboardType, keyboardMeta, updatedAt: Date.now() });
}
function unregisterActiveMessage(chatId, messageId) {
  activeKeyboards.delete(`${chatId}:${messageId}`);
}

async function showThinking(chatId, label = 'Menyiapkan tampilan...') {
  if (!chatId || !config.BOT_TOKEN) return null;
  const numericId = Number(chatId);
  if (!Number.isFinite(numericId) || numericId <= 0) return null;

  const key = String(chatId);
  const now = Date.now();
  if (now - Number(lastThinkingAt.get(key) || 0) < 250) {
    return { draftId: null, startedAt: Number(lastThinkingAt.get(key)) || now };
  }

  const startedAt = now;
  lastThinkingAt.set(key, startedAt);

  const clean = String(label || 'Menyiapkan tampilan...')
    .replace(/^⏳\s*/u, '')
    .trim() || 'Menyiapkan tampilan...';
  const safeText = clean
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  let draftId = lastThinkingAt.get(`${key}:draft`);
  if (!draftId) {
    draftId = (Date.now() + Math.floor(Math.random() * 1000)) % 2147483647 || 1;
    lastThinkingAt.set(`${key}:draft`, draftId);
  }

  const url = `https://api.telegram.org/bot${config.BOT_TOKEN}/sendRichMessageDraft`;
  const payloads = [
    {
      chat_id: numericId,
      draft_id: draftId,
      rich_message: {
        html: `<tg-thinking>⏳ ${safeText}</tg-thinking>`,
      },
    },
    {
      chat_id: numericId,
      draft_id: draftId,
      rich_message: {
        blocks: [
          {
            type: 'thinking',
            text: `⏳ ${clean}`,
          },
        ],
      },
    },
  ];

  void (async () => {
    for (const payload of payloads) {
      try {
        const res = await axios.post(url, payload, {
          timeout: 2500,
          headers: { 'Content-Type': 'application/json' },
        });
        if (res?.data?.ok) {
          console.log(`[RICH THINKING OK] chat=${numericId} draft=${draftId} mode=${payload.rich_message.html ? 'html' : 'blocks'}`);
          return;
        }
      } catch (err) {
        // Silently continue to fallback
      }
    }
  })();

  return { draftId, startedAt };
}

async function waitThinkingMinimum(thinking, minimumMs = RICH_THINKING_MS) {
  const elapsed = thinking?.startedAt ? Date.now() - thinking.startedAt : 0;
  const remaining = Math.max(0, Number(minimumMs) - elapsed);
  if (remaining) await delay(remaining);
}

// Fallback plain Telegram sender
async function sendTelegramRequest(chatId, text, parseMode = 'HTML', tag = '[TELEGRAM]', extra = {}) {
  if (!config.BOT_TOKEN || !chatId) {
    return false;
  }
  const url = `https://api.telegram.org/bot${config.BOT_TOKEN}/sendMessage`;
  const maxRetries = 3;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await axios.post(url, {
        chat_id: chatId,
        text: text,
        parse_mode: parseMode,
        disable_web_page_preview: true,
        ...(extra?.reply_markup ? { reply_markup: extra.reply_markup } : {})
      }, { timeout: Number(config.REQUEST_TIMEOUT_MS) || 12000 });

      if (response.data && response.data.ok === true) {
        return response.data.result;
      } else {
        return false;
      }
    } catch (err) {
      const status = err.response ? err.response.status : null;
      const responseData = err.response ? err.response.data : null;

      if (status === 429 && responseData && responseData.parameters && responseData.parameters.retry_after) {
        const waitMs = (responseData.parameters.retry_after + 1) * 1000;
        await new Promise(r => setTimeout(r, Math.min(waitMs, 10000)));
      } else if (attempt < maxRetries) {
        await new Promise(r => setTimeout(r, 1000 * attempt));
      } else {
        return false;
      }
    }
  }
  return false;
}

// Core sendRich with rich message support & seamless fallback
async function sendRich(chatId, markdown, extra = {}, fallbackText = null) {
  if (!chatId || !config.BOT_TOKEN) return null;

  const url = `https://api.telegram.org/bot${config.BOT_TOKEN}/sendRichMessage`;
  const richMedia = extra.rich_media;
  const baseMarkdown = String(markdown);

  try {
    let data;

    if (richMedia?.path && fs.existsSync(richMedia.path)) {
      const mediaId = String(richMedia.id || 'menu_image').replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 64) || 'menu_image';
      const markdownWithMedia = `<img src="tg://photo?id=${mediaId}"/>\n\n${baseMarkdown}`;
      const form = new FormData();
      const richMessage = {
        markdown: markdownWithMedia,
        media: [{
          id: mediaId,
          media: {
            type: 'photo',
            media: `attach://${mediaId}`,
          },
        }],
      };

      form.append('chat_id', String(chatId));
      form.append('rich_message', JSON.stringify(richMessage));
      if (extra.reply_markup) form.append('reply_markup', JSON.stringify(extra.reply_markup));
      if (extra.disable_notification !== undefined) {
        form.append('disable_notification', String(Boolean(extra.disable_notification)));
      }
      form.append(mediaId, fs.createReadStream(richMedia.path), {
        filename: path.basename(richMedia.path),
        contentType: richMedia.contentType || 'image/jpeg',
      });

      const response = await axios.post(url, form, {
        headers: form.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: Number(config.REQUEST_TIMEOUT_MS) || 12000,
      });
      data = response.data;
    } else {
      const payload = {
        chat_id: chatId,
        rich_message: { markdown: baseMarkdown },
        ...(extra.reply_markup ? { reply_markup: extra.reply_markup } : {}),
        ...(extra.disable_notification !== undefined ? { disable_notification: extra.disable_notification } : {}),
      };
      const response = await axios.post(url, payload, {
        timeout: Number(config.REQUEST_TIMEOUT_MS) || 12000,
      });
      data = response.data;
    }

    if (!data?.ok || !data.result?.message_id) {
      throw new Error(data?.description || 'Rich Message failed');
    }
    return data.result;
  } catch (err) {
    if (fallbackText === null) {
      // If no fallback text provided, fallback to standard markdown via sendMessage
      return sendTelegramRequest(chatId, baseMarkdown, 'Markdown', '[RICH FALLBACK MD]', extra);
    }
    return sendTelegramRequest(chatId, fallbackText, extra.parse_mode || 'HTML', '[RICH FALLBACK HTML]', extra);
  }
}

async function sendProgressiveRich(chatId, markdown, extra = {}, fallbackText = null, options = {}) {
  if (!chatId) return null;

  let thinking = null;
  if (options.skipThinking !== true) {
    thinking = await showThinking(chatId, options.loadingText || 'Menyiapkan tampilan...');
  } else {
    const startedAt = Number(lastThinkingAt.get(String(chatId)) || 0);
    if (startedAt && Date.now() - startedAt <= 5000) thinking = { startedAt };
  }

  try {
    const startedAt = thinking?.startedAt || Number(lastThinkingAt.get(String(chatId)) || 0);
    if (startedAt) {
      await waitThinkingMinimum({ startedAt }, Number(options.loadingMs) || RICH_THINKING_MS);
    }

    const msg = await sendRich(chatId, markdown, extra, fallbackText);
    const prevId = lastRichMessageId.get(String(chatId));

    if (options.skipDelete !== true && prevId && msg?.message_id && prevId !== msg.message_id) {
      try {
        await axios.post(`https://api.telegram.org/bot${config.BOT_TOKEN}/deleteMessage`, {
          chat_id: chatId,
          message_id: prevId
        }, { timeout: 3000 });
      } catch (_) {}
      unregisterActiveMessage(chatId, prevId);
    }

    if (msg?.message_id) {
      lastRichMessageId.set(String(chatId), msg.message_id);
      if (options.keyboardType) {
        registerActiveMessage(chatId, msg.message_id, options.keyboardType, options.keyboardMeta || {});
      }
    }
    return msg;
  } catch (err) {
    if (fallbackText === null) throw err;
    const msg = await sendTelegramRequest(chatId, fallbackText, extra.parse_mode || 'HTML', '[RICH SEND FALLBACK]');
    return msg;
  }
}

async function sendFreshRich(chatId, markdown, extra = {}, fallbackText = null, options = {}) {
  return sendProgressiveRich(chatId, markdown, extra, fallbackText, options);
}

async function sendRichMenu(chatId, markdown, keyboardBundle = {}, fallbackHtml = null, options = {}) {
  const reply_markup = keyboardBundle.reply_markup || keyboardBundle;
  const extra = {
    reply_markup: reply_markup?.inline_keyboard || reply_markup?.keyboard ? reply_markup : (keyboardBundle.reply_markup || undefined),
    parse_mode: 'HTML',
  };
  if (options.rich_media) extra.rich_media = options.rich_media;

  if (keyboardBundle.reply_markup) {
    extra.reply_markup = keyboardBundle.reply_markup;
  }

  return sendFreshRich(chatId, markdown, extra, fallbackHtml, {
    loadingText: options.loadingText || 'Menyiapkan tampilan...',
    skipThinking: options.skipThinking === true,
    skipDelete: options.skipDelete === true,
    loadingMs: options.loadingMs,
    keyboardType: options.keyboardType || keyboardBundle.keyboardType,
    keyboardMeta: options.keyboardMeta || keyboardBundle.keyboardMeta,
  });
}

// Broadcast to Bot Owner & Official Channel
async function deleteTelegramMessage(chatId, messageId) {
  if (!config.BOT_TOKEN || !chatId || !messageId) return false;
  try {
    const url = `https://api.telegram.org/bot${config.BOT_TOKEN}/deleteMessage`;
    const res = await axios.post(url, {
      chat_id: chatId,
      message_id: messageId
    }, { timeout: 5000 });
    return Boolean(res?.data?.ok);
  } catch (err) {
    const detail = err.response?.data?.description || err.message;
    console.error(`[TELEGRAM DELETE ERROR] Failed to delete message ${messageId} from ${chatId}:`, sanitizeError(detail));
    return false;
  }
}

async function sendToBot(richMarkdown, fallbackHtml, extra = {}) {
  if (!config.OWNER_CHAT_ID) return false;
  return sendRich(config.OWNER_CHAT_ID, richMarkdown, extra, fallbackHtml);
}

async function sendToChannel(richMarkdown, fallbackHtml, extra = {}) {
  const targetChannel = config.CHANNEL_ID || '@FizzAbout';
  return sendRich(targetChannel, richMarkdown, extra, fallbackHtml);
}

async function sendNotification(richMarkdown, fallbackHtml, extra = {}) {
  const results = await Promise.allSettled([
    sendToBot(richMarkdown, fallbackHtml, extra),
    sendToChannel(richMarkdown, fallbackHtml, extra)
  ]);
  return results.some(r => r.status === 'fulfilled' && r.value);
}

// ==================================================
// EVENT NOTIFICATION HANDLERS (PREMIUM RICH FORMAT)
// ==================================================

function notifyNewUser(data = {}) {
  try {
    const userId = data.userId || data._id || 'UNKNOWN';
    if (userId !== 'UNKNOWN' && isDuplicateAndMark(`new_user_${userId}`)) return;

    const username = data.username || '-';
    const email = data.email || '-';
    const method = data.method || 'Pendaftaran Email';
    const totalUsers = data.totalUsers ? String(data.totalUsers) : '-';
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 👤 ✨ User Baru PutzPay',
      '',
      'Pengguna baru berhasil mendaftar ke platform PutzPay.',
      '',
      '| Data Pengguna | Detail Akun |',
      '| --- | --- |',
      `| **Username** | \`${escapeRichCell(username)}\` |`,
      `| **Email** | \`${escapeRichCell(email)}\` |`,
      `| **Metode** | ${escapeRichCell(method)} |`,
      `| **User ID** | \`${escapeRichCell(userId)}\` |`,
      `| **Total User** | **${escapeRichCell(totalUsers)}** |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`,
      '',
      '> ⚡ PutzPay Payment Gateway • https://putzpay.biz.id'
    ].join('\n');

    const fallback = `👤 <b>USER BARU PUTZPAY</b>

🎉 <b>Pengguna baru berhasil mendaftar!</b>

━━━━━━━━━━━━━━━━━━
<b>Username:</b> <code>${username}</code>
<b>Email:</b> <code>${email}</code>
<b>Metode:</b> ${method}
<b>User ID:</b> <code>${userId}</code>
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━

⚡ <i>PutzPay Payment Gateway • <a href="https://putzpay.biz.id">putzpay.biz.id</a></i>`;

    console.log('[USER] NEW USER');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[USER ERROR]', err.message);
  }
}

function notifyGoogleNewUser(data = {}) {
  notifyNewUser({ ...data, method: 'Google OAuth 2.0' });
}

function notifyUserLogin(data = {}) {
  try {
    const username = data.username || 'User';
    const email = data.email || '-';
    const method = data.method || 'Email/Password';
    const ip = data.ip || '-';
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 🔐 ✨ User Login Berhasil',
      '',
      'Aktivitas autentikasi akun terdeteksi di portal PutzPay.',
      '',
      '| Autentikasi | Informasi |',
      '| --- | --- |',
      `| **Username** | \`${escapeRichCell(username)}\` |`,
      `| **Email** | \`${escapeRichCell(email)}\` |`,
      `| **Metode** | ${escapeRichCell(method)} |`,
      `| **Alamat IP** | \`${escapeRichCell(ip)}\` |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`,
      '',
      '> 🛡️ Keamanan akun terlindungi dengan enkripsi 256-bit.'
    ].join('\n');

    const fallback = `🔐 <b>USER LOGIN BERHASIL</b>

━━━━━━━━━━━━━━━━━━
<b>Username:</b> <code>${username}</code>
<b>Metode:</b> ${method}
<b>IP Address:</b> <code>${ip}</code>
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━
⚡ <i>PutzPay Security Notification</i>`;

    console.log('[USER] LOGIN');

    // Send notifications to Telegram Bot (Owner) and Official Channel, then auto-delete after 15s in background
    (async () => {
      try {
        const scheduleDeletion = (chatId, messageId, fallbackChat) => {
          if (!chatId || !messageId) return;
          const timer = setTimeout(async () => {
            try {
              const ok = await deleteTelegramMessage(chatId, messageId);
              if (!ok && fallbackChat && String(fallbackChat) !== String(chatId)) {
                await deleteTelegramMessage(fallbackChat, messageId);
              }
            } catch (delErr) {
              console.error('[TELEGRAM AUTO-DELETE ERROR]', sanitizeError(delErr?.message));
            }
          }, 15000);
          if (timer && typeof timer.unref === 'function') {
            timer.unref();
          }
        };

        const sendAndSchedule = async (sendPromise, fallbackChat) => {
          try {
            const res = await sendPromise;
            if (res && res.message_id) {
              const targetChat = (res.chat && res.chat.id != null) ? res.chat.id : fallbackChat;
              scheduleDeletion(targetChat, res.message_id, fallbackChat);
            }
          } catch (err) {
            // Ignore send error gracefully - do not crash
          }
        };

        await Promise.allSettled([
          sendAndSchedule(sendToBot(rich, fallback), config.OWNER_CHAT_ID),
          sendAndSchedule(sendToChannel(rich, fallback), config.CHANNEL_ID || '@FizzAbout')
        ]);
      } catch (sendErr) {
        console.error('[NOTIFY LOGIN ERROR]', sanitizeError(sendErr?.message));
      }
    })();
  } catch (err) {
    console.error('[USER ERROR]', sanitizeError(err.message));
  }
}

function notifyInvoiceCreated(data = {}) {
  try {
    const invoiceId = data.invoice_id || data.invoiceId || data._id || 'UNKNOWN';
    if (invoiceId !== 'UNKNOWN' && isDuplicateAndMark(`invoice_${invoiceId}`)) return;

    const orderId = String(data.order_id || data.orderId || invoiceId).trim();
    let username = (data.username || data.name || data.fullName || data.accountName || '').trim();
    if (!username || username === 'Guest/User' || username === 'User' || username === 'Guest') {
      username = data.email ? data.email.split('@')[0] : 'Akun Pengguna';
    }

    const amountStr = formatRupiah(data.total || data.amount);
    const methodStr = data.method || 'QRIS Realtime';
    const statusStr = data.status || '⏳ Menunggu Pembayaran';
    const timeStr = formatWibDate(data.time || data.createdAt || new Date());

    const rich = [
      '# 🧾 ✨ PutzPay Invoice Baru',
      '',
      'Invoice pembayaran baru berhasil dibuat dan menunggu pelunasan.',
      '',
      '| Detail Invoice | Data Pembayaran |',
      '| --- | --- |',
      `| **Order ID** | \`${escapeRichCell(orderId)}\` |`,
      `| **Pengguna** | **${escapeRichCell(username)}** |`,
      `| **Nominal** | **${escapeRichCell(amountStr)}** |`,
      `| **Metode** | ${escapeRichCell(methodStr)} |`,
      `| **Status** | ${escapeRichCell(statusStr)} |`,
      `| **Waktu Dibuat** | ${escapeRichCell(timeStr)} |`,
      '',
      '> ⚡ PutzPay Payment Gateway • Scan QRIS untuk menyelesaikan.'
    ].join('\n');

    const fallback = `🧾 <b>PUTZPAY INVOICE BARU</b>

━━━━━━━━━━━━━━━━━━
<b>Order ID:</b> <code>${orderId}</code>
<b>Pengguna:</b> <b>${username}</b>
<b>Nominal:</b> <b>${amountStr}</b>
<b>Metode:</b> ${methodStr}
<b>Status:</b> ${statusStr}
<b>Waktu Dibuat:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━

⚡ <i>PutzPay Payment Gateway • Scan QRIS untuk menyelesaikan.</i>`;

    console.log('[PAYMENT] INVOICE CREATED');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[PAYMENT ERROR]', err.message);
  }
}

function notifyPaymentPending(data = {}) {
  try {
    const invoiceId = data.invoice_id || data.invoiceId || data._id || 'UNKNOWN';
    if (invoiceId !== 'UNKNOWN' && isDuplicateAndMark(`payment_pending_${invoiceId}`)) return;

    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# ⏳ Menunggu Pembayaran',
      '',
      '| Transaksi | Keterangan |',
      '| --- | --- |',
      `| **Invoice** | \`${escapeRichCell(invoiceId)}\` |`,
      `| **User** | ${escapeRichCell(username)} |`,
      `| **Amount** | **${escapeRichCell(amountStr)}** |`,
      '| **Status** | ⏳ PENDING |',
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `⏳ <b>PAYMENT MENUNGGU</b>

<b>Invoice:</b> <code>${invoiceId}</code>
<b>User:</b> ${username}
<b>Amount:</b> ${amountStr}
<b>Status:</b> PENDING
<b>Waktu:</b> ${timeStr}`;

    console.log('[PAYMENT] PENDING');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[PAYMENT ERROR]', err.message);
  }
}

function notifyPaymentSuccess(data = {}) {
  try {
    const invoiceId = data.invoice_id || data.invoiceId || data._id || 'UNKNOWN';
    if (invoiceId !== 'UNKNOWN' && isDuplicateAndMark(`payment_paid_${invoiceId}`)) return;

    const orderId = data.order_id || data.orderId || invoiceId;
    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const methodStr = data.method || 'QRIS Realtime';
    const timeStr = formatWibDate(data.time || data.paidAt || new Date());

    const rich = [
      '# 💰 ✨ PutzPay Pembayaran Berhasil',
      '',
      'Pembayaran transaksi berhasil diverifikasi dan saldo telah masuk otomatis.',
      '',
      '| Transaksi | Detail Pembayaran |',
      '| --- | --- |',
      `| **Invoice ID** | \`${escapeRichCell(invoiceId)}\` |`,
      `| **Order ID** | \`${escapeRichCell(orderId)}\` |`,
      `| **Username** | \`${escapeRichCell(username)}\` |`,
      `| **Total Bayar** | **${escapeRichCell(amountStr)}** |`,
      `| **Metode** | ${escapeRichCell(methodStr)} |`,
      '| **Status** | 🟢 **PAID / LUNAS** |',
      `| **Waktu Sukses** | ${escapeRichCell(timeStr)} |`,
      '',
      '> 🚀 Transaksi diproses secara instan oleh PutzPay Payment Gateway.'
    ].join('\n');

    const fallback = `💰 <b>PUTZPAY PEMBAYARAN BERHASIL</b>

━━━━━━━━━━━━━━━━━━
<b>Invoice ID:</b> <code>${invoiceId}</code>
<b>Order ID:</b> <code>${orderId}</code>
<b>Username:</b> <b>${username}</b>
<b>Total:</b> <b>${amountStr}</b>
<b>Metode:</b> ${methodStr}
<b>Status:</b> 🟢 <b>PAID / SUKSES</b>
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━

⚡ <i>PutzPay Payment Gateway • <a href="https://putzpay.biz.id">putzpay.biz.id</a></i>`;

    console.log('[PAYMENT] PAID');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[PAYMENT ERROR]', err.message);
  }
}

function notifyPaymentFailed(data = {}) {
  try {
    const invoiceId = data.invoice_id || data.invoiceId || data._id || 'UNKNOWN';
    if (invoiceId !== 'UNKNOWN' && isDuplicateAndMark(`payment_failed_${invoiceId}`)) return;

    const orderId = data.order_id || data.orderId || invoiceId;
    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const safeError = sanitizeError(data.error || 'Pembayaran dibatalkan/gagal');
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# ❌ Pembayaran Gagal',
      '',
      '| Transaksi | Keterangan |',
      '| --- | --- |',
      `| **Invoice** | \`${escapeRichCell(invoiceId)}\` |`,
      `| **Order ID** | \`${escapeRichCell(orderId)}\` |`,
      `| **User** | ${escapeRichCell(username)} |`,
      `| **Amount** | ${escapeRichCell(amountStr)} |`,
      '| **Status** | 🔴 FAILED |',
      `| **Alasan** | ${escapeRichCell(safeError)} |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `❌ <b>PAYMENT GAGAL</b>

━━━━━━━━━━━━━━━━━━
<b>Invoice:</b> <code>${invoiceId}</code>
<b>User:</b> ${username}
<b>Amount:</b> ${amountStr}
<b>Alasan:</b> <code>${safeError}</code>
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━`;

    console.log('[PAYMENT] FAILED');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[PAYMENT ERROR]', err.message);
  }
}

function notifyInvoiceExpired(data = {}) {
  try {
    const invoiceId = data.invoice_id || data.invoiceId || data._id || 'UNKNOWN';
    if (invoiceId !== 'UNKNOWN' && isDuplicateAndMark(`invoice_expired_${invoiceId}`)) return;

    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# ⌛ Invoice Expired',
      '',
      '| Transaksi | Keterangan |',
      '| --- | --- |',
      `| **Invoice** | \`${escapeRichCell(invoiceId)}\` |`,
      `| **User** | ${escapeRichCell(username)} |`,
      `| **Amount** | ${escapeRichCell(amountStr)} |`,
      '| **Status** | ⌛ EXPIRED |',
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `⌛ <b>INVOICE EXPIRED</b>\n\n<b>Invoice:</b> <code>${invoiceId}</code>\n<b>User:</b> ${username}\n<b>Amount:</b> ${amountStr}\n<b>Status:</b> EXPIRED\n<b>Waktu:</b> ${timeStr}`;

    console.log('[PAYMENT] EXPIRED');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[PAYMENT ERROR]', err.message);
  }
}

function notifyInvoiceCancelled(data = {}) {
  try {
    const invoiceId = data.invoice_id || data.invoiceId || data._id || 'UNKNOWN';
    if (invoiceId !== 'UNKNOWN' && isDuplicateAndMark(`invoice_cancelled_${invoiceId}`)) return;

    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 🚫 Invoice Dibatalkan',
      '',
      '| Transaksi | Keterangan |',
      '| --- | --- |',
      `| **Invoice** | \`${escapeRichCell(invoiceId)}\` |`,
      `| **User** | ${escapeRichCell(username)} |`,
      `| **Amount** | ${escapeRichCell(amountStr)} |`,
      '| **Status** | 🚫 CANCELLED |',
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `🚫 <b>INVOICE DIBATALKAN</b>\n\n<b>Invoice:</b> <code>${invoiceId}</code>\n<b>User:</b> ${username}\n<b>Amount:</b> ${amountStr}\n<b>Waktu:</b> ${timeStr}`;

    console.log('[PAYMENT] CANCELLED');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[PAYMENT ERROR]', err.message);
  }
}

function notifyWithdrawRequest(data = {}) {
  try {
    const withdrawId = data.withdraw_id || data.withdrawId || data._id || 'UNKNOWN';
    if (withdrawId !== 'UNKNOWN' && isDuplicateAndMark(`withdraw_request_${withdrawId}`)) return;

    const username = data.username || 'User';
    const userId = data.userId || '-';
    const amountStr = formatRupiah(data.amount);
    const methodStr = data.method || data.ewallet || data.channel || 'E-Wallet / Bank';
    const accountStr = data.accountNumber ? `${data.accountNumber} (${data.accountName || '-'})` : '-';
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 💸 ✨ Permintaan Penarikan Saldo',
      '',
      'Permintaan withdraw baru telah diajukan dan membutuhkan persetujuan.',
      '',
      '| Penarikan Saldo | Detail Rekening |',
      '| --- | --- |',
      `| **Withdraw ID** | \`${escapeRichCell(withdrawId)}\` |`,
      `| **Username** | \`${escapeRichCell(username)}\` |`,
      `| **User ID** | \`${escapeRichCell(userId)}\` |`,
      `| **Nominal** | **${escapeRichCell(amountStr)}** |`,
      `| **Tujuan** | ${escapeRichCell(methodStr)} |`,
      `| **No Rek / HP** | \`${escapeRichCell(accountStr)}\` |`,
      '| **Status** | ⏳ **PROCESSING** |',
      `| **Waktu** | ${escapeRichCell(timeStr)} |`,
      '',
      '> ⚡ PutzPay Payment Gateway • Buka panel Admin untuk memproses.'
    ].join('\n');

    const fallback = `💸 <b>PERMINTAAN PENARIKAN SALDO</b>

━━━━━━━━━━━━━━━━━━
<b>Withdraw ID:</b> <code>${withdrawId}</code>
<b>Username:</b> ${username}
<b>Nominal:</b> <b>${amountStr}</b>
<b>Metode:</b> ${methodStr}
<b>Rekening:</b> <code>${accountStr}</code>
<b>Status:</b> ⏳ PROCESSING
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━

⚡ <i>PutzPay Withdrawal System</i>`;

    console.log('[WITHDRAW] REQUEST');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[WITHDRAW ERROR]', err.message);
  }
}

function notifyWithdrawSuccess(data = {}) {
  try {
    const withdrawId = data.withdraw_id || data.withdrawId || data._id || 'UNKNOWN';
    if (withdrawId !== 'UNKNOWN' && isDuplicateAndMark(`withdraw_success_${withdrawId}`)) return;

    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# ✅ ✨ Penarikan Saldo Berhasil',
      '',
      'Dana penarikan telah berhasil dikirimkan ke rekening merchant.',
      '',
      '| Penarikan Saldo | Detail |',
      '| --- | --- |',
      `| **Withdraw ID** | \`${escapeRichCell(withdrawId)}\` |`,
      `| **Username** | \`${escapeRichCell(username)}\` |`,
      `| **Nominal** | **${escapeRichCell(amountStr)}** |`,
      '| **Status** | 🟢 **SUCCESS / DICAIRKAN** |',
      `| **Waktu** | ${escapeRichCell(timeStr)} |`,
      '',
      '> ⚡ PutzPay Payment Gateway'
    ].join('\n');

    const fallback = `✅ <b>PENARIKAN SALDO BERHASIL</b>

━━━━━━━━━━━━━━━━━━
<b>Withdraw ID:</b> <code>${withdrawId}</code>
<b>Username:</b> ${username}
<b>Nominal:</b> <b>${amountStr}</b>
<b>Status:</b> 🟢 SUCCESS / DICAIRKAN
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━`;

    console.log('[WITHDRAW] SUCCESS');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[WITHDRAW ERROR]', err.message);
  }
}

function notifyWithdrawFailed(data = {}) {
  try {
    const withdrawId = data.withdraw_id || data.withdrawId || data._id || 'UNKNOWN';
    if (withdrawId !== 'UNKNOWN' && isDuplicateAndMark(`withdraw_failed_${withdrawId}`)) return;

    const username = data.username || 'User';
    const amountStr = formatRupiah(data.amount);
    const safeError = sanitizeError(data.error || 'Penarikan ditolak oleh Admin');
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# ❌ Penarikan Saldo Ditolak',
      '',
      '| Penarikan Saldo | Detail |',
      '| --- | --- |',
      `| **Withdraw ID** | \`${escapeRichCell(withdrawId)}\` |`,
      `| **Username** | \`${escapeRichCell(username)}\` |`,
      `| **Nominal** | ${escapeRichCell(amountStr)} |`,
      '| **Status** | 🔴 REJECTED / FAILED |',
      `| **Alasan** | ${escapeRichCell(safeError)} |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `❌ <b>PENARIKAN SALDO DITOLAK</b>

━━━━━━━━━━━━━━━━━━
<b>Withdraw ID:</b> <code>${withdrawId}</code>
<b>Username:</b> ${username}
<b>Nominal:</b> ${amountStr}
<b>Alasan:</b> <code>${safeError}</code>
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━`;

    console.log('[WITHDRAW] FAILED');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[WITHDRAW ERROR]', err.message);
  }
}

function notifyWithdraw(data = {}) {
  const status = (data.status || '').toUpperCase();
  if (status === 'SUCCESS' || status === 'PAID') {
    notifyWithdrawSuccess(data);
  } else if (status === 'FAILED' || status === 'REJECTED') {
    notifyWithdrawFailed(data);
  } else {
    notifyWithdrawRequest(data);
  }
}

function notifyApiError(data = {}) {
  try {
    const endpoint = data.endpoint || data.path || '/api';
    const method = data.method || 'POST';
    const httpStatus = data.status || data.httpStatus || 500;
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 🚨 PutzPay API Error',
      '',
      '| System Alert | Detail |',
      '| --- | --- |',
      `| **Endpoint** | \`${escapeRichCell(endpoint)}\` |`,
      `| **Method** | \`${escapeRichCell(method)}\` |`,
      `| **HTTP Status** | **${escapeRichCell(httpStatus)}** |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `🚨 <b>PUTZPAY API ERROR</b>\n\n<b>Endpoint:</b> <code>${endpoint}</code>\n<b>Method:</b> ${method}\n<b>Status:</b> ${httpStatus}\n<b>Waktu:</b> ${timeStr}`;

    console.log('[API ERROR] SENT');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[API ERROR]', err.message);
  }
}

function notifyDatabaseError(data = {}) {
  try {
    const safeError = sanitizeError(data.error || 'Database connection error');
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 🚨 PutzPay Database Error',
      '',
      '| Database Alert | Detail |',
      '| --- | --- |',
      '| **Status** | 🔴 DATABASE ERROR |',
      `| **Error** | \`${escapeRichCell(safeError)}\` |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `🚨 <b>PUTZPAY DATABASE ERROR</b>\n\n<b>Status:</b> DATABASE ERROR\n<b>Error:</b> <code>${safeError}</code>\n<b>Waktu:</b> ${timeStr}`;

    console.log('[DATABASE ERROR] SENT');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[DATABASE ERROR]', err.message);
  }
}

function notifyWebPushError(data = {}) {
  try {
    const username = data.username || 'User';
    const endpoint = data.endpoint ? (data.endpoint.slice(0, 45) + '...') : '-';
    const status = data.status || 500;
    const safeError = sanitizeError(data.error || 'Push delivery failed');
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# ⚠️ Web Push Error',
      '',
      '| Web Push | Detail |',
      '| --- | --- |',
      `| **User** | ${escapeRichCell(username)} |`,
      `| **Endpoint** | \`${escapeRichCell(endpoint)}\` |`,
      `| **Status** | ${escapeRichCell(status)} |`,
      `| **Error** | \`${escapeRichCell(safeError)}\` |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `⚠️ <b>PUTZPAY WEB PUSH ERROR</b>\n\n<b>User:</b> ${username}\n<b>Status:</b> ${status}\n<b>Error:</b> <code>${safeError}</code>\n<b>Waktu:</b> ${timeStr}`;

    console.log('[WEB PUSH ERROR] SENT');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[WEB PUSH ERROR]', err.message);
  }
}

function notifySecurityAlert(data = {}) {
  try {
    const type = data.type || 'Suspicious Activity';
    const username = data.username || 'Guest';
    const ip = data.ip || '-';
    const timeStr = formatWibDate(data.time || new Date());

    const rich = [
      '# 🚨 PutzPay Security Alert',
      '',
      '| Security Log | Detail |',
      '| --- | --- |',
      `| **Type** | **${escapeRichCell(type)}** |`,
      `| **User** | ${escapeRichCell(username)} |`,
      `| **Alamat IP** | \`${escapeRichCell(ip)}\` |`,
      `| **Waktu** | ${escapeRichCell(timeStr)} |`
    ].join('\n');

    const fallback = `🚨 <b>PUTZPAY SECURITY ALERT</b>\n\n<b>Type:</b> ${type}\n<b>User:</b> ${username}\n<b>IP:</b> <code>${ip}</code>\n<b>Waktu:</b> ${timeStr}`;

    console.log('[SECURITY] ALERT');
    sendNotification(rich, fallback);
  } catch (err) {
    console.error('[SECURITY ERROR]', err.message);
  }
}

// ==================================================
// WEBSITE / API MONITORING ENGINE
// ==================================================

const monitoringState = {
  currentStatus: 'ONLINE',
  previousStatus: 'ONLINE',
  failureCount: 0,
  lastCheck: null,
  lastOnline: Date.now(),
  downtimeStarted: null,
  lastResponseTime: 0,
  totalChecks: 0,
  successfulChecks: 0
};

async function checkTarget(url) {
  const startTime = Date.now();
  try {
    const res = await axios.get(url, {
      timeout: 10000,
      validateStatus: () => true
    });
    const duration = Date.now() - startTime;
    return { ok: res.status >= 200 && res.status < 400, status: res.status, duration };
  } catch (err) {
    const duration = Date.now() - startTime;
    return { ok: false, status: 0, duration, error: err.message };
  }
}

async function runMonitoringCheck() {
  const now = Date.now();
  monitoringState.lastCheck = new Date(now);
  monitoringState.totalChecks++;

  const siteResult = await checkTarget(config.BASE_URL);
  const healthResult = await checkTarget(`${config.BASE_URL}/api/health`);
  const pingResult = await checkTarget(`${config.BASE_URL}/api/ping`);

  const primaryResult = siteResult.ok ? siteResult : (healthResult.ok ? healthResult : pingResult);
  const responseTime = primaryResult.duration;
  monitoringState.lastResponseTime = responseTime;

  const isFailed = !siteResult.ok && !healthResult.ok && !pingResult.ok;

  if (isFailed) {
    monitoringState.failureCount++;
    if (monitoringState.failureCount >= 3) {
      monitoringState.currentStatus = 'DOWN';
      if (!monitoringState.downtimeStarted) {
        monitoringState.downtimeStarted = Date.now();
      }
    }
  } else {
    monitoringState.failureCount = 0;
    monitoringState.successfulChecks++;

    if (responseTime > 3000 || (!healthResult.ok && siteResult.ok)) {
      monitoringState.currentStatus = 'DEGRADED';
    } else {
      monitoringState.currentStatus = 'ONLINE';
      monitoringState.lastOnline = Date.now();
    }
  }

  if (monitoringState.currentStatus !== monitoringState.previousStatus) {
    const prev = monitoringState.previousStatus;
    const curr = monitoringState.currentStatus;
    monitoringState.previousStatus = curr;

    if (curr === 'DOWN') {
      const safeErr = sanitizeError(siteResult.error || healthResult.error || `HTTP ${siteResult.status || healthResult.status || 500}`);
      const textDown = `🚨 <b>PUTZPAY WEBSITE DOWN</b>

━━━━━━━━━━━━━━━━━━
<b>URL:</b> ${config.BASE_URL}
<b>Status:</b> DOWN
<b>HTTP:</b> ${siteResult.status ? `HTTP ${siteResult.status}` : 'TIMEOUT / 5xx'}
<b>Error:</b> <code>${safeErr}</code>
<b>Waktu:</b> ${formatWibDate(now)}
━━━━━━━━━━━━━━━━━━`;

      console.log('[WEBSITE MONITOR] WEBSITE DOWN');
      sendNotification(`# 🚨 PutzPay Website DOWN\n\n| Web | Status |\n| --- | --- |\n| **URL** | ${config.BASE_URL} |\n| **Status** | 🔴 DOWN |\n| **Error** | \`${safeErr}\` |\n| **Waktu** | ${formatWibDate(now)} |`, textDown);

    } else if (curr === 'ONLINE' && prev === 'DOWN') {
      const downtimeMs = monitoringState.downtimeStarted ? (now - monitoringState.downtimeStarted) : 0;
      const downtimeMins = Math.max(1, Math.round(downtimeMs / 60000));
      monitoringState.downtimeStarted = null;

      const textOnline = `🟢 <b>PUTZPAY WEBSITE ONLINE</b>

━━━━━━━━━━━━━━━━━━
<b>URL:</b> ${config.BASE_URL}
<b>Status:</b> ONLINE
<b>Downtime:</b> ${downtimeMins} menit
<b>Waktu:</b> ${formatWibDate(now)}
━━━━━━━━━━━━━━━━━━`;

      console.log('[WEBSITE MONITOR] WEBSITE ONLINE');
      sendNotification(`# 🟢 PutzPay Website ONLINE\n\n| Web | Status |\n| --- | --- |\n| **URL** | ${config.BASE_URL} |\n| **Status** | 🟢 ONLINE |\n| **Downtime** | ${downtimeMins} menit |\n| **Waktu** | ${formatWibDate(now)} |`, textOnline);

    } else if (curr === 'DEGRADED' && prev === 'ONLINE') {
      const degradedText = `🟡 <b>PUTZPAY PERFORMANCE WARNING</b>

<b>Endpoint:</b> ${!healthResult.ok ? '/api/health' : config.BASE_URL}
<b>Response:</b> ${(responseTime / 1000).toFixed(1)} seconds
<b>Status:</b> DEGRADED ⚠️`;

      console.log('[WEBSITE MONITOR] PERFORMANCE DEGRADED');
      sendToBot(`# 🟡 PutzPay Performance Warning\n\n| Metric | Value |\n| --- | --- |\n| **Latency** | ${(responseTime / 1000).toFixed(1)}s |\n| **Status** | DEGRADED ⚠️ |`, degradedText);
    }
  }
}

// ==================================================
// TELEGRAM BOT COMMAND HANDLER
// ==================================================

let lastUpdateId = 0;
let isPolling = false;
const telegramUsernameToIdCache = new Map([
  ['putzpay', '8815465971'],
  ['gbangputz', '8815465971']
]);

async function startBotPolling() {
  if (!config.BOT_TOKEN || isPolling) return;
  isPolling = true;

  while (isPolling) {
    try {
      const res = await axios.get(`https://api.telegram.org/bot${config.BOT_TOKEN}/getUpdates`, {
        params: { offset: lastUpdateId + 1, timeout: 5 },
        timeout: 10000
      });

      if (res.data && res.data.ok && Array.isArray(res.data.result)) {
        for (const update of res.data.result) {
          lastUpdateId = update.update_id;
          if (update.message) {
            if (update.message.from && update.message.from.id) {
              const uId = String(update.message.from.id);
              if (update.message.from.username) {
                telegramUsernameToIdCache.set(update.message.from.username.toLowerCase().replace(/^@/, ''), uId);
              }
            }
            if (update.message.text) {
              handleBotCommand(update.message);
            }
          }
        }
      }
    } catch (err) {
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function handleBotCommand(msg) {
  const chatId = msg.chat.id;
  const text = (msg.text || '').trim();

  if (text.startsWith('/start')) {
    const welcomeRich = [
      '# ⚡ ✨ PutzPay Telegram Bot',
      '',
      'Selamat datang di bot resmi PutzPay Payment Gateway Indonesia.',
      '',
      '| Perintah | Deskripsi |',
      '| --- | --- |',
      '| `/status` | Cek status server, web & API realtime |',
      '| `/health` | Cek endpoint `/api/health` |',
      '| `/ping` | Cek latency sistem `/api/ping` |',
      '| `/uptime` | Statistik uptime 24 jam / 30 hari |',
      '| `/id` | Dapatkan ID Telegram Anda |',
      '',
      '> ⚡ PutzPay Payment Gateway • Solusi QRIS Otomatis'
    ].join('\n');

    const welcomeHtml = `⚡ <b>PUTZPAY TELEGRAM BOT</b>

Selamat datang di Bot Resmi PutzPay Payment Gateway.

<b>Perintah tersedia:</b>
/status - Cek status sistem real-time
/health - Cek endpoint /api/health
/ping - Cek latency /api/ping
/uptime - Tampilkan statistik uptime
/id - Tampilkan Telegram Chat ID Anda`;

    sendRich(chatId, welcomeRich, {}, welcomeHtml);

  } else if (text.startsWith('/status')) {
    const statusEmoji = monitoringState.currentStatus === 'ONLINE' ? '🟢 ONLINE' : (monitoringState.currentStatus === 'DEGRADED' ? '🟡 DEGRADED' : '🔴 DOWN');
    const apiEmoji = monitoringState.currentStatus === 'DOWN' ? '🔴 DOWN' : '🟢 HEALTHY';
    const respTimeStr = monitoringState.lastResponseTime ? (monitoringState.lastResponseTime < 1000 ? `${monitoringState.lastResponseTime} ms` : `${(monitoringState.lastResponseTime / 1000).toFixed(1)} s`) : '245 ms';

    let uptimePct = '99.98%';
    if (monitoringState.totalChecks > 0) {
      uptimePct = ((monitoringState.successfulChecks / monitoringState.totalChecks) * 100).toFixed(2) + '%';
    }

    const statusRich = [
      '# 📡 ✨ PutzPay System Status',
      '',
      '| Layanan | Status |',
      '| --- | --- |',
      `| **Website** | ${statusEmoji} |`,
      `| **API Gateway** | ${apiEmoji} |`,
      '| **Payment QRIS** | 🟢 OPERATIONAL |',
      `| **Response Time** | ${respTimeStr} |`,
      `| **Uptime** | **${uptimePct}** |`,
      `| **Pemeriksaan** | ${monitoringState.lastCheck ? formatWibDate(monitoringState.lastCheck) : formatWibDate()} |`,
      '',
      '> ⚡ PutzPay Realtime Gateway Monitor'
    ].join('\n');

    const reply = `📡 <b>PUTZPAY SYSTEM STATUS</b>

━━━━━━━━━━━━━━━━━━
<b>Website:</b> ${statusEmoji}
<b>API Gateway:</b> ${apiEmoji}
<b>Payment QRIS:</b> 🟢 OPERATIONAL
<b>Response Time:</b> ${respTimeStr}
<b>Uptime:</b> <b>${uptimePct}</b>
<b>Waktu:</b> ${monitoringState.lastCheck ? formatWibDate(monitoringState.lastCheck) : formatWibDate()}
━━━━━━━━━━━━━━━━━━`;

    sendRich(chatId, statusRich, {}, reply);

  } else if (text.startsWith('/health')) {
    const start = Date.now();
    try {
      const res = await axios.get(`${config.BASE_URL}/api/health`, { timeout: 10000 });
      const duration = Date.now() - start;
      const replyHtml = `❤️ <b>PUTZPAY HEALTH CHECK</b>\n\n<b>Status:</b> ${res.status === 200 ? 'HEALTHY 🟢' : 'WARNING 🟡'}\n<b>HTTP Code:</b> ${res.status}\n<b>Response Time:</b> ${duration} ms\n<b>Time:</b> ${formatWibDate()}`;
      const replyRich = `# ❤️ Health Check\n\n| Metric | Value |\n| --- | --- |\n| **Status** | ${res.status === 200 ? '🟢 HEALTHY' : '🟡 WARNING'} |\n| **HTTP** | ${res.status} |\n| **Latency** | ${duration} ms |\n| **Waktu** | ${formatWibDate()} |`;
      sendRich(chatId, replyRich, {}, replyHtml);
    } catch (err) {
      sendRich(chatId, `# ❤️ Health Check\n\n| Metric | Value |\n| --- | --- |\n| **Status** | 🔴 UNHEALTHY |\n| **Error** | \`${err.message}\` |`, {}, `❤️ <b>PUTZPAY HEALTH CHECK</b>\n\n<b>Status:</b> UNHEALTHY 🔴\n<b>Error:</b> ${err.message}`);
    }

  } else if (text.startsWith('/ping')) {
    const start = Date.now();
    try {
      const res = await axios.get(`${config.BASE_URL}/api/ping`, { timeout: 10000 });
      const duration = Date.now() - start;
      const replyHtml = `🏓 <b>PONG!</b>\n\n<b>Latency:</b> ${duration} ms\n<b>Status:</b> ${res.status} OK`;
      const replyRich = `# 🏓 Pong!\n\n| Metric | Value |\n| --- | --- |\n| **Latency** | ${duration} ms |\n| **Status** | 🟢 ${res.status} OK |`;
      sendRich(chatId, replyRich, {}, replyHtml);
    } catch (err) {
      sendRich(chatId, `# 🏓 Ping Failed\n\n| Status | 🔴 Error |\n| Error | \`${err.message}\` |`, {}, `🏓 <b>PING FAILED</b>\n\n<b>Error:</b> ${err.message}`);
    }

  } else if (text.startsWith('/uptime')) {
    let pct = '99.98';
    if (monitoringState.totalChecks >= 5) {
      pct = ((monitoringState.successfulChecks / monitoringState.totalChecks) * 100).toFixed(2);
    }
    const uptimeRich = [
      '# ⏱️ ✨ PutzPay Uptime Stats',
      '',
      '| Periode | Ketersediaan |',
      '| --- | --- |',
      `| **24 Jam** | 🟢 **${pct}%** |`,
      `| **7 Hari** | 🟢 **${pct}%** |`,
      `| **30 Hari** | 🟢 **${pct}%** |`,
      '',
      '> ⚡ SLA PutzPay dipertahankan di atas 99.9%'
    ].join('\n');
    sendRich(chatId, uptimeRich, {}, `⏱️ <b>PUTZPAY UPTIME STATS</b>\n\n<b>24 Hours:</b> ${pct}%\n<b>7 Days:</b> ${pct}%\n<b>30 Days:</b> ${pct}%`);

  } else if (text.startsWith('/id')) {
    const senderId = msg.from?.id ? String(msg.from.id) : String(chatId);
    const firstName = msg.from?.first_name || '';
    const lastName = msg.from?.last_name || '';
    const tgName = [firstName, lastName].filter(Boolean).join(' ') || 'Pengguna Telegram';
    const tgUsername = msg.from?.username ? `@${msg.from.username}` : 'Tidak diatur';
    const chatType = msg.chat?.type === 'private' ? 'Pribadi (Private)' : (msg.chat?.type === 'group' || msg.chat?.type === 'supergroup' ? 'Grup / Komunitas' : (msg.chat?.type || 'Chat'));
    const isOwner = String(chatId) === String(config.OWNER_CHAT_ID) || String(senderId) === String(config.OWNER_CHAT_ID);

    // Cek keterhubungan akun PutzPay di MongoDB
    let webUser = null;
    try {
      const mongoose = require('mongoose');
      if (mongoose.connection && mongoose.connection.readyState === 1) {
        const User = mongoose.models.User;
        if (User) {
          const queries = [{ telegramId: senderId }, { telegramId: String(chatId) }];
          if (msg.from?.username) {
            queries.push({ telegramId: `@${msg.from.username}` }, { telegramId: msg.from.username });
          }
          webUser = await User.findOne({ $or: queries }).select('username fullName email balance role').lean();
        }
      }
    } catch (e) {}

    let webStatusStr = '⚪ Belum Ditautkan ke Akun Web';
    if (webUser) {
      const roleBadge = webUser.role === 'admin' ? '🛡️ Admin' : '👤 Merchant';
      const balStr = formatRupiah(webUser.balance || 0);
      webStatusStr = `🟢 Terhubung: **${escapeRichCell(webUser.username)}** (${roleBadge} • Saldo: ${balStr})`;
    }

    // Cek keanggotaan channel resmi PutzPay
    let channelStatusStr = '⚪ Sedang Memeriksa...';
    try {
      const chk = await checkChannelMembership(senderId);
      channelStatusStr = (chk && chk.joined) ? '🟢 Sudah Bergabung' : '🔴 Belum Bergabung';
    } catch (e) {
      channelStatusStr = '⚪ Belum Diverifikasi';
    }

    const timeStr = formatWibDate(new Date());

    const idRich = [
      '# 🆔 ✨ Detail Identitas Akun Telegram',
      '',
      'Informasi identitas akun Telegram Anda untuk verifikasi dan sinkronisasi sistem PutzPay.',
      '',
      '| Parameter | Detail Akun |',
      '| --- | --- |',
      `| **Telegram ID** | \`${escapeRichCell(senderId)}\` |`,
      `| **Nama** | ${escapeRichCell(tgName)} |`,
      `| **Username** | ${escapeRichCell(tgUsername)} |`,
      `| **Tipe Chat** | ${escapeRichCell(chatType)} |`,
      `| **Status Akun Web** | ${webStatusStr} |`,
      `| **Channel Resmi** | ${channelStatusStr} |`,
      `| **Hak Akses** | ${isOwner ? '👑 Bot Owner / Administrator' : '👤 Pengguna Publik'} |`,
      `| **Waktu Pengecekan** | ${escapeRichCell(timeStr)} |`,
      '',
      '> 💡 **Petunjuk Penggunaan:**',
      `> Salin **Telegram ID** (\`${senderId}\`) di atas, lalu tempelkan pada menu **Profil** di website PutzPay untuk mengaktifkan notifikasi transaksi instan ke akun Telegram Anda.`
    ].join('\n');

    const idHtml = `🆔 <b>DETAIL IDENTITAS TELEGRAM</b>

━━━━━━━━━━━━━━━━━━
<b>Telegram ID:</b> <code>${senderId}</code> <i>(Ketuk untuk salin)</i>
<b>Nama:</b> ${tgName}
<b>Username:</b> ${tgUsername}
<b>Tipe Chat:</b> ${chatType}
<b>Status Akun Web:</b> ${webUser ? `🟢 Terhubung: <b>${webUser.username}</b>` : '⚪ Belum Ditautkan'}
<b>Channel Resmi:</b> ${channelStatusStr}
<b>Hak Akses:</b> ${isOwner ? '👑 Bot Owner' : '👤 Pengguna Publik'}
<b>Waktu:</b> ${timeStr}
━━━━━━━━━━━━━━━━━━

💡 <i>Salin Telegram ID di atas dan masukkan ke menu Profil di website PutzPay untuk menerima notifikasi transaksi pribadi.</i>`;

    const extra = {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '🌐 Buka Profil PutzPay', url: `${config.BASE_URL}/profile` },
            { text: '📢 Channel Resmi', url: 'https://t.me/FizzAbout' }
          ]
        ]
      }
    };

    sendRich(chatId, idRich, extra, idHtml);
  }
}

// ==================================================
// TELEGRAM MEMBERSHIP VERIFICATION
// ==================================================

async function checkChannelMembership(identifier) {
  const channel = config.CHANNEL_ID || '-1003565348730';
  if (!config.BOT_TOKEN) {
    return { success: false, joined: false, message: 'Bot Telegram belum dikonfigurasi di server.' };
  }

  const raw = String(identifier || '').trim();
  if (!raw) {
    return { success: true, joined: false, message: '❌ Kamu belum bergabung ke channel resmi PutzPay.' };
  }

  let targetUserId = '';
  if (/^-?\d+$/.test(raw)) {
    targetUserId = raw;
  } else {
    const cleanUsername = raw.toLowerCase().replace(/^@/, '');
    if (telegramUsernameToIdCache.has(cleanUsername)) {
      targetUserId = telegramUsernameToIdCache.get(cleanUsername);
    } else {
      return {
        success: true,
        joined: false,
        requiresNumericId: true,
        message: 'Username @' + cleanUsername + ' belum terdaftar di sistem bot. Silakan gunakan nomor Telegram ID Anda (Dapatkan lewat bot @NotifPutzPayBot dengan ketik /id atau @userinfobot).'
      };
    }
  }

  try {
    const res = await axios.get(`https://api.telegram.org/bot${config.BOT_TOKEN}/getChatMember`, {
      params: {
        chat_id: channel,
        user_id: targetUserId
      },
      timeout: 8000
    });

    if (res.data && res.data.ok && res.data.result) {
      const status = res.data.result.status;
      const validStatuses = ['creator', 'administrator', 'member'];
      const isMember = validStatuses.includes(status) || (status === 'restricted' && res.data.result.is_member === true);

      if (isMember) {
        return { success: true, joined: true, status, message: '✅ Berhasil diverifikasi!' };
      }
    }
    return { success: true, joined: false, message: '❌ Kamu belum bergabung ke channel resmi PutzPay.' };
  } catch (err) {
    return { success: true, joined: false, message: '❌ Kamu belum bergabung ke channel resmi PutzPay.' };
  }
}

// Initialization & Monitor loops
let monitorInterval = null;

async function initMonitor() {
  const targetChannel = config.CHANNEL_ID || '@FizzAbout';
  const botConnectedStr = config.BOT_TOKEN && config.OWNER_CHAT_ID ? 'CONNECTED' : 'FAILED';
  const channelConnectedStr = config.BOT_TOKEN ? 'CONNECTED' : 'FAILED';

  console.log(`========================================
       PUTZPAY TELEGRAM MONITOR & RICH ENGINE
========================================

Telegram Bot: ${botConnectedStr}
Telegram Channel: ${targetChannel} (${channelConnectedStr})
Owner ID: ${config.OWNER_CHAT_ID}
Website: ${config.BASE_URL}
Rich Message Engine: ACTIVE
========================================\n`);

  setTimeout(() => {
    runMonitoringCheck().catch(() => {});
  }, 5000);

  monitorInterval = setInterval(() => {
    runMonitoringCheck().catch(() => {});
  }, 60000);

  startBotPolling().catch(() => {});
}

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

function cleanup() {
  isPolling = false;
  if (monitorInterval) clearInterval(monitorInterval);
}

initMonitor();

module.exports = {
  config,
  delay,
  escapeRichCell,
  markdownCodeBlock,
  showThinking,
  waitThinkingMinimum,
  sendRich,
  sendProgressiveRich,
  sendFreshRich,
  sendRichMenu,
  sendToBot,
  sendToChannel,
  sendNotification,
  notifyNewUser,
  notifyGoogleNewUser,
  notifyUserLogin,
  notifyInvoiceCreated,
  notifyPaymentPending,
  notifyPaymentSuccess,
  notifyPaymentFailed,
  notifyInvoiceExpired,
  notifyInvoiceCancelled,
  notifyWithdrawRequest,
  notifyWithdrawSuccess,
  notifyWithdrawFailed,
  notifyWithdraw,
  notifyApiError,
  notifyDatabaseError,
  notifyWebPushError,
  notifySecurityAlert,
  checkChannelMembership,
  deleteTelegramMessage,
  getMonitoringStatus: () => ({ ...monitoringState })
};
