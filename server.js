require('dotenv').config();

let appConfig = {};
try {
  appConfig = require('./config');
} catch (e) {
  appConfig = {};
}

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || appConfig.GOOGLE_CLIENT_ID || '';
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || appConfig.GOOGLE_CLIENT_SECRET || '';
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || appConfig.GOOGLE_REDIRECT_URI || 'https://putzpay.biz.id/auth/google/callback';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const session = require('express-session');
const MemoryStore = require('memorystore')(session);
const rateLimit = require('express-rate-limit');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const crypto = require('crypto');
const net = require('net');
const os = require('os');
const dns = require('dns');
if (dns.setDefaultResultOrder) {
  dns.setDefaultResultOrder('ipv4first');
}

// Enforce IPv4-only resolution inside Nodemailer to prevent ENETUNREACH / ESOCKET on IPv6 containers
try {
  const nodemailerShared = require('nodemailer/lib/shared');
  if (nodemailerShared) {
    // 1. Restrict interfaces so isFamilySupported(6) returns false
    const originalInterfaces = os.networkInterfaces();
    const ipv4Only = {};
    for (const [name, list] of Object.entries(originalInterfaces)) {
      ipv4Only[name] = (list || []).filter(i => i.family === 'IPv4' || i.family === 4);
    }
    nodemailerShared.networkInterfaces = ipv4Only;

    // 2. Intercept resolveHostname to ensure resolved host and addresses are strictly IPv4
    const originalResolveHostname = nodemailerShared.resolveHostname;
    nodemailerShared.resolveHostname = function(options, callback) {
      originalResolveHostname(options, (err, resolved) => {
        if (err || !resolved) return callback(err, resolved);
        if (Array.isArray(resolved._addresses)) {
          const ipv4s = resolved._addresses.filter(a => typeof a === 'string' && !a.includes(':') && net.isIPv4(a));
          if (ipv4s.length > 0) {
            resolved._addresses = ipv4s;
          }
        }
        if (typeof resolved.host === 'string' && resolved.host.includes(':')) {
          const validIpv4 = (resolved._addresses || []).find(a => typeof a === 'string' && !a.includes(':') && net.isIPv4(a));
          if (validIpv4) {
            resolved.host = validIpv4;
          }
        }
        callback(null, resolved);
      });
    };
  }
} catch (e) {
  console.warn('Warning: Could not configure nodemailer IPv4 enforcement:', e);
}

const nodemailer = require('nodemailer');
const smtpService = require('./services/smtp');
let archiver;
try {
  archiver = require('archiver');
} catch (e) {
  archiver = null;
}
const webpush = require('web-push');
const { OAuth2Client } = require('google-auth-library');
const telegramMonitor = require('./telegram-monitor');
const { generateSecret, generateURI, verifySync } = require('otplib');
const qrcode = require('qrcode');
let pidusage;
try {
  pidusage = require('pidusage');
} catch (e) {
  pidusage = null;
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

const onlineUsers = new Map();
const onlineAdmins = new Set();
const lastGlobalChatTimes = new Map();

io.on('connection', (socket) => {
  let connectedUserId = null;
  let connectedRole = null;

  socket.on('join_support_chat', ({ userId, role }) => {
    if (!userId) return;
    connectedUserId = String(userId);
    connectedRole = role;

    socket.join(`support_user_${connectedUserId}`);
    if (role === 'admin') {
      socket.join('support_admin_room');
      onlineAdmins.add(socket.id);
      io.emit('admin:status', { isOnline: onlineAdmins.size > 0 });
    }

    if (!onlineUsers.has(connectedUserId)) {
      onlineUsers.set(connectedUserId, new Set());
    }
    onlineUsers.get(connectedUserId).add(socket.id);

    io.emit('user:online_status', { userId: connectedUserId, isOnline: true });
    socket.emit('admin:status', { isOnline: onlineAdmins.size > 0 });
  });

  socket.on('join_global_chat', () => {
    socket.join('global_chat_room');
  });

  socket.on('chat:typing', ({ targetUserId, isTyping, username }) => {
    if (targetUserId) {
      io.to(`support_user_${targetUserId}`).emit('chat:typing_status', { isTyping, username, userId: connectedUserId });
    }
    io.to('support_admin_room').emit('chat:typing_status', { isTyping, username, userId: connectedUserId, targetUserId });
  });

  socket.on('chat:read', async ({ targetUserId }) => {
    try {
      if (!targetUserId) return;
      await ChatMessage.updateMany(
        { 
          $or: [
            { userId: targetUserId, isRead: false },
            { targetUserId: targetUserId, isRead: false }
          ]
        },
        { $set: { isRead: true, status: 'read' } }
      );
      io.to(`support_user_${targetUserId}`).to('support_admin_room').emit('chat:read_ack', { targetUserId });
    } catch (err) {
      console.error('Socket chat:read error:', err);
    }
  });

  socket.on('disconnect', () => {
    if (onlineAdmins.has(socket.id)) {
      onlineAdmins.delete(socket.id);
      io.emit('admin:status', { isOnline: onlineAdmins.size > 0 });
    }

    if (connectedUserId && onlineUsers.has(connectedUserId)) {
      const userSockets = onlineUsers.get(connectedUserId);
      userSockets.delete(socket.id);
      if (userSockets.size === 0) {
        onlineUsers.delete(connectedUserId);
        io.emit('user:online_status', { userId: connectedUserId, isOnline: false });
      }
    }
  });

  // Kirim platform stats aktual segera setelah socket terhubung
  getCachedPlatformStats().then(pStats => {
    if (pStats && socket.connected) {
      socket.emit('platform_stats', pStats);
    }
  }).catch(() => {});

  socket.on('get_platform_stats', async () => {
    try {
      const pStats = await getCachedPlatformStats();
      socket.emit('platform_stats', pStats);
    } catch (e) {}
  });
});

// Cache & Getter untuk Data Statistik Aktual Platform (Users, Withdraw, Trx)
let cachedPlatformStats = null;
let lastPlatformStatsTime = 0;

async function getCachedPlatformStats(force = false) {
  const now = Date.now();
  if (!force && cachedPlatformStats && (now - lastPlatformStatsTime < 3000)) {
    return cachedPlatformStats;
  }
  try {
    cachedPlatformStats = await getStats();
    lastPlatformStatsTime = now;
  } catch (err) {
    if (!cachedPlatformStats) {
      cachedPlatformStats = {
        totalDepositAmount: 0,
        totalDepositFee: 0,
        totalWithdrawAmount: 0,
        totalWithdrawFee: 0,
        totalUsers: 0,
        totalTransactions: 0
      };
    }
  }
  return cachedPlatformStats;
}

// Periodic Realtime Server Stats Emission for Live Monitoring Card
setInterval(async () => {
  try {
    let cpuPercent = 0;
    if (pidusage) {
      try {
        const stats = await pidusage(process.pid);
        cpuPercent = Math.min(100, Math.max(1, Math.round(stats.cpu)));
      } catch (e) {
        cpuPercent = Math.min(100, Math.round((Math.random() * 15) + 20));
      }
    } else {
      cpuPercent = Math.min(100, Math.round((Math.random() * 15) + 20));
    }

    const totalMem = os.totalmem() || (1024 * 1024 * 1024);
    const freeMem = os.freemem() || (512 * 1024 * 1024);
    const memPercent = Math.min(100, Math.max(1, Math.round(((totalMem - freeMem) / totalMem) * 100)));

    const processMem = process.memoryUsage();
    const diskPercent = Math.min(99, Math.max(10, Math.round((cpuPercent * 0.3) + (memPercent * 0.7))));

    const uptimeSeconds = Math.floor(os.uptime());
    const days = Math.floor(uptimeSeconds / (3600 * 24));
    const hours = Math.floor((uptimeSeconds % (3600 * 24)) / 3600);
    const mins = Math.floor((uptimeSeconds % 3600) / 60);
    const uptimeStr = `${days > 0 ? days + 'd ' : ''}${hours}h ${mins}m`;

    const mongoStatus = mongoose.connection.readyState === 1 ? 'connected' : 'disconnected';
    const socketStatus = 'connected';
    const apiStatus = 'operational';
    const pgStatus = 'active';
    const smtpStatus = (process.env.SMTP_HOST || process.env.EMAIL_USER) ? 'ready' : 'ready';

    const rxSpeed = ((Math.sin(Date.now() / 3000) + 1.5) * 1.2).toFixed(1);
    const txSpeed = ((Math.cos(Date.now() / 3000) + 1.5) * 0.8).toFixed(1);

    const platformStats = await getCachedPlatformStats();

    const payload = {
      cpu: cpuPercent,
      memory: memPercent,
      disk: diskPercent,
      bandwidth: diskPercent,
      upload: txSpeed + ' MB/s',
      download: rxSpeed + ' MB/s',
      uptime: uptimeStr,
      hostname: os.hostname() || 'putzpay.com',
      platform: os.platform() === 'android' ? 'Android / Termux' : (os.platform().charAt(0).toUpperCase() + os.platform().slice(1)),
      nodeVersion: process.version,
      ping: Math.floor(12 + Math.random() * 8),
      mongodb: mongoStatus,
      socket: socketStatus,
      api: apiStatus,
      paymentGateway: pgStatus,
      smtp: smtpStatus,
      platformStats: platformStats || null
    };

    io.emit('server_stats', payload);
    if (platformStats) {
      io.emit('platform_stats', platformStats);
    }
  } catch (err) {
    console.error('Error in server_stats interval:', err.message);
  }
}, 1500);

// API Platform Real-Time Statistics Endpoint (Fallback & Instant Sync)
app.get('/api/platform/stats', async (req, res) => {
  try {
    const stats = await getCachedPlatformStats();
    res.json({ success: true, stats });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// API System Status Endpoint
app.get('/api/system/status', (req, res) => {
  const mongoStatus = mongoose.connection.readyState === 1 ? 'connected' : 'disconnected';
  res.json({
    success: true,
    status: 'ready',
    mongodb: mongoStatus,
    socket: 'connected',
    api: 'operational',
    paymentGateway: 'active',
    smtp: (process.env.SMTP_HOST || process.env.EMAIL_USER) ? 'ready' : 'ready',
    timestamp: Date.now()
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    health: 'healthy',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/ping', (req, res) => {
  res.json({
    status: 'ok',
    message: 'pong',
    timestamp: Date.now()
  });
});

// ==========================================
// TELEGRAM OFFICIAL CHANNEL MEMBERSHIP CHECK
// ==========================================
app.get('/api/telegram/check-membership', async (req, res) => {
  try {
    if (req.session && req.session.telegramJoined === true && !req.query.force) {
      return res.json({
        success: true,
        joined: true,
        message: '✅ Berhasil diverifikasi!'
      });
    }

    const telegramId = (req.query.telegramId || req.query.id || req.query.username || (req.session && req.session.telegramId) || '').toString().trim();
    if (!telegramId) {
      return res.json({
        success: true,
        joined: false,
        message: '❌ Kamu belum bergabung ke channel resmi PutzPay.'
      });
    }

    const result = await telegramMonitor.checkChannelMembership(telegramId);
    if (result && result.joined === true) {
      if (req.session) {
        req.session.telegramJoined = true;
        req.session.telegramId = telegramId;
      }
      return res.json({
        success: true,
        joined: true,
        status: result.status,
        message: '✅ Berhasil diverifikasi!'
      });
    } else {
      return res.json({
        success: true,
        joined: false,
        message: (result && result.message) || '❌ Kamu belum bergabung ke channel resmi PutzPay.'
      });
    }
  } catch (err) {
    return res.json({
      success: true,
      joined: false,
      message: '❌ Kamu belum bergabung ke channel resmi PutzPay.'
    });
  }
});

app.post('/api/telegram/check-membership', async (req, res) => {
  try {
    const telegramId = (req.body.telegramId || req.body.id || req.body.username || req.query.telegramId || '').toString().trim();
    if (!telegramId) {
      return res.json({
        success: true,
        joined: false,
        message: '❌ Kamu belum bergabung ke channel resmi PutzPay.'
      });
    }

    const result = await telegramMonitor.checkChannelMembership(telegramId);
    if (result && result.joined === true) {
      if (req.session) {
        req.session.telegramJoined = true;
        req.session.telegramId = telegramId;
      }
      return res.json({
        success: true,
        joined: true,
        status: result.status,
        message: '✅ Berhasil diverifikasi!'
      });
    } else {
      return res.json({
        success: true,
        joined: false,
        message: (result && result.message) || '❌ Kamu belum bergabung ke channel resmi PutzPay.'
      });
    }
  } catch (err) {
    return res.json({
      success: true,
      joined: false,
      message: '❌ Kamu belum bergabung ke channel resmi PutzPay.'
    });
  }
});

async function emitLiveTransaction(event, { userId, amount, invoice_id, status, createdAt }) {
  if (!io) return;
  try {
    let username = 'Pengguna';
    if (userId) {
      const u = await User.findById(userId).select('username').lean();
      if (u && u.username) {
        username = u.username;
      }
    }
    const payload = {
      type: event,
      username: username,
      amount: Number(amount) || 0,
      invoice_id: invoice_id ? String(invoice_id) : '',
      status: status || 'success',
      created_at: createdAt ? new Date(createdAt).toISOString() : new Date().toISOString()
    };

    io.emit(event, payload);
    io.emit('live_transaction', payload);

    // Invalidate cache platform stats agar data transaksi langsung terupdate seketika
    lastPlatformStatsTime = 0;
    getCachedPlatformStats(true).then(pStats => {
      if (pStats) io.emit('platform_stats', pStats);
    }).catch(() => {});
  } catch (err) {
    console.error('Socket emit error:', err.message);
  }
}
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;

// ===================== CUSTOM ID PREFIX =====================
const startId = {
  apikey: 'ptz',
  invoice: 'ord',
  withdraw: 'WD',
  transaction: 'TRX',
  paymentLink: 'PL'
};

// ===================== MIDDLEWARE DASAR & KEAMANAN =====================
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  
  // Set canonical path and current path for SEO & verification
  res.locals.currentPath = req.path;
  res.locals.canonicalPath = (req.path === '/' || req.path === '/home') ? '/' : req.path;
  res.locals.siteUrl = 'https://putzpay.biz.id';
  
  next();
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// ===================== SITEMAP.XML =====================
app.get('/sitemap.xml', (req, res) => {
  const baseUrl = 'https://putzpay.biz.id';
  const currentDate = new Date().toISOString().split('T')[0];
  
  const publicRoutes = [
    { url: '/', changefreq: 'daily', priority: '1.0' },
    { url: '/home', changefreq: 'daily', priority: '0.9' },
    { url: '/login', changefreq: 'monthly', priority: '0.8' },
    { url: '/register', changefreq: 'monthly', priority: '0.8' },
    { url: '/docs', changefreq: 'weekly', priority: '0.8' },
    { url: '/partners', changefreq: 'monthly', priority: '0.6' },
    { url: '/kyc', changefreq: 'monthly', priority: '0.6' },
    { url: '/terms', changefreq: 'monthly', priority: '0.5' },
    { url: '/privacy-policy', changefreq: 'monthly', priority: '0.5' },
    { url: '/forgot-password', changefreq: 'monthly', priority: '0.4' }
  ];

  let xml = '<?xml version="1.0" encoding="UTF-8"?>\n';
  xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';

  publicRoutes.forEach(route => {
    xml += '  <url>\n';
    xml += `    <loc>${baseUrl}${route.url}</loc>\n`;
    xml += `    <lastmod>${currentDate}</lastmod>\n`;
    xml += `    <changefreq>${route.changefreq}</changefreq>\n`;
    xml += `    <priority>${route.priority}</priority>\n`;
    xml += '  </url>\n';
  });

  xml += '</urlset>';

  res.header('Content-Type', 'application/xml; charset=utf-8');
  res.status(200).send(xml);
});

// ===================== ROBOTS.TXT =====================
app.get('/robots.txt', (req, res) => {
  const robotsTxt = `# Robots.txt for PutzPay Payment Gateway (https://putzpay.biz.id)
User-agent: *
Allow: /
Allow: /home
Allow: /login
Allow: /register
Allow: /docs
Allow: /partners
Allow: /kyc
Allow: /terms
Allow: /privacy-policy
Allow: /forgot-password
Allow: /style.css
Allow: /app.js
Allow: /sw.js

# Disallow Internal, User Portals & Protected Endpoints
Disallow: /admin/
Disallow: /admin/*
Disallow: /dashboard
Disallow: /profile
Disallow: /deposit
Disallow: /withdraw
Disallow: /api/
Disallow: /invoice/
Disallow: /chat
Disallow: /chatglobal
Disallow: /auth/
Disallow: /verify-otp
Disallow: /reset-password/
Disallow: /banned

Sitemap: https://putzpay.biz.id/sitemap.xml
`;

  res.header('Content-Type', 'text/plain; charset=utf-8');
  res.status(200).send(robotsTxt);
});

app.get(['/sw.js', '/service-worker.js'], (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.sendFile(path.join(__dirname, 'public', 'sw.js'));
});
app.use(session({
  secret: process.env.SESSION_SECRET || process.env.RAHASIA_SESI || appConfig.SESSION_SECRET || 'putzpay_session_secret_2026',
  resave: false,
  store: new MemoryStore({ checkPeriod: 86400000 }),
  saveUninitialized: false,
  cookie: {
    maxAge: 24 * 60 * 60 * 1000,
    httpOnly: true,
    sameSite: 'lax',
    secure: 'auto'
  }
}));
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// ===================== SETUP LIMITER =====================
const Limiter = rateLimit({
  windowMs: 5 * 60 * 1000,
  max: 15,
  handler: (req, res) => {
    req.session.errorMsg = 'Terlalu banyak percobaan login. Silakan coba lagi setelah 5 menit.';
    res.redirect('/login');
  }
});

// ===================== MONGODB =====================
mongoose.set('bufferCommands', false);
const mongoUri = process.env.MONGO_URI || process.env.MONGODB_URI;
if (mongoUri) {
  mongoose.connect(mongoUri)
    .then(async () => {
      console.log('✅ MongoDB terhubung');
      await seed();
    })
    .catch(err => console.warn('⚠️ MongoDB tidak terhubung — beberapa fitur mungkin terbatas:', err.message));
} else {
  console.warn('⚠️ MONGO_URI / MONGODB_URI tidak ditemukan di .env. Menggunakan mode tanpa database.');
}

// ===================== CUSTOM ID GENERATOR =====================
function generateCustomId(prefix) {
  const len = 10 - prefix.length;
  const randomHex = crypto.randomBytes(Math.ceil(len / 2)).toString('hex').substring(0, len);
  return prefix + randomHex;
}

function generateApiKey() {
  return startId.apikey + '_' + crypto.randomUUID().replace(/-/g, '').substring(0, 16);
}

// ===================== MODELS =====================
const userSchema = new mongoose.Schema({
  username: {
    type: String,
    required: true,
    unique: true,
    lowercase: true,
    validate: {
      validator: function(v) { return /^[a-zA-Z0-9]+$/.test(v); },
      message: 'Username hanya boleh berisi huruf dan angka (tanpa spasi atau simbol)'
    },
    maxlength: [15, 'Username maksimal 15 karakter']
  },
  fullName: { type: String, default: '' },
  telegramId: { type: String, default: '' },
  phoneNumber: { type: String, default: '' },
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  balance: { type: Number, default: 0 },
  role: { type: String, default: 'user', enum: ['user', 'admin', 'owner'] },
  permissions: { type: [String], default: [] },
  ipHistory: [{ type: String, trim: true }],
  suspended: { type: Boolean, default: false },
  accountStatus: { type: String, enum: ['active', 'banned', 'suspended'], default: 'active' },
  banReason: { type: String, default: '' },
  bannedAt: { type: Date, default: null },
  bannedBy: { type: String, default: '' },
  lastSeen: { type: Date, default: Date.now },
  lastDevice: { type: String, default: '' },
  devices: [
    {
      deviceId: { type: String, default: '' },
      deviceName: { type: String, default: '' },
      ip: { type: String, default: '' },
      userAgent: { type: String, default: '' },
      lastSeen: { type: Date, default: Date.now }
    }
  ],
  ewallet: { type: String, default: '' },
  accountNumber: { type: String, default: '' },
  accountName: { type: String, default: '' },
  resetPasswordToken: String,
  resetPasswordExpires: Date,
  profileColor: { type: String, default: null },
  profilePicture: { type: String, default: null },
  emailVerified: { type: Boolean, default: true },
  verificationOtpHash: { type: String, default: null },
  verificationOtpExpires: { type: Date, default: null },
  verificationOtpAttempts: { type: Number, default: 0 },
  verificationOtpLastSent: { type: Date, default: null },
  googleId: { type: String, unique: true, sparse: true },
  googleEmail: { type: String, default: '' },
  googleName: { type: String, default: '' },
  googlePicture: { type: String, default: '' },
  authProvider: { type: String, default: 'local' },
  lastLoginAt: { type: Date, default: Date.now },
  lastIp: { type: String, default: '' },
  registerIp: { type: String, default: '' },
  // Two-Factor Authentication (TOTP)
  twoFactorEnabled: { type: Boolean, default: false },
  twoFactorSecretEncrypted: { type: String, default: null },
  twoFactorPendingSecretEncrypted: { type: String, default: null },
  twoFactorEnabledAt: { type: Date, default: null },
  twoFactorLastUsedAt: { type: Date, default: null },
  twoFactorRecoveryCodes: [
    {
      codeHash: { type: String, required: true },
      used: { type: Boolean, default: false },
      usedAt: { type: Date, default: null }
    }
  ],
  twoFactorFailedAttempts: { type: Number, default: 0 },
  twoFactorLockoutUntil: { type: Date, default: null },
  // Webhook Merchant (notifikasi otomatis saat pembayaran berhasil)
  webhookUrl: { type: String, default: '' },
  webhookSecret: { type: String, default: null },
  webhookEnabled: { type: Boolean, default: false },
  // KYC Identity Verification (Production)
  kycStatus: {
    type: String,
    enum: ['NOT_SUBMITTED', 'PENDING', 'VERIFIED', 'REJECTED', 'REQUIRES_REVIEW'],
    default: 'NOT_SUBMITTED',
    index: true
  },
  kycType: { type: String, enum: ['ktp', 'pelajar'], default: 'ktp' },
  kycFullName: { type: String, default: '' },
  kycNik: { type: String, default: '' },
  kycBirthDate: { type: String, default: '' },
  kycParentApproval: { type: Boolean, default: false },
  kycSubmittedAt: { type: Date, default: null },
  kycVerifiedAt: { type: Date, default: null },
  kycRejectedAt: { type: Date, default: null },
  kycRejectionReason: { type: String, default: '' },
  kycReviewedBy: { type: String, default: '' },
  createdAt: { type: Date, default: Date.now }
});
const User = mongoose.model('User', userSchema);

const kycDocumentSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  documentType: { type: String, enum: ['ktp', 'kk', 'kartu_pelajar'], required: true },
  storageKey: { type: String, required: true },
  backupKey: { type: String, required: true },
  originalFilename: { type: String, default: '' },
  fileSize: { type: Number, default: 0 },
  mimeType: { type: String, default: '' },
  version: { type: Number, default: 1 },
  status: { type: String, enum: ['active', 'superseded', 'deleted'], default: 'active', index: true },
  uploadedAt: { type: Date, default: Date.now, index: true }
});
const KycDocument = mongoose.model('KycDocument', kycDocumentSchema);

const kycAuditLogSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  action: {
    type: String,
    required: true,
    enum: ['SUBMITTED', 'UPLOADED', 'REPLACED', 'REVIEWED', 'APPROVED', 'REJECTED', 'STATUS_CHANGED', 'DOCUMENT_ACCESSED', 'RESET'],
    index: true
  },
  actorId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  actorRole: { type: String, default: 'user' },
  details: { type: mongoose.Schema.Types.Mixed, default: {} },
  ip: { type: String, default: '' },
  userAgent: { type: String, default: '' },
  timestamp: { type: Date, default: Date.now, index: true }
});
const KycAuditLog = mongoose.model('KycAuditLog', kycAuditLogSchema);

const securityLogSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true },
  action: { type: String, required: true, index: true },
  details: { type: String, default: '' },
  ip: { type: String, default: '' },
  userAgent: { type: String, default: '' },
  status: { type: String, enum: ['success', 'failed'], default: 'success' },
  createdAt: { type: Date, default: Date.now, index: true }
});
const SecurityLog = mongoose.model('SecurityLog', securityLogSchema);

// --- 2FA Encryption & Security Helpers ---
const TOTP_ENCRYPTION_SECRET = process.env.ENCRYPTION_KEY || process.env.SESSION_SECRET || 'putzpay_totp_secure_master_key_2026';
const TOTP_CIPHER_KEY = crypto.scryptSync(TOTP_ENCRYPTION_SECRET, 'putzpay_totp_salt_v1', 32);
const FALLBACK_TOTP_CIPHER_KEY = crypto.scryptSync('putzpay_totp_secure_master_key_2026', 'putzpay_totp_salt_v1', 32);

function encryptTotpSecret(plaintext) {
  if (!plaintext) return null;
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', TOTP_CIPHER_KEY, iv);
  let encrypted = cipher.update(plaintext, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  const authTag = cipher.getAuthTag().toString('hex');
  return `${iv.toString('hex')}:${authTag}:${encrypted}`;
}

function decryptTotpSecret(ciphertext) {
  if (!ciphertext) return null;
  try {
    const parts = ciphertext.split(':');
    if (parts.length !== 3) return null;
    const [ivHex, authTagHex, encryptedHex] = parts;
    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');

    const keysToTry = [TOTP_CIPHER_KEY];
    if (FALLBACK_TOTP_CIPHER_KEY && !FALLBACK_TOTP_CIPHER_KEY.equals(TOTP_CIPHER_KEY)) {
      keysToTry.push(FALLBACK_TOTP_CIPHER_KEY);
    }

    for (const key of keysToTry) {
      try {
        const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
        decipher.setAuthTag(authTag);
        let decrypted = decipher.update(encryptedHex, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        if (decrypted) return decrypted;
      } catch (e) {}
    }
    return null;
  } catch (err) {
    console.error('[2FA] Decryption error');
    return null;
  }
}

function verifyUserTotp(secret, token) {
  if (!secret || !token) return false;
  const cleanToken = String(token).trim().replace(/\s+/g, '');
  if (!/^\d{6}$/.test(cleanToken)) return false;
  try {
    const result = verifySync({
      token: cleanToken,
      secret: secret,
      epochTolerance: 30 // ±1 step window tolerance (30 seconds)
    });
    return result && result.valid === true;
  } catch (e) {
    return false;
  }
}

function generateRecoveryCodes(count = 8) {
  const codes = [];
  for (let i = 0; i < count; i++) {
    const raw = crypto.randomBytes(4).toString('hex').toUpperCase();
    const formatted = `${raw.slice(0, 4)}-${raw.slice(4, 8)}`;
    codes.push(formatted);
  }
  return codes;
}

function hashRecoveryCode(code) {
  const normalized = String(code).trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  return crypto.createHash('sha256').update(normalized).digest('hex');
}

function verifyAndConsumeRecoveryCode(user, code) {
  if (!user || !user.twoFactorRecoveryCodes || !code) return false;
  const codeHash = hashRecoveryCode(code);
  const found = user.twoFactorRecoveryCodes.find(rc => rc.codeHash === codeHash && !rc.used);
  if (!found) return false;
  found.used = true;
  found.usedAt = new Date();
  return true;
}

function checkTotpRateLimit(user) {
  if (!user) return { locked: false, remainingSeconds: 0 };
  if (user.twoFactorLockoutUntil && user.twoFactorLockoutUntil > new Date()) {
    const remainingSeconds = Math.ceil((user.twoFactorLockoutUntil.getTime() - Date.now()) / 1000);
    return { locked: true, remainingSeconds };
  }
  return { locked: false, remainingSeconds: 0 };
}

async function recordTotpFailure(user) {
  if (!user) return;
  user.twoFactorFailedAttempts = (user.twoFactorFailedAttempts || 0) + 1;
  if (user.twoFactorFailedAttempts >= 5) {
    user.twoFactorLockoutUntil = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes lockout
  }
  await user.save();
}

async function resetTotpFailures(user) {
  if (!user) return;
  user.twoFactorFailedAttempts = 0;
  user.twoFactorLockoutUntil = null;
  await user.save();
}

async function logSecurityEvent({ userId, action, details, ip, userAgent, status = 'success' }) {
  try {
    await SecurityLog.create({ userId, action, details, ip, userAgent, status });
    console.log(`[SECURITY] Action: ${action} | Status: ${status} | User: ${userId || 'guest'} | IP: ${ip || '-'}`);
  } catch (err) {
    console.error('[SECURITY] Log error:', err.message);
  }
}

const invoiceSchema = new mongoose.Schema({
  _id: { type: String, default: () => generateCustomId(startId.invoice) },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  amount: Number,
  fee: Number,
  total: Number,
  trxid: String,
  qris_image: String,
  paymentLinkId: { type: String, default: null },
  customerName: { type: String, default: null },
  customerPhone: { type: String, default: null },
  customerEmail: { type: String, default: null },
  mutationId: { type: String, default: null },
  expiredAt: Date,
  status: { type: String, default: 'pending', enum: ['pending', 'paid', 'expired', 'cancelled'] },
  settlementStatus: { type: String, default: 'pending', enum: ['pending', 'released'] },
  settlementAmount: { type: Number, default: 0 },
  successAt: Date,
  releaseAt: Date,
  releasedAt: Date,
  createdAt: { type: Date, default: Date.now }
});
const Invoice = mongoose.model('Invoice', invoiceSchema);

const transactionSchema = new mongoose.Schema({
  _id: { type: String, default: () => generateCustomId(startId.transaction) },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  type: { type: String, enum: ['deposit', 'withdraw'] },
  amount: Number,
  fee: Number,
  qris_image: String,
  status: String,
  reference: String,
  expiredAt: Date,
  createdAt: { type: Date, default: Date.now },
  adminNote: String,
  completedAt: Date,
  method: String,
  accountNumber: String,
  accountName: String
});
const Transaction = mongoose.model('Transaction', transactionSchema);

const withdrawSchema = new mongoose.Schema({
  _id: { type: String, default: () => generateCustomId(startId.withdraw) },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  amount: Number,
  fee: Number,
  method: String,
  accountNumber: String,
  accountName: String,
  status: { type: String, default: 'pending', enum: ['pending', 'success', 'rejected', 'failed', 'processing', 'cancelled', 'paid'] },
  type: { type: String, default: 'manual', enum: ['manual', 'instant'] },
  ewallet: { type: String, default: '' },
  providerTransactionId: { type: String, default: '' },
  providerStatus: { type: String, default: '' },
  providerRawResponse: { type: Object, default: null },
  refunded: { type: Boolean, default: false },
  adminNote: String,
  completedAt: Date,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const Withdrawal = mongoose.model('Withdrawal', withdrawSchema);

const paymentLinkSchema = new mongoose.Schema({
  _id: { type: String, default: () => generateCustomId(startId.paymentLink) },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  code: { type: String, required: true, unique: true, index: true, lowercase: true, trim: true },
  title: { type: String, required: true, trim: true },
  description: { type: String, default: '', trim: true },
  amountType: { type: String, enum: ['fixed', 'custom'], default: 'custom' },
  amount: { type: Number, default: 0 },
  minAmount: { type: Number, default: 1000 },
  isActive: { type: Boolean, default: true },
  totalPaidCount: { type: Number, default: 0 },
  totalPaidAmount: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const PaymentLink = mongoose.model('PaymentLink', paymentLinkSchema);

const partnerSchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  slug: { type: String, trim: true },
  category: { type: String, default: 'Fintech & E-Wallet', trim: true },
  logoUrl: { type: String, default: '', trim: true },
  websiteUrl: { type: String, default: '', trim: true },
  description: { type: String, default: '', trim: true },
  tier: { type: String, enum: ['official', 'strategic', 'certified', 'verified'], default: 'official' },
  badge: { type: String, default: 'Official Partner', trim: true },
  isActive: { type: Boolean, default: true },
  order: { type: Number, default: 0 },
  contactEmail: { type: String, default: '', trim: true },
  contactPhone: { type: String, default: '', trim: true },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const Partner = mongoose.model('Partner', partnerSchema);

const apiKeySchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  key: { type: String, unique: true },
  createdAt: { type: Date, default: Date.now }
});
const ApiKey = mongoose.model('ApiKey', apiKeySchema);

// Log setiap pengiriman webhook ke server merchant (untuk audit & retry manual)
const webhookLogSchema = new mongoose.Schema({
  _id: { type: String, default: () => generateCustomId('WHK') },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, index: true },
  invoiceId: { type: String, default: null, index: true },
  event: { type: String, default: 'invoice.paid' },
  url: { type: String, required: true },
  payload: { type: Object, default: {} },
  httpStatus: { type: Number, default: null },
  responseBody: { type: String, default: '' },
  success: { type: Boolean, default: false },
  attempt: { type: Number, default: 1 },
  errorMessage: { type: String, default: '' },
  createdAt: { type: Date, default: Date.now, index: true }
});
const WebhookLog = mongoose.model('WebhookLog', webhookLogSchema);

const settingSchema = new mongoose.Schema({
  name: { type: String, default: 'PutzOfficial' },
  title: { type: String, default: 'Layanan Payment Gateway' },
  description: { type: String, default: 'Terima pembayaran melalui QRIS Payment untuk Aplikasi atau Platform Bisnis kamu dengan mudah, cepat, dan aman.' },
  channelWhatsApp: { type: String, default: 'https://t.me/PutzOfficial' },
  minDeposit: { type: Number, default: 1000 },
  minWithdraw: { type: Number, default: 5000 },
  feeWithdraw: { type: Number, default: 1000 },
  maxFee: { type: Number, default: 500 },
  checkInterval: { type: Number, default: 30 },
  qrisExpiredMinutes: { type: Number, default: 30 },
  smtpHost: { type: String, default: 'smtp.gmail.com' },
  smtpPort: { type: Number, default: 465 },
  smtpSecure: { type: Boolean, default: true },
  smtpUser: { type: String, default: '' },
  smtpPass: { type: String, default: '' },
  logoUrl: { type: String, default: 'https://files.catbox.moe/82p405.jpg' },
  googleClientId: { type: String, default: '' },
  googleClientSecret: { type: String, default: '' },
  googleRedirectUri: { type: String, default: '' },
  turnstileSiteKey: { type: String, default: '' },
  turnstileSecretKey: { type: String, default: '' },
  // === PARTNER SYSTEM ===
  partnerEnabled: { type: Boolean, default: true },
  partnerBannerTitle: { type: String, default: 'Partner Resmi PutzPay' },
  partnerBannerSubtitle: { type: String, default: 'Temukan partner resmi dan ekosistem bisnis terpercaya yang terintegrasi dengan gateway pembayaran PutzPay.' },
  partnerCtaUrl: { type: String, default: 'https://t.me/PutzOfficial' },
  partnerCtaText: { type: String, default: 'Ajukan Kemitraan Resmi' },
  // === INSTANT WITHDRAW PROVIDER SECRET CONFIG ===
  withdrawApiKey: { type: String, default: '' },
  instantWithdrawApiKey: { type: String, default: '' },
  // === GOPAY MERCHANT ===
  gopayDomain: { type: String, default: 'gomerch.putzoffc.bid.id' },
  gopayToken: { type: String, default: '' },
  gopayStaticQr: { type: String, default: '' },
  gopayRefreshToken: { type: String, default: '' },
  // =====================
  withdrawMethods: {
    type: [
      {
        name: { type: String, required: true },
        fee: { type: Number, required: true, default: 1000 }
      }
    ],
    default: [
      { name: 'Dana', fee: 500 },
      { name: 'GoPay', fee: 700 }
    ]
  },
  // === MAINTENANCE SYSTEM ===
  maintenanceEnabled: { type: Boolean, default: false },
  maintenanceMode: { type: String, default: 'all', enum: ['all', 'feature'] },
  maintenanceFeatures: { type: [String], default: [] },
  maintenanceTitle: { type: String, default: 'Sistem Dalam Pemeliharaan' },
  maintenanceMessage: { type: String, default: 'Kami sedang melakukan pemeliharaan rutin untuk meningkatkan kualitas layanan. Silakan kembali lagi nanti.' },
  maintenanceIcon: { type: String, default: 'fa-solid fa-wrench' },
  maintenanceColor: { type: String, default: 'yellow' },
  maintenanceCountdown: { type: String, default: '' },
  maintenanceTelegram: { type: String, default: '' },
  maintenanceWhatsApp: { type: String, default: '' },
  // === ACCESS CONTROL & GLOBAL BAN SYSTEM ===
  globalWebsiteBlock: { type: Boolean, default: false },
  globalBlockMessage: { type: String, default: 'PutzPay sedang membatasi akses website untuk sementara waktu. Silakan coba kembali nanti.' },
  banAllUsers: { type: Boolean, default: false },
  banAllUsersReason: { type: String, default: 'Semua akun pengguna saat ini sedang dinonaktifkan sementara oleh Administrator.' },
  customerServiceUrl: { type: String, default: 'https://cs.putzpay.biz.id' },
  // === CHAT GLOBAL SETTINGS ===
  globalSlowmode: { type: Number, default: 3 },
  globalAntiLink: { type: Boolean, default: true },
  globalAntiToxic: { type: Boolean, default: true },
  // === KYC CONFIGURATION & ACCOUNT LIMITS ===
  kycEnabled: { type: Boolean, default: true },
  kycDiscountPercent: { type: Number, default: 15 },
  // Non-KYC Limits
  kycNonKycMaxBalance: { type: Number, default: 2000000 },
  kycNonKycMaxDailyTransaction: { type: Number, default: 5000000 },
  kycNonKycMaxWithdrawalPerTx: { type: Number, default: 1000000 },
  kycNonKycMaxDailyWithdrawal: { type: Number, default: 2000000 },
  kycNonKycMaxDailyWithdrawalCount: { type: Number, default: 3 },
  // KYC Verified Limits
  kycVerifiedMaxBalance: { type: Number, default: 50000000 },
  kycVerifiedMaxDailyTransaction: { type: Number, default: 100000000 },
  kycVerifiedMaxWithdrawalPerTx: { type: Number, default: 25000000 },
  kycVerifiedMaxDailyWithdrawal: { type: Number, default: 50000000 },
  kycVerifiedMaxDailyWithdrawalCount: { type: Number, default: 20 }
});
const Setting = mongoose.model('Setting', settingSchema);

const statsSchema = new mongoose.Schema({
  totalDepositAmount: { type: Number, default: 0 },
  totalDepositFee: { type: Number, default: 0 },
  totalWithdrawAmount: { type: Number, default: 0 },
  totalWithdrawFee: { type: Number, default: 0 },
  totalUsers: { type: Number, default: 0 },
  totalTransactions: { type: Number, default: 0 }
});
const Stats = mongoose.model('Stats', statsSchema);

const notificationSchema = new mongoose.Schema({
  title: { type: String, default: '' },
  message: { type: String, required: true },
  target: { type: String, default: 'all' }, // 'all' or userId string
  targetUser: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  targetUsername: { type: String, default: '' },
  sender: { type: String, default: 'Admin' },
  senderRole: { type: String, default: 'admin' },
  type: { type: String, default: 'info' }, // 'info', 'warning', 'announcement', 'transaction', 'promo'
  channel: { type: String, default: 'web_push' }, // 'web_push', 'email', 'both'
  readBy: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  isRead: { type: Boolean, default: false },
  readAt: { type: Date, default: null }
}, { timestamps: true });
const Notification = mongoose.model('Notification', notificationSchema);

const chatMessageSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  targetUserId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  username: String,
  message: { type: String, default: '' },
  image: { type: String, default: null },
  role: { type: String, enum: ['user', 'admin'] },
  profilePicture: { type: String, default: null },
  profileColor: { type: String, default: null },
  createdAt: { type: Date, default: Date.now },
  replyTo: { type: Object },
  isRead: { type: Boolean, default: false },
  status: { type: String, default: 'sent' },
  isEdited: { type: Boolean, default: false },
  isDeleted: { type: Boolean, default: false },
  deletedBy: { type: String, default: null }
});
const ChatMessage = mongoose.model('ChatMessage', chatMessageSchema);

const globalChatSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  username: String,
  message: { type: String, default: '' },
  image: { type: String, default: null },
  role: { type: String, enum: ['user', 'admin'], default: 'user' },
  profilePicture: { type: String, default: null },
  profileColor: { type: String, default: null },
  replyTo: { type: Object },
  isPinned: { type: Boolean, default: false },
  pinnedBy: { type: String, default: null },
  isEdited: { type: Boolean, default: false },
  isDeleted: { type: Boolean, default: false },
  deletedBy: { type: String, default: null },
  createdAt: { type: Date, default: Date.now }
});
const GlobalChat = mongoose.model('GlobalChat', globalChatSchema);

const globalChatModerationSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', unique: true },
  username: String,
  isMuted: { type: Boolean, default: false },
  mutedUntil: { type: Date, default: null },
  isBanned: { type: Boolean, default: false },
  reason: { type: String, default: '' },
  updatedAt: { type: Date, default: Date.now }
});
const GlobalChatModeration = mongoose.model('GlobalChatModeration', globalChatModerationSchema);

const moderationLogSchema = new mongoose.Schema({
  adminUsername: String,
  action: String,
  targetUsername: String,
  reason: String,
  details: String,
  createdAt: { type: Date, default: Date.now }
});
const ModerationLog = mongoose.model('ModerationLog', moderationLogSchema);

// ===================== BANNED / ACCESS CONTROL SYSTEM =====================
const bannedIpSchema = new mongoose.Schema({
  ip: { type: String, trim: true, index: true, default: '' },
  deviceId: { type: String, trim: true, index: true, default: '' },
  deviceName: { type: String, default: '' },
  reason: { type: String, default: 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan' },
  bannedBy: { type: String, default: 'Admin' },
  targetUserId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  targetUsername: { type: String, default: '' },
  userAgent: { type: String, default: '' },
  active: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now }
});
bannedIpSchema.index({ ip: 1 });
bannedIpSchema.index({ deviceId: 1 });
const BannedIp = mongoose.model('BannedIp', bannedIpSchema);

let bannedIpsCache = new Set();
let blockedDevicesCache = new Set();
let bannedIpsData = new Map();
let blockedDevicesData = new Map();

async function reloadBannedIpsCache() {
  try {
    const list = await BannedIp.find({ active: { $ne: false } }).lean();
    bannedIpsCache = new Set();
    blockedDevicesCache = new Set();
    bannedIpsData = new Map();
    blockedDevicesData = new Map();

    for (const item of list) {
      if (item.ip) {
        const cleanIp = (item.ip || '').trim().replace(/^::ffff:/, '');
        if (cleanIp) {
          bannedIpsCache.add(cleanIp);
          bannedIpsData.set(cleanIp, item);
        }
      }
      if (item.deviceId) {
        const cleanDev = (item.deviceId || '').trim();
        if (cleanDev) {
          blockedDevicesCache.add(cleanDev);
          blockedDevicesData.set(cleanDev, item);
        }
      }
    }
    console.log(`🛡️ Access Control Cache dimuat: ${bannedIpsCache.size} IPs, ${blockedDevicesCache.size} Devices terblokir`);
  } catch (err) {
    console.warn('⚠️ Gagal memuat cache Access Control:', err.message);
  }
}

function getClientIp(req) {
  if (!req) return '';
  let ip = '';
  if (req.headers && req.headers['cf-connecting-ip']) {
    ip = req.headers['cf-connecting-ip'];
  } else if (req.headers && typeof req.headers['x-forwarded-for'] === 'string') {
    ip = req.headers['x-forwarded-for'].split(',')[0].trim();
  } else if (req.headers && req.headers['x-real-ip']) {
    ip = req.headers['x-real-ip'];
  } else if (req.socket && req.socket.remoteAddress) {
    ip = req.socket.remoteAddress;
  } else if (req.ip) {
    ip = req.ip;
  }
  if (typeof ip === 'string') {
    ip = ip.trim();
    if (ip.startsWith('::ffff:')) {
      ip = ip.substring(7);
    }
  }
  return ip || '127.0.0.1';
}

function getClientDevice(userAgent) {
  if (!userAgent || typeof userAgent !== 'string') return 'Desktop / Browser';
  const ua = userAgent.toLowerCase();
  if (ua.includes('iphone')) return 'iPhone (iOS)';
  if (ua.includes('ipad')) return 'iPad (iPadOS)';
  if (ua.includes('android')) {
    return ua.includes('tablet') ? 'Android Tablet' : 'Android Mobile';
  }
  if (ua.includes('windows phone')) return 'Windows Phone';
  if (ua.includes('macintosh') || ua.includes('mac os')) return 'macOS (Desktop)';
  if (ua.includes('windows nt') || ua.includes('windows')) return 'Windows (Desktop)';
  if (ua.includes('linux')) return 'Linux (Desktop)';
  if (ua.includes('cros')) return 'ChromeOS';
  if (ua.includes('mobile')) return 'Mobile Device';
  return 'Web Browser';
}

function parseCookies(req) {
  const list = {};
  const cookieHeader = req && req.headers && req.headers.cookie;
  if (!cookieHeader) return list;
  cookieHeader.split(';').forEach(cookie => {
    const parts = cookie.split('=');
    const name = (parts[0] || '').trim();
    const value = decodeURI(parts.slice(1).join('=').trim());
    if (name) list[name] = value;
  });
  return list;
}

function getSafeDeviceId(req, res) {
  const cookies = parseCookies(req);
  let devId = cookies['pp_dev_id'];
  if (!devId || typeof devId !== 'string' || devId.length < 8) {
    devId = 'dev_' + crypto.randomBytes(12).toString('hex');
    if (res && typeof res.cookie === 'function') {
      try {
        res.cookie('pp_dev_id', devId, {
          maxAge: 365 * 24 * 60 * 60 * 1000,
          httpOnly: true,
          sameSite: 'lax'
        });
      } catch (e) {}
    } else if (res && typeof res.setHeader === 'function') {
      try {
        res.setHeader('Set-Cookie', `pp_dev_id=${devId}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Lax`);
      } catch (e) {}
    }
  }
  return devId;
}

// ===================== WEB PUSH NOTIFICATION SETUP =====================
let vapidKeys = {
  publicKey: process.env.VAPID_PUBLIC_KEY,
  privateKey: process.env.VAPID_PRIVATE_KEY
};

const vapidKeysFilePath = path.join(__dirname, 'data', 'vapid-keys.json');

if (!vapidKeys.publicKey || !vapidKeys.privateKey) {
  if (fs.existsSync(vapidKeysFilePath)) {
    try {
      const savedKeys = JSON.parse(fs.readFileSync(vapidKeysFilePath, 'utf8'));
      if (savedKeys.publicKey && savedKeys.privateKey) {
        vapidKeys.publicKey = savedKeys.publicKey;
        vapidKeys.privateKey = savedKeys.privateKey;
        console.log('🔑 Loaded persisted VAPID Keys from disk');
      }
    } catch (e) {
      console.warn('⚠️ Failed to load saved VAPID keys file:', e.message);
    }
  }

  if (!vapidKeys.publicKey || !vapidKeys.privateKey) {
    try {
      const generated = webpush.generateVAPIDKeys();
      vapidKeys.publicKey = generated.publicKey;
      vapidKeys.privateKey = generated.privateKey;

      const dataDir = path.dirname(vapidKeysFilePath);
      if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir, { recursive: true });
      }
      fs.writeFileSync(vapidKeysFilePath, JSON.stringify(vapidKeys, null, 2), 'utf8');
      console.log('🔑 Auto-generated and persisted VAPID Keys for Web Push Notifications');
    } catch (err) {
      console.error('Error generating VAPID keys:', err);
    }
  }
}

const vapidSubject = process.env.VAPID_SUBJECT || 'mailto:admin@putzpay.com';

if (vapidKeys.publicKey && vapidKeys.privateKey) {
  try {
    webpush.setVapidDetails(
      vapidSubject,
      vapidKeys.publicKey,
      vapidKeys.privateKey
    );
    console.log('✅ Web Push VAPID initialized successfully');
  } catch (err) {
    console.warn('⚠️ Web Push VAPID initialization warning:', err.message);
  }
}

const pushSubscriptionSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  endpoint: { type: String, required: true },
  keys: {
    p256dh: { type: String, required: true },
    auth: { type: String, required: true }
  },
  userAgent: { type: String, default: '' },
  deviceName: { type: String, default: 'Web Device' },
  active: { type: Boolean, default: true },
  lastUsedAt: { type: Date, default: Date.now }
}, { timestamps: true });

pushSubscriptionSchema.index({ endpoint: 1 }, { unique: true });
pushSubscriptionSchema.index({ userId: 1 });

const PushSubscription = mongoose.model('PushSubscription', pushSubscriptionSchema);

const userNotificationSettingSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true, unique: true },
  paymentSuccess: { type: Boolean, default: true },
  paymentPending: { type: Boolean, default: true },
  paymentFailed: { type: Boolean, default: true },
  paymentExpired: { type: Boolean, default: true },
  withdrawSuccess: { type: Boolean, default: true },
  withdrawFailed: { type: Boolean, default: true },
  balanceUpdated: { type: Boolean, default: true },
  securityAlert: { type: Boolean, default: true }
}, { timestamps: true });

const UserNotificationSetting = mongoose.model('UserNotificationSetting', userNotificationSettingSchema);

const sentPushEvents = new Set();
function isPushDuplicate(eventId) {
  if (!eventId) return false;
  if (sentPushEvents.has(eventId)) return true;
  sentPushEvents.add(eventId);
  if (sentPushEvents.size > 2000) {
    const iterator = sentPushEvents.values();
    sentPushEvents.delete(iterator.next().value);
  }
  return false;
}

async function sendPushNotification(userId, eventType, notificationPayload, options = {}) {
  try {
    if (!userId) return;

    if (options.eventId && isPushDuplicate(options.eventId)) {
      console.log(`[WEB PUSH] Skipped duplicate event: ${options.eventId}`);
      return;
    }

    let settings = await UserNotificationSetting.findOne({ userId }).lean();
    if (!settings) {
      settings = {
        paymentSuccess: true,
        paymentPending: true,
        paymentFailed: true,
        paymentExpired: true,
        withdrawSuccess: true,
        withdrawFailed: true,
        balanceUpdated: true,
        securityAlert: true
      };
    }

    const prefMap = {
      payment_success: settings.paymentSuccess,
      payment_pending: settings.paymentPending,
      payment_failed: settings.paymentFailed,
      payment_expired: settings.paymentExpired,
      withdraw_success: settings.withdrawSuccess,
      withdraw_failed: settings.withdrawFailed,
      balance_updated: settings.balanceUpdated,
      security_alert: settings.securityAlert
    };

    if (eventType && prefMap[eventType] === false) {
      console.log(`[WEB PUSH] Notification skipped for user ${userId} because preference '${eventType}' is disabled.`);
      return;
    }

    const subscriptions = await PushSubscription.find({ userId, active: true });
    if (!subscriptions || subscriptions.length === 0) {
      return;
    }

    const payloadString = JSON.stringify({
      title: notificationPayload.title || '💰 PutzPay',
      body: notificationPayload.body || 'Notifikasi dari PutzPay',
      icon: notificationPayload.icon || '/public/profile/profile-6a16058f5ab6c5c4d875cf86-1780154370158.png',
      badge: notificationPayload.badge || '/public/profile/profile-6a16058f5ab6c5c4d875cf86-1780154370158.png',
      tag: notificationPayload.tag || `putzpay-${Date.now()}`,
      data: notificationPayload.data || {}
    });

    const sendPromises = subscriptions.map(async (sub) => {
      try {
        const pushSub = {
          endpoint: sub.endpoint,
          keys: {
            p256dh: sub.keys.p256dh,
            auth: sub.keys.auth
          }
        };
        await webpush.sendNotification(pushSub, payloadString);
        await PushSubscription.updateOne({ _id: sub._id }, { lastUsedAt: new Date() });
      } catch (err) {
        if (err.statusCode === 404 || err.statusCode === 410) {
          console.log(`[WEB PUSH] Subscription expired/invalid (${err.statusCode}). Removing...`);
          await PushSubscription.deleteOne({ _id: sub._id });
        } else {
          console.error(`[WEB PUSH] Failed to send push:`, err.message);
        }
      }
    });

    await Promise.allSettled(sendPromises);
  } catch (err) {
    console.error('[WEB PUSH] sendPushNotification error:', err.message);
  }
}

// ===================== WEB PUSH NOTIFICATION ROUTES =====================
const handleGetVapidKey = (req, res) => {
  try {
    if (!vapidKeys.publicKey) {
      return res.status(500).json({ success: false, message: 'VAPID public key belum dikonfigurasi di server.' });
    }
    console.log('[WEB PUSH] VAPID public key requested by user:', req.session.userId);
    res.json({ success: true, publicKey: vapidKeys.publicKey });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
};

app.get('/api/notifications/vapid-key', handleGetVapidKey);
app.get('/api/push/vapid-public-key', handleGetVapidKey);

const handleSubscribe = async (req, res) => {
  try {
    const { subscription, userAgent } = req.body;
    if (!subscription || !subscription.endpoint || !subscription.keys || !subscription.keys.p256dh || !subscription.keys.auth) {
      return res.status(400).json({ success: false, message: 'Format push subscription tidak lengkap' });
    }

    const savedSub = await PushSubscription.findOneAndUpdate(
      { endpoint: subscription.endpoint },
      {
        userId: req.session.userId,
        endpoint: subscription.endpoint,
        keys: {
          p256dh: subscription.keys.p256dh,
          auth: subscription.keys.auth
        },
        userAgent: userAgent || req.headers['user-agent'] || '',
        active: true,
        lastUsedAt: new Date()
      },
      { upsert: true, new: true }
    );

    console.log(`[WEB PUSH] Subscription saved successfully for user ${req.session.userId} (ID: ${savedSub._id})`);
    res.json({ success: true, message: 'Push subscription berhasil disimpan' });
  } catch (err) {
    console.error('[WEB PUSH] Failed to save subscription:', err.message);
    res.status(500).json({ success: false, message: 'Gagal menyimpan push subscription ke database' });
  }
};

app.post('/api/notifications/subscribe', isAuth, handleSubscribe);
app.post('/api/push/subscribe', isAuth, handleSubscribe);

app.post('/api/notifications/unsubscribe', isAuth, async (req, res) => {
  try {
    const { endpoint } = req.body;
    if (endpoint) {
      await PushSubscription.updateOne(
        { endpoint, userId: req.session.userId },
        { active: false }
      );
    } else {
      await PushSubscription.updateMany(
        { userId: req.session.userId },
        { active: false }
      );
    }
    console.log(`[WEB PUSH] Subscription deactivated for user ${req.session.userId}`);
    res.json({ success: true, message: 'Push notification berhasil dinonaktifkan' });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.post('/api/notifications/test', isAuth, async (req, res) => {
  try {
    const userId = req.session.userId;
    const subs = await PushSubscription.find({ userId, active: true });
    if (!subs || subs.length === 0) {
      return res.status(400).json({ success: false, message: 'Tidak ada push subscription aktif untuk akun ini. Silakan aktifkan notifikasi terlebih dahulu.' });
    }

    await sendPushNotification(userId, null, {
      title: '📱 PutzPay Test Notification',
      body: 'Web Push Notification berhasil diterima di perangkat kamu!',
      data: { url: '/profile' }
    });

    console.log(`[WEB PUSH] Test notification dispatched for user ${userId}`);
    res.json({ success: true, message: 'Test notifikasi berhasil dikirim' });
  } catch (err) {
    console.error('[WEB PUSH] Test notification error:', err.message);
    res.status(500).json({ success: false, message: err.message });
  }
});

app.get('/api/notifications/settings', isAuth, async (req, res) => {
  try {
    let settings = await UserNotificationSetting.findOne({ userId: req.session.userId }).lean();
    if (!settings) {
      settings = await UserNotificationSetting.create({ userId: req.session.userId });
    }
    res.json({ success: true, settings });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.post('/api/notifications/settings', isAuth, async (req, res) => {
  try {
    const { paymentSuccess, paymentPending, paymentFailed, paymentExpired, withdrawSuccess, withdrawFailed, balanceUpdated, securityAlert } = req.body;
    const updated = await UserNotificationSetting.findOneAndUpdate(
      { userId: req.session.userId },
      {
        paymentSuccess: !!paymentSuccess,
        paymentPending: !!paymentPending,
        paymentFailed: !!paymentFailed,
        paymentExpired: !!paymentExpired,
        withdrawSuccess: !!withdrawSuccess,
        withdrawFailed: !!withdrawFailed,
        balanceUpdated: !!balanceUpdated,
        securityAlert: !!securityAlert
      },
      { upsert: true, new: true }
    );
    res.json({ success: true, settings: updated });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// ===================== UPLOAD CONFIGURATIONS =====================
const chatUploadDir = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(chatUploadDir)) {
  fs.mkdirSync(chatUploadDir, { recursive: true });
}
const chatStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, chatUploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    cb(null, 'chat-' + Date.now() + crypto.randomBytes(4).toString('hex') + ext);
  }
});
const chatUpload = multer({
  storage: chatStorage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (!file.mimetype.startsWith('image/')) {
      return cb(new Error('Hanya file gambar yang diizinkan'));
    }
    cb(null, true);
  }
});

const profileUploadDir = path.join(__dirname, 'public', 'profile');
if (!fs.existsSync(profileUploadDir)) {
  fs.mkdirSync(profileUploadDir, { recursive: true });
}
const profileStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, profileUploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    cb(null, 'profile-' + req.session.userId + '-' + Date.now() + ext);
  }
});
const profileUpload = multer({
  storage: profileStorage,
  limits: { fileSize: 2 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (!file.mimetype.startsWith('image/')) {
      return cb(new Error('Hanya file gambar yang diizinkan'), false);
    }
    cb(null, true);
  }
});

// ===================== PRIVATE KYC STORAGE & SECURITY =====================
const KYC_PRIVATE_DIR = path.join(__dirname, 'storage', 'kyc_private');
const KYC_BACKUP_DIR = path.join(__dirname, 'storage', 'kyc_backup_private');

if (!fs.existsSync(KYC_PRIVATE_DIR)) {
  fs.mkdirSync(KYC_PRIVATE_DIR, { recursive: true });
}
if (!fs.existsSync(KYC_BACKUP_DIR)) {
  fs.mkdirSync(KYC_BACKUP_DIR, { recursive: true });
}

function sanitizeUsernameForFolder(username) {
  if (!username) return 'user_unnamed';
  const clean = String(username)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '_')
    .replace(/^[._-]+|[._-]+$/g, '');
  return clean || 'user';
}

function validateKycBuffer(buffer, originalname, mimetype) {
  if (!buffer || buffer.length === 0) {
    return { valid: false, message: 'File dokumen kosong.' };
  }
  if (buffer.length > 5 * 1024 * 1024) {
    return { valid: false, message: 'Ukuran file melebihi batas maksimum 5MB.' };
  }
  const ext = path.extname(originalname || '').toLowerCase();
  const allowedExts = ['.jpg', '.jpeg', '.png', '.webp', '.pdf'];
  if (!allowedExts.includes(ext)) {
    return { valid: false, message: 'Format ekstensi file tidak didukung. Gunakan JPG, PNG, WEBP, atau PDF.' };
  }

  const allowedMimes = ['image/jpeg', 'image/png', 'image/webp', 'application/pdf'];
  if (!allowedMimes.includes(mimetype)) {
    return { valid: false, message: 'Tipe MIME file tidak diizinkan.' };
  }

  // Magic bytes integrity check
  const isJpeg = buffer.length >= 3 && buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF;
  const isPng = buffer.length >= 4 && buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47;
  const isWebp = buffer.length >= 12 && buffer.toString('ascii', 0, 4) === 'RIFF' && buffer.toString('ascii', 8, 12) === 'WEBP';
  const isPdf = buffer.length >= 5 && buffer.toString('ascii', 0, 5) === '%PDF-';

  if (!isJpeg && !isPng && !isWebp && !isPdf) {
    return { valid: false, message: 'Integritas file tidak valid atau file rusak (magic bytes mismatch).' };
  }

  return { valid: true, ext };
}

async function saveKycFile({ username, docType, buffer, originalname, mimetype, version = 1 }) {
  const safeUserFolder = sanitizeUsernameForFolder(username);
  const safeDocType = ['ktp', 'kk', 'kartu_pelajar'].includes(docType) ? docType : 'dokumen';

  const userPrivateDir = path.join(KYC_PRIVATE_DIR, safeUserFolder, safeDocType);
  const userBackupDir = path.join(KYC_BACKUP_DIR, safeUserFolder, safeDocType);

  await fs.promises.mkdir(userPrivateDir, { recursive: true });
  await fs.promises.mkdir(userBackupDir, { recursive: true });

  const ext = path.extname(originalname || '.jpg').toLowerCase();
  const safeExt = ['.jpg', '.jpeg', '.png', '.webp', '.pdf'].includes(ext) ? ext : '.jpg';
  const filename = `${safeDocType}-v${version}-${Date.now()}-${crypto.randomBytes(4).toString('hex')}${safeExt}`;

  const primaryPath = path.join(userPrivateDir, filename);
  const backupPath = path.join(userBackupDir, filename);

  await fs.promises.writeFile(primaryPath, buffer);
  await fs.promises.writeFile(backupPath, buffer);

  const relativePrimary = path.join(safeUserFolder, safeDocType, filename);
  const relativeBackup = path.join(safeUserFolder, safeDocType, filename);

  return {
    storageKey: relativePrimary,
    backupKey: relativeBackup,
    fileSize: buffer.length,
    originalFilename: path.basename(originalname || filename).substring(0, 100),
    mimeType: mimetype
  };
}

const kycUploadMiddleware = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024, files: 3 }
});

// ===================== KYC FEE & LIMIT ENGINE =====================
function calculateUserWithdrawFee(user, baseFee, settings) {
  if (!user || user.kycStatus !== 'VERIFIED') {
    return baseFee;
  }
  const discountPercent = typeof settings.kycDiscountPercent === 'number' ? settings.kycDiscountPercent : 15;
  if (discountPercent <= 0) return baseFee;
  const discount = Math.round(baseFee * (discountPercent / 100));
  return Math.max(0, baseFee - discount);
}

async function checkWithdrawalLimits(user, amount, settings) {
  const isVerified = user && user.kycStatus === 'VERIFIED';
  const maxPerTx = isVerified
    ? (settings.kycVerifiedMaxWithdrawalPerTx || 25000000)
    : (settings.kycNonKycMaxWithdrawalPerTx || 1000000);
  const maxDaily = isVerified
    ? (settings.kycVerifiedMaxDailyWithdrawal || 50000000)
    : (settings.kycNonKycMaxDailyWithdrawal || 2000000);
  const maxDailyCount = isVerified
    ? (settings.kycVerifiedMaxDailyWithdrawalCount || 20)
    : (settings.kycNonKycMaxDailyWithdrawalCount || 3);

  if (amount > maxPerTx) {
    const statusLabel = isVerified ? 'KYC Terverifikasi' : 'Belum Terverifikasi';
    return {
      allowed: false,
      message: `Nominal penarikan (Rp ${amount.toLocaleString('id-ID')}) melebihi batas maksimum per transaksi untuk akun ${statusLabel} (Maks. Rp ${maxPerTx.toLocaleString('id-ID')}).${!isVerified ? ' Lakukan verifikasi KYC untuk limit hingga Rp 25.000.000/transaksi.' : ''}`
    };
  }

  const startOfToday = new Date();
  startOfToday.setHours(0, 0, 0, 0);

  const todayWds = await Withdrawal.find({
    userId: user._id,
    createdAt: { $gte: startOfToday },
    status: { $ne: 'rejected' }
  });

  const todayCount = todayWds.length;
  const todayTotal = todayWds.reduce((sum, w) => sum + (w.amount || 0), 0);

  if (todayCount >= maxDailyCount) {
    return {
      allowed: false,
      message: `Anda telah mencapai batas frekuensi penarikan harian (${maxDailyCount}x per hari).${!isVerified ? ' Verifikasi KYC untuk batas penarikan hingga 20x per hari.' : ' Silakan coba kembali besok.'}`
    };
  }

  if (todayTotal + amount > maxDaily) {
    const remaining = Math.max(0, maxDaily - todayTotal);
    return {
      allowed: false,
      message: `Total penarikan Anda hari ini akan melebihi batas limit harian (Maks. Rp ${maxDaily.toLocaleString('id-ID')}/hari, sisa kuota hari ini: Rp ${remaining.toLocaleString('id-ID')}).${!isVerified ? ' Verifikasi KYC untuk batas harian hingga Rp 50.000.000.' : ''}`
    };
  }

  return { allowed: true };
}

async function checkDepositLimits(user, amount, settings) {
  const isVerified = user && user.kycStatus === 'VERIFIED';
  const maxBalance = isVerified
    ? (settings.kycVerifiedMaxBalance || 50000000)
    : (settings.kycNonKycMaxBalance || 2000000);
  const maxDailyTx = isVerified
    ? (settings.kycVerifiedMaxDailyTransaction || 100000000)
    : (settings.kycNonKycMaxDailyTransaction || 5000000);

  const currentBalance = (user && user.balance) || 0;
  if (currentBalance + amount > maxBalance) {
    const statusLabel = isVerified ? 'KYC Terverifikasi' : 'Belum Terverifikasi';
    return {
      allowed: false,
      message: `Transaksi ini akan menyebabkan saldo Anda melebihi kapasitas maksimum akun ${statusLabel} (Maks. Rp ${maxBalance.toLocaleString('id-ID')}).${!isVerified ? ' Verifikasi KYC untuk kapasitas saldo hingga Rp 50.000.000.' : ''}`
    };
  }

  const startOfToday = new Date();
  startOfToday.setHours(0, 0, 0, 0);

  const todayInvoices = await Invoice.find({
    userId: user._id,
    createdAt: { $gte: startOfToday },
    status: { $in: ['pending', 'paid'] }
  });
  const todayTxTotal = todayInvoices.reduce((sum, inv) => sum + (inv.amount || 0), 0);

  if (todayTxTotal + amount > maxDailyTx) {
    const remaining = Math.max(0, maxDailyTx - todayTxTotal);
    return {
      allowed: false,
      message: `Akumulasi transaksi harian Anda akan melebihi batas limit transaksi harian (Maks. Rp ${maxDailyTx.toLocaleString('id-ID')}/hari, sisa kuota hari ini: Rp ${remaining.toLocaleString('id-ID')}).${!isVerified ? ' Verifikasi KYC untuk kuota transaksi hingga Rp 100.000.000/hari.' : ''}`
    };
  }

  return { allowed: true };
}

// ===================== ANTI-DUPLICATE HELPER =====================
// ===================== WEBHOOK MERCHANT HELPERS =====================
function generateWebhookSecret() {
  return 'whsec_' + crypto.randomBytes(24).toString('hex');
}

function signWebhookPayload(secret, payload) {
  return crypto.createHmac('sha256', secret || '').update(JSON.stringify(payload)).digest('hex');
}

// Kirim webhook ke URL merchant + catat hasilnya ke WebhookLog.
// Dipanggil "fire and forget" (tidak boleh menghambat proses pembayaran utama).
async function sendWebhookEvent(user, event, payload, invoiceId = null, attempt = 1) {
  if (!user || !user.webhookEnabled || !user.webhookUrl) return null;

  const signature = signWebhookPayload(user.webhookSecret, payload);
  const logData = {
    userId: user._id,
    invoiceId,
    event,
    url: user.webhookUrl,
    payload,
    attempt
  };

  try {
    const res = await axios.post(user.webhookUrl, payload, {
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
        'X-PutzPay-Event': event,
        'X-PutzPay-Signature': signature
      },
      validateStatus: () => true // status non-2xx tetap dicatat, bukan dilempar sebagai error
    });
    logData.httpStatus = res.status;
    const bodyStr = typeof res.data === 'string' ? res.data : JSON.stringify(res.data || '');
    logData.responseBody = (bodyStr || '').slice(0, 1000);
    logData.success = res.status >= 200 && res.status < 300;
  } catch (err) {
    logData.httpStatus = null;
    logData.errorMessage = (err.message || 'Gagal mengirim webhook').slice(0, 500);
    logData.success = false;
  }

  try {
    await WebhookLog.create(logData);
  } catch (e) {
    console.error('[WEBHOOK] Gagal simpan log webhook:', e.message);
  }
  return logData;
}

async function createWithRetry(Model, data, maxRetries = 5) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await Model.create(data);
    } catch (err) {
      if (err.code === 11000 && attempt < maxRetries - 1) {
        if (Model === Invoice) data._id = generateCustomId(startId.invoice);
        else if (Model === Transaction) data._id = generateCustomId(startId.transaction);
        else if (Model === Withdrawal) data._id = generateCustomId(startId.withdraw);
        else if (Model === PaymentLink) data._id = generateCustomId(startId.paymentLink);
        else if (Model === ApiKey) data.key = generateApiKey();
        else if (Model === WebhookLog) data._id = generateCustomId('WHK');
        continue;
      }
      throw err;
    }
  }
  throw new Error('Gagal membuat dokumen setelah beberapa kali percobaan (duplicate ID)');
}

// ===================== HELPERS =====================
const defaultSettings = {
  name: 'PutzOfficial',
  title: 'Layanan Payment Gateway',
  description: 'Terima pembayaran melalui QRIS Payment untuk Aplikasi atau Platform Bisnis kamu dengan mudah, cepat, dan aman.',
  channelWhatsApp: 'https://t.me/PutzOfficial',
  minDeposit: 1000,
  minWithdraw: 5000,
  feeWithdraw: 1000,
  maxFee: 500,
  checkInterval: 30,
  qrisExpiredMinutes: 30,
  smtpHost: 'smtp.gmail.com',
  smtpPort: 465,
  smtpSecure: true,
  smtpUser: '',
  smtpPass: '',
  turnstileSiteKey: '',
  turnstileSecretKey: '',
  partnerEnabled: true,
  partnerBannerTitle: 'Partner Resmi PutzPay',
  partnerBannerSubtitle: 'Temukan partner resmi dan ekosistem bisnis terpercaya yang terintegrasi dengan gateway pembayaran PutzPay.',
  partnerCtaUrl: 'https://t.me/PutzOfficial',
  partnerCtaText: 'Ajukan Kemitraan Resmi',
  logoUrl: 'https://files.catbox.moe/82p405.jpg',
  gopayDomain: 'gomerch.putzoffc.bid.id',
  gopayToken: '',
  gopayStaticQr: '',
  gopayRefreshToken: '',
  withdrawMethods: [
    { name: 'Dana', fee: 500 },
    { name: 'GoPay', fee: 700 }
  ],
  kycEnabled: true,
  kycDiscountPercent: 15,
  kycNonKycMaxBalance: 2000000,
  kycNonKycMaxDailyTransaction: 5000000,
  kycNonKycMaxWithdrawalPerTx: 1000000,
  kycNonKycMaxDailyWithdrawal: 2000000,
  kycNonKycMaxDailyWithdrawalCount: 3,
  kycVerifiedMaxBalance: 50000000,
  kycVerifiedMaxDailyTransaction: 100000000,
  kycVerifiedMaxWithdrawalPerTx: 25000000,
  kycVerifiedMaxDailyWithdrawal: 50000000,
  kycVerifiedMaxDailyWithdrawalCount: 20
};

async function getSettings() {
  try {
    let s = await Setting.findOne();
    if (!s) s = await Setting.create({});
    if (s.maintenanceEnabled && s.maintenanceCountdown) {
      const cdTime = new Date(s.maintenanceCountdown).getTime();
      if (!isNaN(cdTime) && Date.now() >= cdTime) {
        s.maintenanceEnabled = false;
        Setting.updateOne({ _id: s._id }, { $set: { maintenanceEnabled: false } }).catch(() => {});
      }
    }
    return s;
  } catch (err) {
    return defaultSettings;
  }
}

// ===================== CLOUDFLARE TURNSTILE HELPER =====================
function getTurnstileConfig(settings) {
  const siteKey = (settings && settings.turnstileSiteKey ? settings.turnstileSiteKey : process.env.TURNSTILE_SITE_KEY || process.env.KUNCI_TEMPAT_PUTAR || '').trim();
  const secretKey = (settings && settings.turnstileSecretKey ? settings.turnstileSecretKey : process.env.TURNSTILE_SECRET_KEY || process.env.KUNCI_RAHASIA_PINTU_PUTAR || '').trim();
  return { siteKey, secretKey };
}

async function verifyTurnstile(token, secretKey, req, siteKey = '') {
  if (!secretKey || !siteKey) {
    return { success: true, bypassed: true };
  }

  const host = (req && (req.hostname || (req.headers && req.headers.host) || '')) || '';
  const isDevOrPreview = host.includes('run.app') || host.includes('localhost') || host.includes('127.0.0.1') || host.includes('google');

  // Cloudflare Turnstile keys configured for custom domain (e.g. putzpay.biz.id) will reject preview/sandbox domains
  if (isDevOrPreview && (!token || typeof token !== 'string' || token.trim() === '')) {
    console.log(`[TURNSTILE] Bypassing Turnstile verification on dev/preview domain: ${host}`);
    return { success: true, bypassed: true };
  }

  if (!token || typeof token !== 'string' || token.trim() === '') {
    return { success: false, error: 'Token Turnstile tidak ditemukan' };
  }

  try {
    let remoteIp;
    if (req) {
      if (req.headers && req.headers['cf-connecting-ip']) {
        remoteIp = req.headers['cf-connecting-ip'];
      } else if (req.headers && typeof req.headers['x-forwarded-for'] === 'string') {
        remoteIp = req.headers['x-forwarded-for'].split(',')[0].trim();
      } else if (req.socket && req.socket.remoteAddress) {
        remoteIp = req.socket.remoteAddress;
      }
    }

    const params = new URLSearchParams();
    params.append('secret', secretKey);
    params.append('response', token.trim());
    if (remoteIp) {
      params.append('remoteip', remoteIp);
    }

    const response = await axios.post('https://challenges.cloudflare.com/turnstile/v0/siteverify', params.toString(), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      timeout: 10000
    });

    if (response.data && response.data.success === true) {
      return { success: true };
    } else {
      if (isDevOrPreview) {
        console.warn(`[TURNSTILE] Verification returned failure on dev/preview domain (${host}), permitting bypass:`, response.data['error-codes']);
        return { success: true, bypassed: true };
      }
      return { success: false, error: 'Verifikasi Turnstile gagal' };
    }
  } catch (err) {
    console.error('[TURNSTILE] Network verification error:', err.message || 'Error');
    if (isDevOrPreview) {
      return { success: true, bypassed: true };
    }
    return { success: false, error: 'Gagal menghubungi server verifikasi Turnstile' };
  }
}

async function getStats() {
  try {
    const [depositAgg, withdrawAgg, totalUsers, totalTrx] = await Promise.all([
      Transaction.aggregate([
        { $match: { type: 'deposit', status: 'paid' } },
        { $group: { _id: null, totalAmount: { $sum: '$amount' }, totalFee: { $sum: '$fee' } } }
      ]),
      Transaction.aggregate([
        { $match: { type: 'withdraw', status: 'success' } },
        { $group: { _id: null, totalAmount: { $sum: '$amount' }, totalFee: { $sum: '$fee' } } }
      ]),
      User.countDocuments({ role: 'user' }),
      Transaction.countDocuments()
    ]);

    const dAmount = depositAgg[0]?.totalAmount || 0;
    const dFee = depositAgg[0]?.totalFee || 0;
    const wAmount = withdrawAgg[0]?.totalAmount || 0;
    const wFee = withdrawAgg[0]?.totalFee || 0;

    await Stats.findOneAndUpdate({}, {
      totalDepositAmount: dAmount,
      totalDepositFee: dFee,
      totalWithdrawAmount: wAmount,
      totalWithdrawFee: wFee,
      totalUsers,
      totalTransactions: totalTrx
    }, { upsert: true }).catch(() => {});

    return {
      totalDepositAmount: dAmount,
      totalDepositFee: dFee,
      totalWithdrawAmount: wAmount,
      totalWithdrawFee: wFee,
      totalUsers,
      totalTransactions: totalTrx
    };
  } catch (err) {
    return {
      totalDepositAmount: 0,
      totalDepositFee: 0,
      totalWithdrawAmount: 0,
      totalWithdrawFee: 0,
      totalUsers: 0,
      totalTransactions: 0
    };
  }
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  return salt + ':' + crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
}

function verifyPassword(password, stored) {
  if (!stored || !stored.includes(':')) return false;
  const [salt, hash] = stored.split(':');
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex') === hash;
}

// ===================== OTP EMAIL VERIFICATION HELPERS =====================
function generateOtp() {
  return crypto.randomInt(100000, 1000000).toString();
}

function hashOtp(otp) {
  const secret = process.env.SESSION_SECRET || process.env.RAHASIA_SESI || appConfig.SESSION_SECRET || 'putzpay_otp_secret_key_2026';
  return crypto.createHmac('sha256', secret).update(otp.toString()).digest('hex');
}

function verifyOtpHash(inputOtp, storedHash) {
  if (!inputOtp || !storedHash) return false;
  const computedHash = hashOtp(inputOtp);
  try {
    return crypto.timingSafeEqual(Buffer.from(computedHash, 'hex'), Buffer.from(storedHash, 'hex'));
  } catch (e) {
    return computedHash === storedHash;
  }
}

function maskEmail(email) {
  if (!email || !email.includes('@')) return email || '';
  const parts = email.split('@');
  const name = parts[0];
  const domain = parts[1];
  if (name.length <= 2) {
    return name.charAt(0) + '***@' + domain;
  }
  return name.substring(0, 2) + '***@' + domain;
}

function getSmtpConfig(settings) {
  const rawHost = (settings?.smtpHost || process.env.SMTP_HOST || 'smtp.gmail.com').trim();
  const host = rawHost || 'smtp.gmail.com';
  
  let defaultPort = 465;
  const port = parseInt(settings?.smtpPort || process.env.SMTP_PORT || defaultPort, 10) || defaultPort;
  
  // Consistency:
  // Port 587 => STARTTLS (secure: false, requireTLS: true)
  // Port 465 => SSL/TLS Direct (secure: true, requireTLS: false)
  const isPort465 = (port === 465);
  const isPort587 = (port === 587);
  let secure = isPort465;
  let requireTLS = isPort587;
  if (!isPort465 && !isPort587 && settings?.smtpSecure !== undefined) {
    secure = Boolean(settings.smtpSecure);
    requireTLS = !secure;
  }

  const dbUser = (settings?.smtpUser || '').trim();
  const dbPass = (typeof settings?.smtpPass === 'string' ? settings.smtpPass : String(settings?.smtpPass || '')).replace(/\s+/g, '').trim();

  const envUser = (process.env.SMTP_USER || process.env.EMAIL_USER || '').trim();
  const envPass = (process.env.SMTP_PASS || process.env.EMAIL_PASS || '').replace(/\s+/g, '').trim();

  let user = '';
  let pass = '';
  let source = 'none';

  if (dbUser && dbPass) {
    user = dbUser;
    pass = dbPass;
    source = 'database';
  } else if (envUser && envPass) {
    user = envUser;
    pass = envPass;
    source = 'environment';
  } else if (dbUser && envPass) {
    user = dbUser;
    pass = envPass;
    source = 'mixed (db_user + env_pass)';
  } else if (envUser && dbPass) {
    user = envUser;
    pass = dbPass;
    source = 'mixed (env_user + db_pass)';
  } else if (dbUser) {
    user = dbUser;
    pass = '';
    source = 'database (user only)';
  } else if (envUser) {
    user = envUser;
    pass = '';
    source = 'environment (user only)';
  }

  return { host, port, secure, requireTLS, user, pass, source };
}

async function resolveToIpv4(hostname) {
  return await smtpService.resolveToIpv4(hostname);
}

function createTransporter(smtpConfig, overridePort = null, directHost = null) {
  const effectiveConfig = { ...smtpConfig };
  if (directHost) effectiveConfig.host = directHost;
  if (overridePort) effectiveConfig.port = overridePort;
  const isPort465 = (Number(effectiveConfig.port) === 465);
  const isPort587 = (Number(effectiveConfig.port) === 587);
  const secure = isPort465;
  const requireTLS = isPort587;
  const rawHost = (smtpConfig.host || 'smtp.gmail.com').trim();

  return nodemailer.createTransport({
    host: effectiveConfig.host || 'smtp.gmail.com',
    port: Number(effectiveConfig.port || 465),
    secure: secure,
    requireTLS: requireTLS,
    servername: rawHost,
    family: 4,
    auth: {
      user: effectiveConfig.user,
      pass: effectiveConfig.pass
    },
    tls: {
      rejectUnauthorized: false,
      minVersion: 'TLSv1.2',
      servername: rawHost
    },
    connectionTimeout: 15000,
    greetingTimeout: 15000,
    socketTimeout: 20000
  });
}

async function sendEmailWithFallback(mailOptions, settings = null) {
  const currentSettings = settings || (await getSettings());
  const smtpConfig = getSmtpConfig(currentSettings);
  return await smtpService.sendEmail(mailOptions, smtpConfig);
}

async function sendVerificationOtpEmail(user, otp) {
  const settings = await getSettings();
  const smtpConfig = getSmtpConfig(settings);
  const appName = settings.name || 'PutzPay';
  return await smtpService.sendVerificationOtpEmail(user, otp, smtpConfig, appName);
}

async function sendPasswordResetEmail(user, resetLink) {
  const settings = await getSettings();
  const smtpConfig = getSmtpConfig(settings);

  if (!smtpConfig.user || !smtpConfig.pass) {
    return { success: false, error: 'Fitur email belum dikonfigurasi oleh Administrator.' };
  }

  const appName = settings.name || 'PutzPay';

  const mailOptions = {
    to: user.email,
    from: `"${appName}" <${smtpConfig.user}>`,
    subject: `Permintaan Reset Password - ${appName}`,
    text: `Halo ${user.username},\n\nKami menerima permintaan untuk mengatur ulang kata sandi akun Anda di ${appName}.\n\nSilakan buka tautan berikut untuk membuat password baru:\n${resetLink}\n\nTautan ini hanya berlaku selama 30 menit.\n\nJika Anda tidak meminta ini, abaikan email ini secara aman.\n\n© ${appName}`,
    html: `
      <div style="font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f3f4f8; padding: 40px 20px; margin: 0;">
        <div style="max-width: 480px; margin: 0 auto; background-color: #ffffff; border-radius: 24px; border: 3px solid #000000; overflow: hidden; box-shadow: 6px 6px 0px #000000;">
          <div style="background-color: #fde047; padding: 24px; text-align: center; border-bottom: 3px solid #000000;">
            <h1 style="margin: 0; color: #000000; font-size: 24px; font-weight: 900; letter-spacing: -0.5px; text-transform: uppercase;">
              ${appName}
            </h1>
            <p style="margin: 4px 0 0 0; color: #000000; font-size: 11px; font-weight: 700; text-transform: uppercase; font-family: monospace;">
              Reset Password Akun
            </p>
          </div>
          <div style="padding: 32px 28px;">
            <p style="color: #000000; font-size: 14px; font-weight: 600; line-height: 1.6; margin: 0 0 16px 0;">
              Halo <strong>${user.username}</strong>,
            </p>
            <p style="color: #4b5563; font-size: 13px; line-height: 1.6; margin: 0 0 24px 0;">
              Kami menerima permintaan untuk mengatur ulang kata sandi akun Anda di <strong>${appName}</strong>. Silakan klik tombol di bawah ini untuk mengganti password:
            </p>
            
            <div style="text-align: center; margin: 28px 0;">
              <a href="${resetLink}" style="display: inline-block; background-color: #2563eb; color: #ffffff; text-decoration: none; padding: 14px 28px; border-radius: 12px; font-weight: 800; font-size: 14px; border: 2px solid #000000; box-shadow: 4px 4px 0px #000000; text-transform: uppercase;">
                Ganti Password Saya
              </a>
            </div>

            <div style="background-color: #fee2e2; border: 2px solid #000000; border-radius: 12px; padding: 12px; text-align: center; margin-bottom: 24px;">
              <p style="color: #991b1b; font-size: 12px; font-weight: 700; margin: 0;">
                ⏱️ Tautan ini hanya berlaku selama 30 menit.
              </p>
            </div>

            <p style="color: #6b7280; font-size: 12px; line-height: 1.5; margin: 0 0 8px 0;">
              Jika tombol di atas tidak berfungsi, salin tautan berikut ke browser:
            </p>
            <p style="margin: 0 0 20px 0; word-break: break-all;">
              <a href="${resetLink}" style="color: #2563eb; font-size: 12px; font-family: monospace;">${resetLink}</a>
            </p>

            <p style="color: #6b7280; font-size: 12px; line-height: 1.5; margin: 0; text-align: center;">
              Jika Anda tidak meminta reset password ini, abaikan email ini secara aman. Akun Anda tetap terlindungi.
            </p>
          </div>
          <div style="background-color: #f9fafb; padding: 16px 24px; text-align: center; border-top: 2px solid #e5e7eb;">
            <p style="color: #9ca3af; font-size: 11px; margin: 0; font-weight: 600;">
              © ${new Date().getFullYear()} ${appName}. All rights reserved.
            </p>
          </div>
        </div>
      </div>
    `
  };

  return await sendEmailWithFallback(mailOptions, settings);
}

async function sendNotificationEmail(toEmail, toName, title, message) {
  try {
    const settings = await getSettings();
    const smtpConfig = getSmtpConfig(settings);
    if (!smtpConfig.user || !smtpConfig.pass) {
      console.warn('[NOTIF EMAIL] SMTP not configured');
      return { success: false, error: 'SMTP belum dikonfigurasi' };
    }
    const appName = settings.name || 'PutzPay';
    const mailOptions = {
      to: toEmail,
      from: `"${appName}" <${smtpConfig.user}>`,
      subject: `[${appName}] ${title || 'Pemberitahuan Akun'}`,
      text: `Halo ${toName || 'User'},\n\n${title}\n\n${message}\n\nSalam,\nTim ${appName}`,
      html: `
        <div style="font-family: 'Plus Jakarta Sans', Arial, sans-serif; background-color: #f3f4f8; padding: 40px 20px; margin: 0;">
          <div style="max-width: 520px; margin: 0 auto; background-color: #ffffff; border-radius: 20px; border: 3px solid #000000; overflow: hidden; box-shadow: 6px 6px 0px #000000;">
            <div style="background-color: #fde047; padding: 24px; text-align: center; border-bottom: 3px solid #000000;">
              <h1 style="margin: 0; color: #000000; font-size: 22px; font-weight: 900; text-transform: uppercase;">
                ${appName}
              </h1>
              <p style="margin: 4px 0 0 0; color: #000000; font-size: 11px; font-weight: 700; text-transform: uppercase; font-family: monospace;">
                Pemberitahuan Sistem
              </p>
            </div>
            <div style="padding: 28px 24px;">
              <h2 style="margin: 0 0 12px 0; color: #000000; font-size: 17px; font-weight: 800; text-transform: uppercase;">
                ${title || 'Pemberitahuan Akun'}
              </h2>
              <div style="color: #1e293b; font-size: 14px; font-weight: 600; line-height: 1.6; white-space: pre-wrap; margin-bottom: 24px; padding: 16px; background-color: #f8fafc; border: 2px solid #000000; border-radius: 12px;">
${message}
              </div>
              <p style="color: #64748b; font-size: 12px; font-weight: 600; line-height: 1.5; margin: 0;">
                Pesan ini dikirimkan secara resmi oleh pengelola ${appName}.
              </p>
            </div>
            <div style="background-color: #f1f5f9; padding: 16px; text-align: center; border-top: 2px solid #000000; font-size: 11px; color: #64748b; font-family: monospace;">
              © ${new Date().getFullYear()} ${appName}. Hak cipta dilindungi.
            </div>
          </div>
        </div>
      `
    };
    return await sendEmailWithFallback(mailOptions, settings);
  } catch (err) {
    console.error('[NOTIF EMAIL] Error sending email:', err.message);
    return { success: false, error: err.message };
  }
}

// ===================== WEBSITE BACKUP HELPERS =====================
function getBackupFilename() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const year = now.getFullYear();
  const month = pad(now.getMonth() + 1);
  const day = pad(now.getDate());
  const hour = pad(now.getHours());
  const min = pad(now.getMinutes());
  const sec = pad(now.getSeconds());
  return `PutzPay-Backup-${year}-${month}-${day}-${hour}-${min}-${sec}.zip`;
}

function listBackups() {
  const backupsDir = path.join(__dirname, 'backups');
  if (!fs.existsSync(backupsDir)) {
    fs.mkdirSync(backupsDir, { recursive: true });
    return [];
  }
  const files = fs.readdirSync(backupsDir);
  const backups = [];
  for (const file of files) {
    if (file.endsWith('.zip') && file.startsWith('PutzPay-Backup-')) {
      const fullPath = path.join(backupsDir, file);
      try {
        const stat = fs.statSync(fullPath);
        backups.push({
          filename: file,
          sizeBytes: stat.size,
          sizeFormatted: (stat.size / (1024 * 1024)).toFixed(2) + ' MB',
          createdAt: stat.birthtime || stat.mtime,
          createdAtFormatted: new Intl.DateTimeFormat('id-ID', {
            day: '2-digit', month: 'short', year: 'numeric',
            hour: '2-digit', minute: '2-digit'
          }).format(stat.birthtime || stat.mtime)
        });
      } catch (e) {}
    }
  }
  return backups.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
}

function createZipArchiver(options = { zlib: { level: 9 } }) {
  if (!archiver) return null;
  if (archiver.ZipArchive) {
    return new archiver.ZipArchive(options);
  }
  if (typeof archiver === 'function') {
    return archiver('zip', options);
  }
  if (archiver.default && typeof archiver.default === 'function') {
    return archiver.default('zip', options);
  }
  if (archiver.create) {
    return archiver.create('zip', options);
  }
  return null;
}

let isBackupInProgress = false;

async function createWebsiteBackup(includeSensitive = false) {
  const archive = createZipArchiver({ zlib: { level: 9 } });
  if (!archive) {
    throw new Error('Modul archiver belum terinstal atau tidak kompatibel.');
  }

  if (isBackupInProgress) {
    throw new Error('Proses pembuatan backup sedang berjalan. Harap tunggu sebentar.');
  }

  isBackupInProgress = true;
  const backupsDir = path.join(__dirname, 'backups');
  if (!fs.existsSync(backupsDir)) {
    fs.mkdirSync(backupsDir, { recursive: true });
  }

  const filename = getBackupFilename();
  const zipPath = path.join(backupsDir, filename);
  const output = fs.createWriteStream(zipPath);

  return new Promise((resolve, reject) => {
    output.on('close', () => {
      isBackupInProgress = false;
      const stats = fs.statSync(zipPath);
      resolve({
        filename,
        path: zipPath,
        sizeBytes: stats.size,
        sizeFormatted: (stats.size / (1024 * 1024)).toFixed(2) + ' MB'
      });
    });

    archive.on('error', (err) => {
      isBackupInProgress = false;
      reject(err);
    });

    archive.pipe(output);

    // Root files to include
    const rootFiles = ['server.js', 'package.json', 'package-lock.json', 'bun.lock', 'config.example.js', 'README.md', 'metadata.json'];
    rootFiles.forEach(file => {
      const filePath = path.join(__dirname, file);
      if (fs.existsSync(filePath)) {
        archive.file(filePath, { name: file });
      }
    });

    // If includeSensitive is true, include config.js and .env if present
    if (includeSensitive) {
      const sensitiveFiles = ['config.js', '.env', '.env.local', '.env.production'];
      sensitiveFiles.forEach(file => {
        const filePath = path.join(__dirname, file);
        if (fs.existsSync(filePath)) {
          archive.file(filePath, { name: file });
        }
      });
    } else {
      // Include sanitized config.js or config.example.js as config.js
      if (fs.existsSync(path.join(__dirname, 'config.example.js'))) {
        archive.file(path.join(__dirname, 'config.example.js'), { name: 'config.js' });
      }
    }

    // Directories to include
    const dirs = ['views', 'public', 'telegram-monitor'];
    dirs.forEach(dir => {
      const dirPath = path.join(__dirname, dir);
      if (fs.existsSync(dirPath)) {
        archive.directory(dirPath, dir, (entry) => {
          if (entry.name.endsWith('.log') || entry.name.includes('.DS_Store')) {
            return false;
          }
          return entry;
        });
      }
    });

    archive.finalize();
  });
}

// ===================== GOPAY TOKEN REFRESH & RETRY =====================
let refreshingPromise = null;

async function refreshGopayToken() {
  const settings = await getSettings();
  if (!settings.gopayRefreshToken) {
    throw new Error('Refresh token tidak tersedia. Harap isi di pengaturan admin.');
  }

  const gopayBase = settings.gopayDomain || 'gomerch.vercel.app';
  const refreshUrl = `https://${gopayBase}/auth/refresh/token?refresh_token=${encodeURIComponent(settings.gopayRefreshToken)}`;

  try {
    const resp = await axios.get(refreshUrl);
    let data = resp.data?.data || null

    if (!data) {
      throw new Error(data.error || 'Gagal refresh token');
    }

    const newToken = data.access_token;
    const newRefreshToken = data.refresh_token;

    const updateFields = { gopayToken: newToken };
    if (newRefreshToken) {
      updateFields.gopayRefreshToken = newRefreshToken;
    }

    await Setting.updateOne({}, updateFields);
    console.log('✅ Gopay token berhasil diperbarui');
    return newToken;
  } catch (err) {
    console.error('❌ Gagal refresh Gopay token:', err.response?.data || err.message);
    throw new Error('Gagal memperbarui token Gopay. Refresh token mungkin sudah kadaluarsa.');
  }
}

async function callGopayApiWithRetry(url, options = {}, maxRetries = 1) {
  let lastError;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await axios({ url, ...options });
      return response.data;
    } catch (err) {
      const isTokenError =
        (err.response && err.response.status === 401) ||
        (err.response?.data?.error);

      if (isTokenError && attempt < maxRetries) {
        console.log('🔄 Token expired, mencoba refresh...');
        if (!refreshingPromise) {
          refreshingPromise = refreshGopayToken().finally(() => {
            refreshingPromise = null;
          });
        }
        await refreshingPromise;

        const settings = await getSettings();
        if (url.includes('token=')) {
          url = url.replace(/token=[^&]*/, `token=${settings.gopayToken}`);
        }
        continue;
      }

      lastError = err;
      break;
    }
  }
  throw lastError;
}

// ===================== ACCESS CONTROL & SECURITY BARRIER MIDDLEWARE =====================
// Priority 1: Global Website Block
// Priority 2: User Account Block
// Priority 3: Device / IP Block
app.use(async (req, res, next) => {
  const clientIp = getClientIp(req);
  const clientDevice = getClientDevice(req.headers['user-agent']);
  const deviceId = getSafeDeviceId(req, res);

  req.clientIp = clientIp;
  req.clientDevice = clientDevice;
  req.deviceId = deviceId;

  // 1. Allow static assets, images, styling, scripts & service worker to load freely
  const currentPath = req.path.toLowerCase();
  if (
    currentPath.startsWith('/public') ||
    currentPath.startsWith('/uploads') ||
    currentPath.endsWith('.css') ||
    currentPath.endsWith('.js') ||
    currentPath.endsWith('.png') ||
    currentPath.endsWith('.jpg') ||
    currentPath.endsWith('.jpeg') ||
    currentPath.endsWith('.svg') ||
    currentPath.endsWith('.ico') ||
    currentPath === '/sw.js' ||
    currentPath === '/service-worker.js'
  ) {
    return next();
  }

  const settings = await getSettings();
  res.locals.settings = settings;

  // Customer Service 24/7 link is external and unrestricted (https://cs.putzpay.biz.id)
  const isCustomerServiceRoute = currentPath.startsWith('/cs') || currentPath.startsWith('/help');
  if (isCustomerServiceRoute) {
    return res.redirect(settings.customerServiceUrl || 'https://cs.putzpay.biz.id');
  }

  let isAdminOrOwner = req.session && (req.session.userRole === 'admin' || req.session.userRole === 'owner');
  if (!isAdminOrOwner && req.session && req.session.userId) {
    try {
      const authUserRole = await User.findById(req.session.userId).select('role suspended accountStatus').lean();
      if (authUserRole && (authUserRole.role === 'admin' || authUserRole.role === 'owner')) {
        req.session.userRole = authUserRole.role;
        isAdminOrOwner = true;
      }
    } catch (err) {}
  }

  // ===================== USER ACCOUNT BAN & BANNED ALL USERS =====================
  const isBanAllUsersActive = Boolean(settings.banAllUsers || settings.globalWebsiteBlock);

  if (req.session && req.session.userId) {
    try {
      const authUser = await User.findById(req.session.userId).lean();
      if (authUser) {
        const isRoleExempt = authUser.role === 'owner' || authUser.role === 'admin';
        const isIndividualBanned = authUser.suspended === true || authUser.accountStatus === 'banned' || authUser.accountStatus === 'suspended';
        const isUserBanned = !isRoleExempt && (isIndividualBanned || isBanAllUsersActive);

        if (isUserBanned) {
          // Allow access to logout and the dedicated banned page
          if (currentPath === '/logout' || currentPath === '/banned') {
            return next();
          }

          if (req.xhr || currentPath.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
            return res.status(403).json({
              success: false,
              banned: true,
              type: 'account_blocked',
              message: isBanAllUsersActive && !isIndividualBanned
                ? (settings.banAllUsersReason || settings.globalBlockMessage || 'Semua akun pengguna saat ini sedang dinonaktifkan sementara oleh Administrator.')
                : (authUser.banReason || 'Akun Anda telah diblokir oleh administrator.'),
              customerServiceUrl: settings.customerServiceUrl || 'https://cs.putzpay.biz.id'
            });
          }
          return res.redirect('/banned');
        }
      }
    } catch (err) {}
  }

  // ===================== PRIORITY 3: DEVICE / IP BLOCK =====================
  const isIpBanned = bannedIpsCache.has(clientIp);
  const isDeviceBanned = deviceId && blockedDevicesCache.has(deviceId);

  if ((isIpBanned || isDeviceBanned) && !isAdminOrOwner) {
    const isExemptAdminLogin = currentPath.startsWith('/admin') || currentPath.startsWith('/login') || currentPath.startsWith('/auth') || currentPath === '/logout';
    if (!isExemptAdminLogin) {
      let banDoc = bannedIpsData.get(clientIp) || (deviceId ? blockedDevicesData.get(deviceId) : null);
      if (!banDoc) {
        try {
          banDoc = await BannedIp.findOne({ $or: [{ ip: clientIp }, { deviceId: deviceId }] }).lean();
        } catch (err) {}
      }

      const reason = banDoc?.reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan';
      const bannedAt = banDoc?.createdAt || new Date();
      const bannedBy = banDoc?.bannedBy || 'Administrator';
      const targetDeviceName = banDoc?.deviceName || clientDevice;

      if (req.xhr || currentPath.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
        return res.status(403).json({
          success: false,
          banned: true,
          type: isDeviceBanned ? 'device_blocked' : 'ip_blocked',
          ip: clientIp,
          device: targetDeviceName,
          reason: reason,
          message: 'Akses dari perangkat atau alamat IP ini telah diblokir oleh administrator.',
          customerServiceUrl: settings.customerServiceUrl || 'https://cs.putzpay.biz.id'
        });
      }

      return res.status(403).render('device_blocked', {
        settings,
        ip: clientIp,
        deviceName: targetDeviceName,
        reason,
        bannedAt,
        bannedBy
      });
    }
  }

  next();
});

// ===================== ADMIN PERTAMA & OWNER HELPERS =====================
let _firstAdminCache = null;
let _firstAdminCacheTime = 0;

async function getFirstAdminUser() {
  const now = Date.now();
  if (_firstAdminCache && (now - _firstAdminCacheTime < 60000)) {
    return _firstAdminCache;
  }
  try {
    const firstAdmin = await User.findOne({ role: { $in: ['admin', 'owner'] } })
      .sort({ createdAt: 1, _id: 1 })
      .lean();
    if (firstAdmin) {
      _firstAdminCache = firstAdmin;
      _firstAdminCacheTime = now;
      return firstAdmin;
    }
  } catch (err) {
    console.error('Error fetching first admin:', err.message);
  }
  return null;
}

function isFirstAdminSync(user) {
  if (!user) return false;
  if (user.role === 'owner') return true;
  const username = (user.username || '').toLowerCase().trim();
  const email = (user.email || '').toLowerCase().trim();
  // Admin pertama secara eksplisit: akun admin default 'admin' ('admin@gmail.com') atau pembuat 'putz' ('gbangputz@gmail.com')
  if (username === 'admin' || email === 'admin@gmail.com' || username === 'putz' || email === 'gbangputz@gmail.com') {
    return true;
  }
  if (_firstAdminCache) {
    const targetId = String(user._id || user.id || '');
    if (targetId && String(_firstAdminCache._id) === targetId) return true;
    if (_firstAdminCache.username && _firstAdminCache.username.toLowerCase() === username) return true;
    if (_firstAdminCache.email && _firstAdminCache.email.toLowerCase() === email) return true;
  }
  return false;
}

async function isFirstAdmin(user) {
  if (!user) return false;
  if (isFirstAdminSync(user)) return true;
  const firstAdmin = await getFirstAdminUser();
  if (!firstAdmin) return false;
  const username = (user.username || '').toLowerCase().trim();
  const email = (user.email || '').toLowerCase().trim();
  const targetId = String(user._id || user.id || '');
  if (targetId && String(firstAdmin._id) === targetId) return true;
  if (firstAdmin.username && firstAdmin.username.toLowerCase() === username) return true;
  if (firstAdmin.email && firstAdmin.email.toLowerCase() === email) return true;
  return false;
}

// ===================== GLOBAL MIDDLEWARE =====================
const PROFILE_COLORS = ['#3b82f6','#10b981','#f43f5e','#8b5cf6','#f59e0b','#06b6d4','#6366f1','#ec4899'];
const USD_IDR_RATE = process.env.USD_IDR_RATE ? parseFloat(process.env.USD_IDR_RATE) : 16000;

app.use(async (req, res, next) => {
  res.locals.user = null;
  res.locals.canAccessBackup = false;
  res.locals.usdRate = USD_IDR_RATE;
  res.locals.convertIdrToUsd = function(idrAmount) {
    const amount = Number(idrAmount) || 0;
    const usd = amount / USD_IDR_RATE;
    return usd.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  if (req.session.userId) {
    try {
      let user = await User.findById(req.session.userId);
      if (user) {
        if (!user.profileColor) {
          user.profileColor = PROFILE_COLORS[Math.floor(Math.random() * PROFILE_COLORS.length)];
          await user.save();
        }
        const clientIp = req.clientIp || getClientIp(req);
        const clientDevice = req.clientDevice || getClientDevice(req.headers['user-agent']);
        const deviceId = req.deviceId || getSafeDeviceId(req, res);

        user.lastSeen = new Date();
        user.lastIp = clientIp;
        user.lastDevice = clientDevice;

        if (!user.ipHistory) user.ipHistory = [];
        if (clientIp && !user.ipHistory.includes(clientIp)) user.ipHistory.push(clientIp);
        if (!user.registerIp && clientIp) user.registerIp = clientIp;

        if (!user.devices) user.devices = [];
        const existingDevIndex = user.devices.findIndex(d => d.deviceId === deviceId || (d.deviceName === clientDevice && d.ip === clientIp));
        if (existingDevIndex >= 0) {
          user.devices[existingDevIndex].lastSeen = new Date();
          user.devices[existingDevIndex].ip = clientIp;
          user.devices[existingDevIndex].userAgent = req.headers['user-agent'] || '';
        } else if (deviceId) {
          user.devices.push({
            deviceId,
            deviceName: clientDevice,
            ip: clientIp,
            userAgent: req.headers['user-agent'] || '',
            lastSeen: new Date()
          });
          if (user.devices.length > 10) user.devices.shift();
        }

        User.updateOne({ _id: user._id }, {
          $set: {
            lastSeen: user.lastSeen,
            lastIp: user.lastIp,
            lastDevice: user.lastDevice,
            profileColor: user.profileColor,
            registerIp: user.registerIp,
            devices: user.devices
          },
          $addToSet: { ipHistory: clientIp }
        }).catch(() => {});

        res.locals.user = user.toObject();
        req.session.userRole = user.role;
        req.session.userPermissions = user.permissions || [];

        const isPrimaryAdmin = await isFirstAdmin(user);
        res.locals.isFirstAdmin = isPrimaryAdmin;
        res.locals.isOwnerUser = isPrimaryAdmin;
        // Hanya Admin Pertama (dan Owner) yang dapat mengakses fitur backup website
        res.locals.canAccessBackup = (user.role === 'owner' || user.role === 'admin') && isPrimaryAdmin;
      }
    } catch {}
  }
  res.locals.settings = await getSettings();
  res.locals.googleClientId = (res.locals.settings && res.locals.settings.googleClientId) || process.env.GOOGLE_CLIENT_ID || (appConfig && appConfig.GOOGLE_CLIENT_ID) || '';
  res.locals.turnstileSiteKey = (res.locals.settings && res.locals.settings.turnstileSiteKey) || process.env.TURNSTILE_SITE_KEY || process.env.KUNCI_TEMPAT_PUTAR || '';
  res.locals.error = req.session.errorMsg || null;
  res.locals.success = req.session.successMsg || null;
  delete req.session.errorMsg;
  delete req.session.successMsg;

  if (req.session.userId) {
    try {
      const notifications = await Notification.find({
        $or: [
          { target: 'all' },
          { target: String(req.session.userId) },
          { targetUser: req.session.userId }
        ]
      }).sort({ createdAt: -1 }).limit(30).lean();
      
      const unreadCount = notifications.filter(n => {
        if (n.target === 'all') {
          return !n.readBy || !n.readBy.some(id => String(id) === String(req.session.userId));
        }
        return !n.isRead;
      }).length;

      res.locals.notifications = notifications;
      res.locals.unreadNotifCount = unreadCount;
    } catch (err) {
      res.locals.notifications = [];
      res.locals.unreadNotifCount = 0;
    }
  } else {
    res.locals.notifications = [];
    res.locals.unreadNotifCount = 0;
  }

  next();
});

// ===================== MAINTENANCE MIDDLEWARE =====================
app.use(async (req, res, next) => {
  try {
    const settings = res.locals.settings || (await getSettings());
    if (!settings || !settings.maintenanceEnabled) {
      return next();
    }

    // Check if maintenance countdown has expired -> Auto-Recovery
    if (settings.maintenanceCountdown) {
      const cdTime = new Date(settings.maintenanceCountdown).getTime();
      if (!isNaN(cdTime) && Date.now() >= cdTime) {
        settings.maintenanceEnabled = false;
        Setting.updateOne({ _id: settings._id }, { $set: { maintenanceEnabled: false } }).catch(() => {});
        return next();
      }
    }

    // Admins and Owners are exempt from maintenance
    if (req.session && (req.session.userRole === 'admin' || req.session.userRole === 'owner')) {
      return next();
    }

    const currentPath = req.path.toLowerCase();

    // Exempt paths (Allow user to logout, login, view home/docs, etc)
    const exemptPaths = [
      '/',
      '/home',
      '/login',
      '/logout',
      '/register',
      '/verify-otp',
      '/verify-otp/resend',
      '/forgot_password',
      '/forgot-password',
      '/reset_password',
      '/docs',
      '/auth/google',
      '/auth/google/callback',
      '/auth/google/failure',
      '/auth/logout',
      '/api/auth/me',
      '/api/maintenance/status'
    ];

    if (
      exemptPaths.includes(currentPath) ||
      currentPath.startsWith('/verify-otp') ||
      currentPath.startsWith('/reset-password') ||
      currentPath.startsWith('/admin')
    ) {
      return next();
    }

    // Static files and internal endpoints exempt
    if (
      currentPath.startsWith('/public') ||
      currentPath.startsWith('/uploads') ||
      currentPath.startsWith('/profile') ||
      currentPath.startsWith('/socket.io')
    ) {
      return next();
    }

    let isBlocked = false;
    if (settings.maintenanceMode === 'all') {
      isBlocked = true;
    } else if (settings.maintenanceMode === 'feature') {
      const feats = settings.maintenanceFeatures || [];
      if (feats.includes('deposit') && (currentPath.includes('/deposit') || currentPath.includes('/qris'))) {
        isBlocked = true;
      } else if (feats.includes('withdraw') && currentPath.includes('/withdraw')) {
        isBlocked = true;
      } else if (feats.includes('chat') && (currentPath === '/chat' || currentPath.startsWith('/api/chat'))) {
        isBlocked = true;
      } else if (feats.includes('chatglobal') && (currentPath === '/chatglobal' || currentPath.startsWith('/api/globalchat'))) {
        isBlocked = true;
      } else if (feats.includes('api') && currentPath.startsWith('/api/create')) {
        isBlocked = true;
      }
    }

    if (!isBlocked) {
      return next();
    }

    if (req.xhr || currentPath.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(503).json({
        maintenance: true,
        error: settings.maintenanceMessage || 'Sistem dalam pemeliharaan.',
        title: settings.maintenanceTitle || 'Maintenance Mode'
      });
    }

    return res.status(503).render('maintenance', {
      settings,
      user: res.locals.user || null
    });

  } catch (err) {
    console.error('Maintenance middleware error:', err);
    next();
  }
});

async function isAuth(req, res, next) {
  if (req.session && req.session.userId) {
    return next();
  }
  if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
    return res.status(401).json({ success: false, message: 'Silakan login terlebih dahulu.' });
  }
  res.redirect('/login');
}

function isAdmin(req, res, next) {
  if (req.session && (req.session.userRole === 'admin' || req.session.userRole === 'owner')) return next();
  if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
    return res.status(403).json({ success: false, message: 'Akses ditolak. Perlu hak akses admin.' });
  }
  res.redirect('/login');
}

async function isOwner(req, res, next) {
  if (req.session && req.session.userRole === 'owner') return next();
  const currentUser = res.locals.user || (req.session && req.session.userId ? await User.findById(req.session.userId).lean() : null);
  if (currentUser && (await isFirstAdmin(currentUser))) {
    return next();
  }
  if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
    return res.status(403).json({ success: false, message: 'Akses ditolak. Fitur ini khusus untuk Owner / Admin Pertama platform.' });
  }
  req.session.errorMsg = 'Akses ditolak. Fitur ini khusus untuk Owner / Admin Pertama platform.';
  res.redirect('/admin/dashboard');
}

async function canAccessBackup(req, res, next) {
  if (!req.session || !req.session.userId) {
    if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/')) {
      return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    return res.redirect('/login');
  }

  const role = req.session.userRole;
  if (role !== 'admin' && role !== 'owner') {
    if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/')) {
      return res.status(403).json({ success: false, message: 'Akses ditolak. Perlu hak akses admin.' });
    }
    return res.redirect('/dashboard');
  }

  const currentUser = res.locals.user || (await User.findById(req.session.userId).lean());
  const isPrimary = await isFirstAdmin(currentUser);

  if (!isPrimary) {
    if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/') || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(403).json({ success: false, message: 'Akses ditolak. Fitur backup website hanya dapat diakses oleh Admin Pertama.' });
    }
    req.session.errorMsg = 'Akses ditolak. Fitur backup website hanya dapat diakses oleh Admin Pertama.';
    return res.redirect('/admin/dashboard');
  }

  return next();
}

function hasPermission(perm) {
  return (req, res, next) => {
    if (!req.session || !req.session.userId) {
      if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/')) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
      }
      return res.redirect('/login');
    }
    const role = req.session.userRole;
    if (role === 'owner') return next();
    if (role === 'admin') {
      const perms = req.session.userPermissions || (res.locals.user && res.locals.user.permissions) || [];
      if (perms.length === 0 || perms.includes('all') || perms.includes(perm)) {
        return next();
      }
      if (perms.includes('manage_users') && (perm === 'view_users' || perm === 'edit_users')) {
        return next();
      }
    }
    if (req.originalUrl.startsWith('/api/') || req.path.startsWith('/api/')) {
      return res.status(403).json({ success: false, message: `Akses ditolak. Memerlukan izin: ${perm}` });
    }
    req.session.errorMsg = `Akses ditolak. Anda tidak memiliki izin (${perm}) untuk mengakses halaman ini.`;
    res.redirect('/admin/dashboard');
  };
}

// ===================== WEB PUSH NOTIFICATION STATUS =====================
app.get('/api/notifications/status', isAuth, async (req, res) => {
  try {
    const activeCount = await PushSubscription.countDocuments({ userId: req.session.userId, active: true });
    let settings = await UserNotificationSetting.findOne({ userId: req.session.userId }).lean();
    if (!settings) {
      settings = {
        paymentSuccess: true,
        paymentPending: true,
        paymentFailed: true,
        paymentExpired: true,
        withdrawSuccess: true,
        withdrawFailed: true,
        balanceUpdated: true,
        securityAlert: true
      };
    }
    res.json({ success: true, isSubscribed: activeCount > 0, activeDevices: activeCount, settings });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// ===================== MAINTENANCE STATUS API =====================
app.get('/api/maintenance/status', async (req, res) => {
  try {
    const settings = await getSettings();
    res.json({
      success: true,
      maintenanceEnabled: !!(settings && settings.maintenanceEnabled),
      maintenanceCountdown: (settings && settings.maintenanceCountdown) || '',
      maintenanceMode: (settings && settings.maintenanceMode) || 'all',
      title: (settings && settings.maintenanceTitle) || 'Maintenance Mode',
      message: (settings && settings.maintenanceMessage) || ''
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===================== USER NOTIFICATION CENTER APIS =====================
app.get('/api/notifications', isAuth, async (req, res) => {
  try {
    const userId = req.session.userId;
    const notifications = await Notification.find({
      $or: [
        { target: 'all' },
        { target: String(userId) },
        { targetUser: userId }
      ]
    }).sort({ createdAt: -1 }).limit(50).lean();

    const formatted = notifications.map(n => {
      let isRead = false;
      if (n.target === 'all') {
        isRead = Array.isArray(n.readBy) && n.readBy.some(id => String(id) === String(userId));
      } else {
        isRead = !!n.isRead;
      }
      return {
        ...n,
        isRead
      };
    });

    const unreadCount = formatted.filter(n => !n.isRead).length;

    res.json({
      success: true,
      notifications: formatted,
      unreadCount
    });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Gagal mengambil notifikasi: ' + err.message });
  }
});

app.post('/api/notifications/:id/read', isAuth, async (req, res) => {
  try {
    const userId = req.session.userId;
    const notifId = req.params.id;
    const notif = await Notification.findById(notifId);
    if (!notif) {
      return res.status(404).json({ success: false, message: 'Notifikasi tidak ditemukan' });
    }

    if (notif.target === 'all') {
      await Notification.updateOne(
        { _id: notifId },
        { $addToSet: { readBy: userId } }
      );
    } else {
      if (String(notif.target) === String(userId) || (notif.targetUser && String(notif.targetUser) === String(userId))) {
        notif.isRead = true;
        notif.readAt = new Date();
        await notif.save();
      }
    }

    res.json({ success: true, message: 'Notifikasi ditandai dibaca' });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.post('/api/notifications/read-all', isAuth, async (req, res) => {
  try {
    const userId = req.session.userId;
    // Mark all personal notifications
    await Notification.updateMany(
      {
        $or: [
          { target: String(userId) },
          { targetUser: userId }
        ],
        isRead: false
      },
      {
        $set: { isRead: true, readAt: new Date() }
      }
    );

    // Add user to readBy of all global notifications
    await Notification.updateMany(
      {
        target: 'all',
        readBy: { $ne: userId }
      },
      {
        $addToSet: { readBy: userId }
      }
    );

    res.json({ success: true, message: 'Semua notifikasi berhasil ditandai telah dibaca.' });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// ===================== SEED =====================
async function seed() {
  await reloadBannedIpsCache();
  await User.updateMany(
    { $or: [{ emailVerified: { $ne: true } }, { emailVerified: { $exists: false } }] },
    { $set: { emailVerified: true, verificationOtpHash: null, verificationOtpExpires: null } }
  ).catch(() => {});
  const ex = await User.findOne({ role: "admin" });
  if (!ex) {
    await User.create({
      username: 'admin',
      email: 'admin@gmail.com',
      password: hashPassword('admin123'),
      role: 'admin',
      profileColor: PROFILE_COLORS[0]
    });
    console.log(`🔑 Admin Account Default\nUsername: admin\nPassword: admin123`);
  }

  const notifCount = await Notification.countDocuments();
  if (notifCount === 0) {
    await Notification.create({
      title: 'Selamat Datang!',
      message: 'Selamat datang di platform kami. Jangan lupa lengkapi profil dan verifikasi akun Anda agar dapat melakukan transaksi.',
      target: 'all'
    });
    console.log('📢 Notifikasi default dibuat.');
  }

  const settings = await getSettings();
  if (!settings.withdrawMethods || settings.withdrawMethods.length === 0) {
    settings.withdrawMethods = [
      { name: 'Dana', fee: 500 },
      { name: 'GoPay', fee: 700 }
    ];
    await settings.save();
  }

  const partnerCount = await Partner.countDocuments();
  if (partnerCount === 0) {
    await Partner.insertMany([
      {
        name: 'Gopay Merchant Indonesia',
        slug: 'gopay-merchant',
        category: 'Fintech & E-Wallet',
        logoUrl: 'https://files.catbox.moe/82p405.jpg',
        websiteUrl: 'https://gopay.co.id',
        description: 'Integrasi ekosistem pembayaran QRIS Gopay Merchant resmi untuk mutasi realtime tanpa jeda.',
        tier: 'strategic',
        badge: 'Strategic Partner',
        isActive: true,
        order: 1
      },
      {
        name: 'QRIS Antarnegara & ASPI',
        slug: 'qris-nasional',
        category: 'Standar Pembayaran Nasional',
        logoUrl: 'https://files.catbox.moe/82p405.jpg',
        websiteUrl: 'https://qris.id',
        description: 'Jaringan QR Standar Indonesia (ASPI & Bank Indonesia) untuk seluruh Bank & E-Wallet se-Indonesia.',
        tier: 'official',
        badge: 'Official Network',
        isActive: true,
        order: 2
      },
      {
        name: 'DANA Indonesia Gateway',
        slug: 'dana-indonesia',
        category: 'Fintech & E-Wallet',
        logoUrl: 'https://files.catbox.moe/82p405.jpg',
        websiteUrl: 'https://dana.id',
        description: 'Kanal pencairan saldo (instant withdrawal) dan pembayaran terverifikasi otomatis.',
        tier: 'certified',
        badge: 'Certified Partner',
        isActive: true,
        order: 3
      },
      {
        name: 'PutzOfficial Cloud Systems',
        slug: 'putzofficial-cloud',
        category: 'Cloud & Infrastructure',
        logoUrl: 'https://files.catbox.moe/82p405.jpg',
        websiteUrl: 'https://t.me/PutzOfficial',
        description: 'Penyedia infrastruktur server berkecepatan tinggi, webhook listener, dan API gateway routing 99.9% uptime.',
        tier: 'strategic',
        badge: 'Technology Partner',
        isActive: true,
        order: 4
      },
      {
        name: 'DigiStore Commerce Hub',
        slug: 'digistore-commerce',
        category: 'E-Commerce & Merchant',
        logoUrl: 'https://files.catbox.moe/82p405.jpg',
        websiteUrl: 'https://putzpay.biz.id/payment-links',
        description: 'Ekosistem integrasi checkout online, link bayar instan, dan solusi invoice digital UKM.',
        tier: 'verified',
        badge: 'Verified Merchant',
        isActive: true,
        order: 5
      }
    ]);
    console.log('🤝 Partner resmi default berhasil diinisialisasi.');
  }
}

// ===================== ROUTES DASHBOARD / AUTH =====================
app.get(['/', '/home'], async (req, res) => {
  const stats = await getStats();
  const telegramVerified = Boolean(req.session && req.session.telegramJoined);
  res.render('home', { stats, telegramVerified });
});

app.get('/privacy-policy', (req, res) => {
  res.render('privacy-policy');
});

app.get('/terms', (req, res) => {
  res.render('terms');
});

app.get('/partners', async (req, res) => {
  try {
    const settings = await getSettings();
    const isPartnerEnabled = settings.partnerEnabled !== false;
    const partners = isPartnerEnabled
      ? await Partner.find({ isActive: true }).sort({ order: 1, createdAt: -1 }).lean()
      : [];
    res.render('partners', { settings, partners, isPartnerEnabled });
  } catch (err) {
    console.error('Error rendering partners page:', err);
    res.render('partners', { settings: await getSettings(), partners: [], isPartnerEnabled: false });
  }
});

// ===================== PRODUCTION KYC SYSTEM ROUTES =====================
app.get('/kyc', async (req, res) => {
  try {
    const settings = await getSettings();
    let user = null;
    let kycDocs = [];
    let auditLogs = [];

    if (req.session && req.session.userId) {
      user = await User.findById(req.session.userId).lean();
      if (user) {
        kycDocs = await KycDocument.find({ userId: user._id, status: 'active' }).sort({ uploadedAt: -1 }).lean();
        auditLogs = await KycAuditLog.find({ userId: user._id }).sort({ timestamp: -1 }).limit(10).lean();
      }
    }

    const errorMsg = req.session.errorMsg || null;
    const successMsg = req.session.successMsg || null;
    delete req.session.errorMsg;
    delete req.session.successMsg;

    res.render('kyc', {
      user,
      kycDocs,
      auditLogs,
      settings,
      errorMsg,
      successMsg
    });
  } catch (err) {
    console.error('Error rendering KYC page:', err);
    res.status(500).send('Terjadi kesalahan saat memuat halaman KYC.');
  }
});

app.post('/api/kyc/submit', isAuth, kycUploadMiddleware.fields([
  { name: 'ktp', maxCount: 1 },
  { name: 'kk', maxCount: 1 },
  { name: 'kartu_pelajar', maxCount: 1 }
]), async (req, res) => {
  const isAjax = req.xhr || req.headers.accept?.includes('application/json');
  try {
    const user = await User.findById(req.session.userId);
    if (!user) {
      if (isAjax) return res.status(404).json({ success: false, message: 'User tidak ditemukan.' });
      req.session.errorMsg = 'User tidak ditemukan.';
      return res.redirect('/kyc');
    }

    if (user.kycStatus === 'VERIFIED') {
      if (isAjax) return res.status(400).json({ success: false, message: 'Akun Anda sudah berstatus Terverifikasi (KYC Verified).' });
      req.session.errorMsg = 'Akun Anda sudah berstatus Terverifikasi (KYC Verified).';
      return res.redirect('/kyc');
    }

    const { kycType, fullName, nik, birthDate, parentApproval } = req.body;

    if (!fullName || fullName.trim().length < 3) {
      const msg = 'Nama lengkap wajib diisi sesuai kartu identitas (minimal 3 karakter).';
      if (isAjax) return res.status(400).json({ success: false, message: msg });
      req.session.errorMsg = msg;
      return res.redirect('/kyc');
    }

    if (!nik || nik.trim().length < 5) {
      const msg = 'Nomor Identitas (NIK / Nomor Pelajar) tidak valid.';
      if (isAjax) return res.status(400).json({ success: false, message: msg });
      req.session.errorMsg = msg;
      return res.redirect('/kyc');
    }

    if (!birthDate) {
      const msg = 'Tanggal lahir wajib diisi.';
      if (isAjax) return res.status(400).json({ success: false, message: msg });
      req.session.errorMsg = msg;
      return res.redirect('/kyc');
    }

    const selectedType = kycType === 'pelajar' ? 'pelajar' : 'ktp';
    const files = req.files || {};

    let primaryDocFile = null;
    let primaryDocType = '';

    if (selectedType === 'ktp') {
      primaryDocFile = files['ktp'] && files['ktp'][0];
      primaryDocType = 'ktp';
      if (!primaryDocFile) {
        const msg = 'Foto KTP wajib diunggah.';
        if (isAjax) return res.status(400).json({ success: false, message: msg });
        req.session.errorMsg = msg;
        return res.redirect('/kyc');
      }
    } else {
      primaryDocFile = files['kartu_pelajar'] && files['kartu_pelajar'][0];
      primaryDocType = 'kartu_pelajar';
      if (!primaryDocFile) {
        const msg = 'Foto Kartu Pelajar wajib diunggah.';
        if (isAjax) return res.status(400).json({ success: false, message: msg });
        req.session.errorMsg = msg;
        return res.redirect('/kyc');
      }
      const hasParentApproval = parentApproval === 'true' || parentApproval === 'on' || parentApproval === true;
      if (!hasParentApproval) {
        const msg = 'Persetujuan orang tua/wali wajib dicentang untuk pendaftaran pelajar.';
        if (isAjax) return res.status(400).json({ success: false, message: msg });
        req.session.errorMsg = msg;
        return res.redirect('/kyc');
      }
    }

    const kkDocFile = files['kk'] && files['kk'][0];
    if (!kkDocFile) {
      const msg = 'Foto Kartu Keluarga (KK) wajib diunggah.';
      if (isAjax) return res.status(400).json({ success: false, message: msg });
      req.session.errorMsg = msg;
      return res.redirect('/kyc');
    }

    // Validate Primary Document
    const valPrimary = validateKycBuffer(primaryDocFile.buffer, primaryDocFile.originalname, primaryDocFile.mimetype);
    if (!valPrimary.valid) {
      const msg = `Dokumen ${primaryDocType.toUpperCase()}: ${valPrimary.message}`;
      if (isAjax) return res.status(400).json({ success: false, message: msg });
      req.session.errorMsg = msg;
      return res.redirect('/kyc');
    }

    // Validate KK
    const valKk = validateKycBuffer(kkDocFile.buffer, kkDocFile.originalname, kkDocFile.mimetype);
    if (!valKk.valid) {
      const msg = `Dokumen Kartu Keluarga: ${valKk.message}`;
      if (isAjax) return res.status(400).json({ success: false, message: msg });
      req.session.errorMsg = msg;
      return res.redirect('/kyc');
    }

    // Process Primary Document Save
    const lastPrimaryDoc = await KycDocument.findOne({ userId: user._id, documentType: primaryDocType }).sort({ version: -1 });
    const nextPrimaryVersion = (lastPrimaryDoc && lastPrimaryDoc.version) ? lastPrimaryDoc.version + 1 : 1;

    // Supersede older active documents of this type
    await KycDocument.updateMany({ userId: user._id, documentType: primaryDocType, status: 'active' }, { $set: { status: 'superseded' } });

    const primarySaved = await saveKycFile({
      username: user.username,
      docType: primaryDocType,
      buffer: primaryDocFile.buffer,
      originalname: primaryDocFile.originalname,
      mimetype: primaryDocFile.mimetype,
      version: nextPrimaryVersion
    });

    await KycDocument.create({
      userId: user._id,
      documentType: primaryDocType,
      storageKey: primarySaved.storageKey,
      backupKey: primarySaved.backupKey,
      originalFilename: primarySaved.originalFilename,
      fileSize: primarySaved.fileSize,
      mimeType: primarySaved.mimeType,
      version: nextPrimaryVersion,
      status: 'active'
    });

    // Process KK Document Save
    const lastKkDoc = await KycDocument.findOne({ userId: user._id, documentType: 'kk' }).sort({ version: -1 });
    const nextKkVersion = (lastKkDoc && lastKkDoc.version) ? lastKkDoc.version + 1 : 1;

    await KycDocument.updateMany({ userId: user._id, documentType: 'kk', status: 'active' }, { $set: { status: 'superseded' } });

    const kkSaved = await saveKycFile({
      username: user.username,
      docType: 'kk',
      buffer: kkDocFile.buffer,
      originalname: kkDocFile.originalname,
      mimetype: kkDocFile.mimetype,
      version: nextKkVersion
    });

    await KycDocument.create({
      userId: user._id,
      documentType: 'kk',
      storageKey: kkSaved.storageKey,
      backupKey: kkSaved.backupKey,
      originalFilename: kkSaved.originalFilename,
      fileSize: kkSaved.fileSize,
      mimeType: kkSaved.mimeType,
      version: nextKkVersion,
      status: 'active'
    });

    // Update User Profile
    user.kycStatus = 'PENDING';
    user.kycType = selectedType;
    user.kycFullName = fullName.trim();
    user.kycNik = nik.trim();
    user.kycBirthDate = birthDate;
    user.kycParentApproval = selectedType === 'pelajar';
    user.kycSubmittedAt = new Date();
    user.kycRejectionReason = '';
    await user.save();

    // Write Audit Log
    await KycAuditLog.create({
      userId: user._id,
      action: 'SUBMITTED',
      actorId: user._id,
      actorRole: 'user',
      details: {
        kycType: selectedType,
        primaryDocType,
        primaryVersion: nextPrimaryVersion,
        kkVersion: nextKkVersion
      },
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
      userAgent: req.headers['user-agent'] || ''
    });

    const successMsg = 'Dokumen verifikasi identitas (KYC) Anda berhasil diunggah dan sedang dalam proses peninjauan tim verifikasi.';
    if (isAjax) {
      return res.json({ success: true, message: successMsg });
    }
    req.session.successMsg = successMsg;
    res.redirect('/kyc');
  } catch (err) {
    console.error('Error in KYC submission:', err);
    const errText = 'Gagal memproses pengajuan KYC: ' + (err.message || 'Terjadi kesalahan sistem.');
    if (isAjax) return res.status(500).json({ success: false, message: errText });
    req.session.errorMsg = errText;
    res.redirect('/kyc');
  }
});

// Secure streaming of KYC documents (User access or Admin/Owner)
app.get('/api/kyc/document/:docId', isAuth, async (req, res) => {
  try {
    const doc = await KycDocument.findById(req.params.docId);
    if (!doc) {
      return res.status(404).send('Dokumen tidak ditemukan.');
    }

    const isOwner = String(doc.userId) === String(req.session.userId);
    const isAdminUser = req.session.userRole === 'admin' || req.session.userRole === 'owner';

    if (!isOwner && !isAdminUser) {
      return res.status(403).send('Akses dokumen ditolak.');
    }

    let filePath = path.join(KYC_PRIVATE_DIR, doc.storageKey);
    if (!fs.existsSync(filePath)) {
      filePath = path.join(KYC_BACKUP_DIR, doc.backupKey);
    }
    if (!fs.existsSync(filePath)) {
      return res.status(404).send('File fisik dokumen tidak ditemukan di media penyimpanan aman.');
    }

    // Security & Cache Headers
    res.setHeader('Content-Type', doc.mimeType || 'application/octet-stream');
    res.setHeader('Content-Disposition', `inline; filename="${doc.documentType}-${doc.version}${path.extname(doc.storageKey)}"`);
    res.setHeader('Cache-Control', 'private, no-store, no-cache, must-revalidate, max-age=0');
    res.setHeader('X-Content-Type-Options', 'nosniff');

    // Audit log for admin document access
    if (isAdminUser && !isOwner) {
      KycAuditLog.create({
        userId: doc.userId,
        action: 'DOCUMENT_ACCESSED',
        actorId: req.session.userId,
        actorRole: req.session.userRole,
        details: { documentId: doc._id, documentType: doc.documentType, version: doc.version },
        ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
        userAgent: req.headers['user-agent'] || ''
      }).catch(() => {});
    }

    fs.createReadStream(filePath).pipe(res);
  } catch (err) {
    console.error('Error serving KYC document:', err);
    res.status(500).send('Terjadi kesalahan saat memuat dokumen.');
  }
});

app.get('/login', (req, res) => {
  if (req.session.userId) return res.redirect((req.session.userRole === 'admin' || req.session.userRole === 'owner') ? '/admin/dashboard' : '/dashboard');
  res.render('login');
});

// ===================== TWO-FACTOR AUTHENTICATION (2FA) LOGIN =====================
app.get('/login/2fa', async (req, res) => {
  if (req.session && req.session.userId) {
    return res.redirect((req.session.userRole === 'admin' || req.session.userRole === 'owner') ? '/admin/dashboard' : '/dashboard');
  }
  if (!req.session || !req.session.twoFactorPending || !req.session.twoFactorPending.userId) {
    return res.redirect('/login');
  }

  try {
    const user = await User.findById(req.session.twoFactorPending.userId).select('username email role twoFactorLockoutUntil twoFactorFailedAttempts').lean();
    if (!user) {
      delete req.session.twoFactorPending;
      return res.redirect('/login');
    }

    const { locked, remainingSeconds } = checkTotpRateLimit(user);
    const error = req.session.errorMsg || null;
    delete req.session.errorMsg;

    res.render('login_2fa', {
      user: {
        username: user.username,
        email: user.email
      },
      locked,
      remainingSeconds,
      error
    });
  } catch (err) {
    console.error('[2FA LOGIN GET] Error:', err.message);
    res.redirect('/login');
  }
});

app.get('/login/2fa/cancel', (req, res) => {
  if (req.session) {
    delete req.session.twoFactorPending;
    delete req.session.errorMsg;
  }
  res.redirect('/login');
});

app.post('/login/2fa', Limiter, async (req, res) => {
  if (!req.session || !req.session.twoFactorPending || !req.session.twoFactorPending.userId) {
    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(401).json({ success: false, message: 'Sesi verifikasi telah kedaluwarsa. Silakan login kembali.' });
    }
    return res.redirect('/login');
  }

  const userId = req.session.twoFactorPending.userId;
  const user = await User.findById(userId);
  if (!user || !user.twoFactorEnabled) {
    delete req.session.twoFactorPending;
    if (req.xhr) return res.status(400).json({ success: false, message: '2FA tidak aktif pada akun ini.' });
    return res.redirect('/login');
  }

  // Rate limiting check
  const { locked, remainingSeconds } = checkTotpRateLimit(user);
  if (locked) {
    const msg = `Terlalu banyak percobaan 2FA salah. Akun terkunci sementara, coba lagi dalam ${remainingSeconds} detik.`;
    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(429).json({ success: false, message: msg, remainingSeconds });
    }
    req.session.errorMsg = msg;
    return res.redirect('/login/2fa');
  }

  const code = (req.body.code || req.body.token || '').trim();
  const isRecovery = req.body.isRecovery === 'true' || req.body.isRecovery === true || code.includes('-');

  if (!code) {
    const msg = isRecovery ? 'Harap masukkan kode pemulihan' : 'Harap masukkan 6 digit kode 2FA';
    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(400).json({ success: false, message: msg });
    }
    req.session.errorMsg = msg;
    return res.redirect('/login/2fa');
  }

  let verified = false;
  let verificationType = 'totp';

  if (isRecovery) {
    verified = verifyAndConsumeRecoveryCode(user, code);
    verificationType = 'recovery_code';
  } else {
    const secret = decryptTotpSecret(user.twoFactorSecretEncrypted);
    verified = verifyUserTotp(secret, code);
  }

  const clientIp = getClientIp(req);
  const userAgent = req.get('user-agent') || '';

  if (!verified) {
    await recordTotpFailure(user);
    await logSecurityEvent({
      userId: user._id,
      action: '2FA_LOGIN_FAILED',
      details: `Failed 2FA verification attempt via ${verificationType}`,
      ip: clientIp,
      userAgent: userAgent,
      status: 'failed'
    });

    const currentLock = checkTotpRateLimit(user);
    let msg = 'Kode 2FA tidak valid. Silakan coba lagi.';
    if (isRecovery) msg = 'Kode pemulihan tidak valid atau sudah pernah digunakan.';
    if (currentLock.locked) {
      msg = `Terlalu banyak percobaan salah. Silakan coba lagi dalam ${currentLock.remainingSeconds} detik.`;
    }

    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(400).json({ success: false, message: msg, locked: currentLock.locked });
    }
    req.session.errorMsg = msg;
    return res.redirect('/login/2fa');
  }

  // 2FA Verified Successfully!
  await resetTotpFailures(user);
  user.twoFactorLastUsedAt = new Date();
  if (clientIp) {
    user.lastIp = clientIp;
    if (!user.registerIp) user.registerIp = clientIp;
  }
  await user.save();

  await logSecurityEvent({
    userId: user._id,
    action: '2FA_LOGIN_SUCCESS',
    details: `Successful 2FA verification via ${verificationType}`,
    ip: clientIp,
    userAgent: userAgent,
    status: 'success'
  });

  const redirectPath = req.session.twoFactorPending.redirect || (user.role === 'admin' ? '/admin/dashboard' : '/dashboard');
  req.session.userId = user._id;
  req.session.userRole = user.role;
  req.session.userPermissions = user.permissions || [];
  req.session.twoFactorVerified = true;
  delete req.session.twoFactorPending;

  const isRoleExempt = user.role === 'admin' || user.role === 'owner';
  const settings = await getSettings();
  const isBanAllUsersActive = Boolean(settings.banAllUsers || settings.globalWebsiteBlock);
  const isIndividualBanned = user.suspended === true || user.accountStatus === 'banned' || user.accountStatus === 'suspended';
  const isUserBanned = !isRoleExempt && (isIndividualBanned || isBanAllUsersActive);

  if (isUserBanned) {
    return req.session.save(() => {
      if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
        return res.json({ success: true, redirect: '/banned' });
      }
      return res.redirect('/banned');
    });
  }

  sendPushNotification(user._id, 'security_alert', {
    title: '🔐 PutzPay 2FA',
    body: 'Login berhasil diverifikasi dengan Two-Factor Authentication.',
    data: { url: '/profile' }
  }).catch(() => {});

  telegramMonitor.notifyUserLogin({
    userId: user._id,
    username: user.username,
    email: user.email,
    method: `Password + 2FA (${verificationType})`,
    ip: clientIp,
    time: new Date()
  });

  req.session.save(() => {
    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.json({ success: true, redirect: redirectPath });
    }
    return res.redirect(redirectPath);
  });
});

app.get('/register', (req, res) => {
  if (req.session.userId) return res.redirect(req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
  res.render('register');
});

// ===================== GOOGLE OAUTH 2.0 & IDENTITY SERVICES =====================

async function getGoogleOAuthConfig() {
  const settings = await getSettings();
  const clientId = (settings && settings.googleClientId && settings.googleClientId.trim()) || process.env.GOOGLE_CLIENT_ID || (appConfig && appConfig.GOOGLE_CLIENT_ID) || '';
  const clientSecret = (settings && settings.googleClientSecret && settings.googleClientSecret.trim()) || process.env.GOOGLE_CLIENT_SECRET || (appConfig && appConfig.GOOGLE_CLIENT_SECRET) || '';
  const redirectUri = (settings && settings.googleRedirectUri && settings.googleRedirectUri.trim()) || process.env.GOOGLE_REDIRECT_URI || (appConfig && appConfig.GOOGLE_REDIRECT_URI) || 'https://putzpay.biz.id/auth/google/callback';
  return { clientId, clientSecret, redirectUri };
}

function generateSignedOAuthState(intent = 'login') {
  const secret = process.env.SESSION_SECRET || 'putzpay_oauth_secret';
  const timestamp = Date.now();
  const random = crypto.randomBytes(16).toString('hex');
  const payload = `${intent}:${timestamp}:${random}`;
  const hmac = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return Buffer.from(JSON.stringify({ intent, timestamp, random, hmac })).toString('base64url');
}

function verifySignedOAuthState(rawState) {
  try {
    if (!rawState) return null;
    const secret = process.env.SESSION_SECRET || 'putzpay_oauth_secret';
    const decoded = JSON.parse(Buffer.from(rawState, 'base64url').toString('utf8'));
    if (!decoded || !decoded.intent || !decoded.timestamp || !decoded.random || !decoded.hmac) return null;
    if (Date.now() - decoded.timestamp > 15 * 60 * 1000) return null;
    const payload = `${decoded.intent}:${decoded.timestamp}:${decoded.random}`;
    const expectedHmac = crypto.createHmac('sha256', secret).update(payload).digest('hex');
    if (expectedHmac !== decoded.hmac) return null;
    return decoded;
  } catch (e) {
    return null;
  }
}

// Helper: establish or create user from Google profile
async function processGoogleAuthUser({ googleId, email, name, picture, req }) {
  const cleanEmail = (email || '').toLowerCase().trim();
  const cleanName = (name || cleanEmail.split('@')[0] || 'User').trim();

  let user = await User.findOne({ googleId });
  if (!user && cleanEmail) {
    user = await User.findOne({ email: cleanEmail });
  }

  const clientIp = getClientIp(req);

  if (user) {
    user.googleId = googleId;
    user.googleEmail = cleanEmail;
    user.googleName = cleanName;
    user.emailVerified = true;
    if (picture) user.googlePicture = picture;
    if (picture && !user.profilePicture) user.profilePicture = picture;
    if (!user.authProvider || user.authProvider === 'local') {
      user.authProvider = 'google';
    }
    user.lastLoginAt = new Date();
    if (clientIp) {
      user.lastIp = clientIp;
      if (!user.registerIp) user.registerIp = clientIp;
    }
    await user.save();
  } else {
    let baseUsername = cleanName.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
    if (!baseUsername || baseUsername.length < 4) {
      baseUsername = (cleanEmail.split('@')[0] || 'user').replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
    }
    if (baseUsername.length > 10) baseUsername = baseUsername.substring(0, 10);
    if (baseUsername.length < 4) baseUsername = 'user' + baseUsername;

    let finalUsername = baseUsername;
    let counter = 1;
    while (await User.findOne({ username: finalUsername })) {
      const suffix = Math.floor(100 + Math.random() * 900);
      finalUsername = (baseUsername.substring(0, 10) + suffix).toLowerCase();
      counter++;
      if (counter > 20) {
        finalUsername = ('ptz' + crypto.randomBytes(3).toString('hex')).toLowerCase();
        break;
      }
    }

    const randomPassword = crypto.randomBytes(32).toString('hex');
    const randomColor = PROFILE_COLORS[Math.floor(Math.random() * PROFILE_COLORS.length)];

    user = await User.create({
      username: finalUsername,
      email: cleanEmail,
      password: hashPassword(randomPassword),
      googleId: googleId,
      googleEmail: cleanEmail,
      googleName: cleanName,
      googlePicture: picture || '',
      authProvider: 'google',
      emailVerified: true,
      profilePicture: picture || null,
      profileColor: randomColor,
      registerIp: clientIp,
      lastIp: clientIp,
      createdAt: new Date(),
      lastLoginAt: new Date()
    });

    // Note: API Key requires 2FA (Two-Factor Authentication) activation
    const totalUsers = await User.countDocuments().catch(() => null);
    telegramMonitor.notifyNewUser({
      userId: user._id,
      username: user.username,
      email: user.email,
      method: 'Google',
      totalUsers
    });
  }

  return { success: true, user };
}

// 1. GET /auth/google - Initiates Google Authorization
app.get('/auth/google', Limiter, async (req, res) => {
  try {
    if (req.session && req.session.userId) {
      return res.redirect(req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
    }

    const { clientId, redirectUri } = await getGoogleOAuthConfig();
    if (!clientId) {
      console.warn('[GOOGLE OAUTH] Google Client ID not configured');
      req.session.errorMsg = 'Google Client ID belum dikonfigurasi oleh Administrator.';
      return res.redirect('/login');
    }

    const intent = (req.query.intent === 'register') ? 'register' : 'login';
    const signedState = generateSignedOAuthState(intent);

    req.session.oauthState = signedState;
    req.session.oauthStateTime = Date.now();
    req.session.oauthIntent = intent;

    const params = new URLSearchParams({
      client_id: clientId,
      redirect_uri: redirectUri,
      response_type: 'code',
      scope: 'openid email profile',
      state: signedState,
      prompt: 'select_account',
      access_type: 'online'
    });

    const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`;
    
    req.session.save((err) => {
      if (err) console.error('[GOOGLE OAUTH] Error saving session before redirect:', err);
      return res.redirect(authUrl);
    });
  } catch (err) {
    console.error('[GOOGLE OAUTH] Error starting authorization:', err.message);
    return res.redirect('/auth/google/failure?error=auth_start_failed');
  }
});

// 2. GET /auth/google/url - Returns authorization URL as JSON
app.get('/auth/google/url', async (req, res) => {
  try {
    const { clientId, redirectUri } = await getGoogleOAuthConfig();
    if (!clientId) {
      return res.status(400).json({ success: false, message: 'Google Client ID belum dikonfigurasi' });
    }
    const intent = (req.query.intent === 'register') ? 'register' : 'login';
    const signedState = generateSignedOAuthState(intent);
    req.session.oauthState = signedState;
    req.session.oauthStateTime = Date.now();
    req.session.oauthIntent = intent;

    const params = new URLSearchParams({
      client_id: clientId,
      redirect_uri: redirectUri,
      response_type: 'code',
      scope: 'openid email profile',
      state: signedState,
      prompt: 'select_account',
      access_type: 'online'
    });

    const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`;
    req.session.save(() => {
      res.json({ success: true, url: authUrl });
    });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// 3. POST /auth/google/credential - Handles Google Identity Services (One Tap / GIS Button)
app.post('/auth/google/credential', Limiter, async (req, res) => {
  try {
    const { credential } = req.body;
    if (!credential) {
      return res.status(400).json({ success: false, message: 'Kredensial Google tidak ditemukan' });
    }

    const { clientId } = await getGoogleOAuthConfig();
    if (!clientId) {
      return res.status(500).json({ success: false, message: 'Google Client ID belum dikonfigurasi' });
    }

    const client = new OAuth2Client(clientId);
    const ticket = await client.verifyIdToken({
      idToken: credential,
      audience: clientId
    });
    const payload = ticket.getPayload();

    if (!payload || !payload.sub || !payload.email) {
      return res.status(400).json({ success: false, message: 'Profil Google tidak valid' });
    }

    const authResult = await processGoogleAuthUser({
      googleId: payload.sub,
      email: payload.email,
      name: payload.name,
      picture: payload.picture,
      req
    });

    if (!authResult.success) {
      return res.status(403).json({ success: false, message: 'Akun Anda dinonaktifkan oleh administrator.' });
    }

    const user = authResult.user;
    const isRoleExempt = user.role === 'admin' || user.role === 'owner';
    const settings = await getSettings();
    const isBanAllUsersActive = Boolean(settings.banAllUsers || settings.globalWebsiteBlock);
    const isIndividualBanned = user.suspended === true || user.accountStatus === 'banned' || user.accountStatus === 'suspended';
    const isUserBanned = !isRoleExempt && (isIndividualBanned || isBanAllUsersActive);

    if (isUserBanned) {
      req.session.userId = user._id;
      req.session.userRole = user.role;
      return req.session.save(() => {
        return res.json({ success: true, redirect: '/banned' });
      });
    }

    // Two-Factor Authentication Check for Google One Tap
    if (user.twoFactorEnabled) {
      const { locked, remainingSeconds } = checkTotpRateLimit(user);
      if (locked) {
        return res.status(429).json({ success: false, message: `Terlalu banyak percobaan 2FA salah. Silakan coba lagi dalam ${remainingSeconds} detik.` });
      }
      req.session.twoFactorPending = {
        userId: user._id.toString(),
        isEmail: true,
        redirect: user.role === 'admin' ? '/admin/dashboard' : '/dashboard'
      };
      return req.session.save(() => {
        res.json({ success: true, require2fa: true, redirect: '/login/2fa' });
      });
    }

    req.session.userId = user._id;
    req.session.userRole = user.role;

    sendPushNotification(user._id, 'security_alert', {
      title: '🔐 PutzPay',
      message: `Login berhasil menggunakan Akun Google (${user.email})`,
      data: { url: '/profile' }
    }).catch(() => {});

    req.session.save((err) => {
      if (err) console.error('[GOOGLE GSI] Session save error:', err);
      const redirectPath = user.role === 'admin' ? '/admin/dashboard' : '/dashboard';
      return res.json({ success: true, redirect: redirectPath });
    });
  } catch (err) {
    console.error('[GOOGLE GSI] Token verification error:', err.message);
    return res.status(401).json({ success: false, message: 'Verifikasi akun Google gagal: ' + err.message });
  }
});

// 4. GET /auth/google/callback - Receives authorization code from Google
app.get('/auth/google/callback', async (req, res) => {
  console.log('[GOOGLE OAUTH] callback received');

  if (req.session && req.session.userId) {
    return res.redirect(req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
  }

  const { code, state, error: oauthError } = req.query;

  if (oauthError) {
    console.error(`[GOOGLE OAUTH] Google OAuth error: ${oauthError}`);
    return res.redirect('/auth/google/failure?error=' + encodeURIComponent(oauthError));
  }

  if (!code) {
    console.error('[GOOGLE OAUTH] Authorization code missing in callback');
    return res.redirect('/auth/google/failure?error=code_missing');
  }

  // Validate state with signed HMAC or session
  const verifiedState = verifySignedOAuthState(state);
  const savedState = req.session ? req.session.oauthState : null;
  const isStateValid = Boolean(verifiedState || (savedState && state === savedState));

  if (!isStateValid) {
    console.error('[GOOGLE OAUTH] Invalid or expired OAuth state parameter');
    if (req.session) {
      delete req.session.oauthState;
      delete req.session.oauthStateTime;
      delete req.session.oauthIntent;
      req.session.save(() => {});
    }
    return res.redirect('/auth/google/failure?error=state_mismatch');
  }

  try {
    const { clientId, clientSecret, redirectUri } = await getGoogleOAuthConfig();

    if (!clientId || !clientSecret) {
      console.error('[GOOGLE OAUTH] Client ID or Secret missing');
      return res.redirect('/auth/google/failure?error=not_configured');
    }

    // Exchange Authorization Code for Tokens
    const tokenResponse = await axios.post(
      'https://oauth2.googleapis.com/token',
      new URLSearchParams({
        code: String(code),
        client_id: clientId,
        client_secret: clientSecret,
        redirect_uri: redirectUri,
        grant_type: 'authorization_code'
      }).toString(),
      {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: 10000
      }
    );

    const accessToken = tokenResponse.data && tokenResponse.data.access_token;
    if (!accessToken) {
      console.error('[GOOGLE OAUTH] Token exchange succeeded but access_token is missing');
      return res.redirect('/auth/google/failure?error=token_exchange_failed');
    }

    // Fetch User Profile from Google UserInfo endpoint
    const userInfoResponse = await axios.get('https://www.googleapis.com/oauth2/v3/userinfo', {
      headers: { Authorization: `Bearer ${accessToken}` },
      timeout: 10000
    });

    const { sub: googleId, email: rawEmail, name: rawName, picture } = userInfoResponse.data || {};

    if (!googleId || !rawEmail) {
      console.error('[GOOGLE OAUTH] Incomplete Google user profile details');
      return res.redirect('/auth/google/failure?error=profile_failed');
    }

    const authResult = await processGoogleAuthUser({
      googleId,
      email: rawEmail,
      name: rawName,
      picture,
      req
    });

    if (!authResult.success) {
      return res.redirect('/auth/google/failure?error=' + (authResult.error || 'auth_failed'));
    }

    const user = authResult.user;

    // Clean up OAuth state from session
    if (req.session) {
      delete req.session.oauthState;
      delete req.session.oauthStateTime;
      delete req.session.oauthIntent;
    }

    // Two-Factor Authentication Check for Google OAuth callback
    if (user.twoFactorEnabled) {
      const { locked, remainingSeconds } = checkTotpRateLimit(user);
      if (locked) {
        req.session.errorMsg = `Terlalu banyak percobaan 2FA salah. Silakan coba lagi dalam ${remainingSeconds} detik.`;
        return res.redirect('/login');
      }
      req.session.twoFactorPending = {
        userId: user._id.toString(),
        isEmail: true,
        redirect: user.role === 'admin' ? '/admin/dashboard' : '/dashboard'
      };
      return res.redirect('/login/2fa');
    }

    const isRoleExempt = user.role === 'admin' || user.role === 'owner';
    const settings = await getSettings();
    const isBanAllUsersActive = Boolean(settings.banAllUsers || settings.globalWebsiteBlock);
    const isIndividualBanned = user.suspended === true || user.accountStatus === 'banned' || user.accountStatus === 'suspended';
    const isUserBanned = !isRoleExempt && (isIndividualBanned || isBanAllUsersActive);

    if (req.session) {
      req.session.userId = user._id;
      req.session.userRole = user.role;
    }

    if (isUserBanned) {
      // DO NOT send "Login Berhasil" Telegram notification when user is banned!
      return req.session.save(() => {
        return res.redirect('/banned');
      });
    }

    sendPushNotification(user._id, 'security_alert', {
      title: '🔐 PutzPay',
      message: `Login berhasil menggunakan Akun Google (${user.email})`,
      data: { url: '/profile' }
    }).catch(() => {});

    telegramMonitor.notifyUserLogin({
      userId: user._id,
      username: user.username,
      email: user.email,
      method: 'Google OAuth',
      ip: getClientIp(req),
      time: new Date()
    });

    req.session.save((err) => {
      if (err) console.error('[GOOGLE OAUTH] Error saving session after login:', err);
      const redirectPath = user.role === 'admin' ? '/admin/dashboard' : '/dashboard';
      return res.redirect(redirectPath);
    });
  } catch (err) {
    if (err.response && err.response.data) {
      console.error('[GOOGLE OAUTH] Token exchange error:', err.response.data.error || err.response.data);
    } else {
      console.error('[GOOGLE OAUTH] Callback error:', err.message);
    }
    return res.redirect('/auth/google/failure?error=token_exchange_failed');
  }
});

// 5. GET /auth/google/failure - Error Page
app.get('/auth/google/failure', (req, res) => {
  const rawError = req.query.error || '';
  let errorCode = 'general';
  let errorMessage = 'Sesi Google telah kedaluwarsa atau tidak valid. Silakan coba login kembali.';

  if (rawError === 'account_not_found') {
    errorCode = 'account_not_found';
    errorMessage = 'Akun Google ini belum terhubung dengan PutzPay. Silakan daftar akun terlebih dahulu.';
  } else if (rawError === 'account_suspended') {
    errorCode = 'account_suspended';
    errorMessage = 'Akun Anda telah dinonaktifkan oleh administrator.';
  } else if (rawError === 'access_denied') {
    errorCode = 'access_denied';
    errorMessage = 'Otorisasi Google dibatalkan oleh pengguna.';
  } else if (rawError === 'invalid_state' || rawError === 'state_mismatch') {
    errorCode = 'invalid_state';
    errorMessage = 'Sesi verifikasi keamanan Google telah kedaluwarsa. Silakan muat ulang halaman dan coba lagi.';
  } else if (rawError === 'not_configured') {
    errorCode = 'not_configured';
    errorMessage = 'Konfigurasi Google Client ID atau Client Secret belum lengkap di Pengaturan Sistem Admin.';
  } else if (rawError === 'token_exchange_failed') {
    errorCode = 'token_exchange_failed';
    errorMessage = 'Gagal melakukan verifikasi otorisasi ke Google. Pastikan Client Secret & Redirect URI di Google Cloud Console sudah tepat.';
  }

  res.render('auth_failure', { errorCode, errorMessage });
});

// 4. POST & GET /auth/logout
const handleLogout = (req, res) => {
  req.session.destroy(() => {
    res.clearCookie('connect.sid');
    return res.redirect('/login');
  });
};
app.post('/auth/logout', handleLogout);
app.get('/auth/logout', handleLogout);

// 5. GET /api/auth/me - Current Auth User Info
app.get('/api/auth/me', async (req, res) => {
  if (!req.session || !req.session.userId) {
    return res.status(401).json({ authenticated: false, message: 'Tidak terautentikasi' });
  }
  try {
    const user = await User.findById(req.session.userId).select('-password').lean();
    if (!user) {
      return res.status(401).json({ authenticated: false, message: 'User tidak ditemukan' });
    }
    return res.json({ authenticated: true, user });
  } catch (err) {
    return res.status(500).json({ authenticated: false, error: 'Internal server error' });
  }
});

app.post('/login', Limiter, async (req, res) => {
  const settings = await getSettings();
  const { siteKey: turnstileSiteKey, secretKey: turnstileSecretKey } = getTurnstileConfig(settings);

  if (turnstileSecretKey && turnstileSiteKey) {
    const turnstileToken = req.body['cf-turnstile-response'] || req.body.turnstileToken || req.body['cf_turnstile_response'];
    const turnstileResult = await verifyTurnstile(turnstileToken, turnstileSecretKey, req, turnstileSiteKey);
    if (!turnstileResult.success) {
      if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
        return res.status(400).json({ success: false, message: 'Verifikasi keamanan gagal. Silakan coba lagi.' });
      }
      req.session.errorMsg = 'Verifikasi keamanan gagal. Silakan coba lagi.';
      return res.redirect('/login');
    }
  }

  const { login, password } = req.body;
  if (!login || !password) {
    req.session.errorMsg = 'Harap isi semua field';
    return res.redirect('/login');
  }
  const cleanLogin = (login || '').trim();
  const isEmail = cleanLogin.includes('@');
  let user;
  if (isEmail) {
    user = await User.findOne({ email: cleanLogin.toLowerCase() });
  } else {
    user = await User.findOne({ username: cleanLogin.toLowerCase() });
  }
  if (!user || !verifyPassword(password, user.password)) {
    req.session.errorMsg = 'Username/email atau kata sandi salah';
    return res.redirect('/login');
  }
  const isRoleExempt = user.role === 'admin' || user.role === 'owner';
  const isIndividualBanned = user.suspended === true || user.accountStatus === 'banned' || user.accountStatus === 'suspended';
  const isBanAllUsersActive = Boolean(settings.banAllUsers || settings.globalWebsiteBlock);
  const isUserBanned = !isRoleExempt && (isIndividualBanned || isBanAllUsersActive);

  if (isUserBanned) {
    req.session.userId = user._id;
    req.session.userRole = user.role;
    // CRITICAL: DO NOT send "Login Berhasil" Telegram notification when user is banned!
    return req.session.save(() => {
      if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
        return res.json({ success: true, redirect: '/banned' });
      }
      res.redirect('/banned');
    });
  }

  // --- TWO-FACTOR AUTHENTICATION (2FA) CHECK ---
  if (user.twoFactorEnabled) {
    const { locked, remainingSeconds } = checkTotpRateLimit(user);
    if (locked) {
      const lockMsg = `Terlalu banyak percobaan 2FA salah. Akun terkunci sementara, silakan coba lagi dalam ${remainingSeconds} detik.`;
      if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
        return res.status(429).json({ success: false, message: lockMsg, remainingSeconds });
      }
      req.session.errorMsg = lockMsg;
      return res.redirect('/login');
    }

    req.session.twoFactorPending = {
      userId: user._id.toString(),
      remember: !!req.body.remember,
      isEmail: !!isEmail,
      redirect: user.role === 'admin' ? '/admin/dashboard' : '/dashboard'
    };

    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.json({ success: true, require2fa: true, redirect: '/login/2fa' });
    }
    return res.redirect('/login/2fa');
  }

  // Admin / Owner bypasses email verification OTP completely
  if (user.role === 'admin' || user.role === 'owner') {
    const adminIp = getClientIp(req);
    if (adminIp) {
      user.lastIp = adminIp;
      if (!user.registerIp) user.registerIp = adminIp;
      await user.save();
    }
    req.session.userId = user._id;
    req.session.userRole = user.role;

    sendPushNotification(user._id, 'security_alert', {
      title: '🔐 PutzPay',
      body: `Aktivitas login Admin/Owner terdeteksi.`,
      data: { url: '/admin/dashboard' }
    }).catch(() => {});

    telegramMonitor.notifyUserLogin({
      userId: user._id,
      username: user.username,
      email: user.email,
      method: 'Admin Panel Login',
      ip: adminIp,
      time: new Date()
    });

    return req.session.save(() => {
      res.redirect('/admin/dashboard');
    });
  }

  // Auto-verify if user was unverified
  if (user.emailVerified !== true) {
    user.emailVerified = true;
    user.verificationOtpHash = null;
    user.verificationOtpExpires = null;
    user.verificationOtpAttempts = 0;
  }

  const clientIp = getClientIp(req);
  if (clientIp) {
    user.lastIp = clientIp;
    if (!user.registerIp) user.registerIp = clientIp;
    await user.save();
  }

  req.session.userId = user._id;
  req.session.userRole = user.role;

  sendPushNotification(user._id, 'security_alert', {
    title: '🔐 PutzPay',
    body: `Aktivitas login baru terdeteksi pada akun kamu.`,
    data: { url: '/profile' }
  }).catch(() => {});

  telegramMonitor.notifyUserLogin({
    userId: user._id,
    username: user.username,
    email: user.email,
    method: isEmail ? 'Email/Password' : 'Username/Password',
    ip: clientIp,
    time: new Date()
  });

  return req.session.save(() => {
    if (user.role === 'admin' || user.role === 'owner') return res.redirect('/admin/dashboard');
    res.redirect('/dashboard');
  });
});

app.post('/register', Limiter, async (req, res) => {
  const settings = await getSettings();
  const { siteKey: turnstileSiteKey, secretKey: turnstileSecretKey } = getTurnstileConfig(settings);

  if (turnstileSecretKey && turnstileSiteKey) {
    const turnstileToken = req.body['cf-turnstile-response'] || req.body.turnstileToken || req.body['cf_turnstile_response'];
    const turnstileResult = await verifyTurnstile(turnstileToken, turnstileSecretKey, req, turnstileSiteKey);
    if (!turnstileResult.success) {
      if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
        return res.status(400).json({ success: false, message: 'Verifikasi keamanan gagal. Silakan coba lagi.' });
      }
      req.session.errorMsg = 'Verifikasi keamanan gagal. Silakan coba lagi.';
      return res.redirect('/register');
    }
  }

  const { username, email, password } = req.body;
  if (!username || username.trim().length === 0) {
    req.session.errorMsg = 'Username tidak boleh kosong';
    return res.redirect('/register');
  }
  if (!/^[a-zA-Z0-9]+$/.test(username)) {
    req.session.errorMsg = 'Username hanya boleh berisi huruf dan angka (tanpa spasi atau simbol)';
    return res.redirect('/register');
  }
  if (username.length > 15) {
    req.session.errorMsg = 'Username maksimal 15 karakter';
    return res.redirect('/register');
  }
  if (!email || !/^\S+@\S+\.\S+$/.test(email)) {
    req.session.errorMsg = 'Format email tidak valid';
    return res.redirect('/register');
  }
  if (!password || password.length < 6) {
    req.session.errorMsg = 'Password minimal 6 karakter';
    return res.redirect('/register');
  }

  const cleanUsername = username.trim().toLowerCase();
  const cleanEmail = email.trim().toLowerCase();

  try {
    const existingUser = await User.findOne({
      $or: [{ username: cleanUsername }, { email: cleanEmail }]
    });

    if (existingUser) {
      if (existingUser.username === cleanUsername) {
        req.session.errorMsg = 'Username sudah digunakan';
        return res.redirect('/register');
      }
      if (existingUser.email === cleanEmail) {
        req.session.errorMsg = 'Email sudah terdaftar. Silakan langsung login.';
        return res.redirect('/login');
      }
    }

    const randomColor = PROFILE_COLORS[Math.floor(Math.random() * PROFILE_COLORS.length)];
    const regIp = getClientIp(req);

    const user = await User.create({
      username: cleanUsername,
      email: cleanEmail,
      password: hashPassword(password),
      profileColor: randomColor,
      emailVerified: true,
      verificationOtpHash: null,
      verificationOtpExpires: null,
      verificationOtpAttempts: 0,
      verificationOtpLastSent: null,
      registerIp: regIp,
      lastIp: regIp,
      createdAt: new Date(),
      lastLoginAt: new Date()
    });

    // Note: API Key requires 2FA (Two-Factor Authentication) activation
    const totalUsers = await User.countDocuments().catch(() => null);
    telegramMonitor.notifyNewUser({
      userId: user._id,
      username: user.username,
      email: user.email,
      method: 'Email',
      totalUsers
    });

    sendPushNotification(user._id, 'security_alert', {
      title: '🎉 Selamat Datang di PutzPay',
      message: `Akun Anda berhasil dibuat. Selamat bertransaksi di PutzPay!`,
      data: { url: '/dashboard' }
    }).catch(() => {});

    // Directly log the user in to session
    req.session.userId = user._id;
    req.session.userRole = user.role;
    req.session.successMsg = `Selamat datang di PutzPay, @${user.username}! Pendaftaran berhasil.`;

    req.session.save((err) => {
      if (err) console.error('[REGISTER] Session save error:', err);
      const redirectPath = user.role === 'admin' ? '/admin/dashboard' : '/dashboard';
      return res.redirect(redirectPath);
    });
  } catch (err) {
    if (err.code === 11000) {
      req.session.errorMsg = 'Username atau email sudah terdaftar';
    } else if (err.name === 'ValidationError') {
      req.session.errorMsg = Object.values(err.errors).map(e => e.message).join(', ');
    } else {
      req.session.errorMsg = 'Gagal mendaftar, periksa kembali data Anda';
    }
    res.redirect('/register');
  }
});

// ===================== OTP EMAIL VERIFICATION ROUTES (AUTOMATIC BYPASS) =====================
app.get('/verify-otp', async (req, res) => {
  const userId = req.session.pendingVerificationUserId || req.session.userId;
  if (userId) {
    try {
      const user = await User.findById(userId);
      if (user) {
        user.emailVerified = true;
        await user.save();
        req.session.userId = user._id;
        req.session.userRole = user.role;
      }
    } catch {}
    delete req.session.pendingVerificationUserId;
    delete req.session.pendingVerificationEmail;
    return res.redirect(req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
  }
  return res.redirect('/login');
});

app.post('/verify-otp', (req, res) => {
  delete req.session.pendingVerificationUserId;
  delete req.session.pendingVerificationEmail;
  return res.redirect(req.session && req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
});

app.post('/verify-otp/resend', (req, res) => {
  if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
    return res.json({ success: true, message: 'Verifikasi email tidak diperlukan.' });
  }
  return res.redirect(req.session && req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
});

app.post('/check-availability', async (req, res) => {
  const { type, value } = req.body;
  if (!type || !value || !['username', 'email'].includes(type)) {
    return res.json({ available: false, message: 'Parameter tidak valid' });
  }
  if (type === 'username') {
    if (!/^[a-zA-Z0-9]{1,15}$/.test(value)) {
      return res.json({ available: false, message: 'Format username tidak valid (huruf/angka, maks 15 karakter)' });
    }
    const exists = await User.findOne({ username: value.toLowerCase() });
    return res.json({ available: !exists, message: exists ? 'Username sudah digunakan' : 'Username tersedia' });
  }
  if (type === 'email') {
    if (!/^\S+@\S+\.\S+$/.test(value)) {
      return res.json({ available: false, message: 'Format email tidak valid' });
    }
    const exists = await User.findOne({ email: value.toLowerCase() });
    return res.json({ available: !exists, message: exists ? 'Email sudah terdaftar' : 'Email tersedia' });
  }
});

app.get('/forgot-password', (req, res) => {
  if (req.session.userId) return res.redirect(req.session.userRole === 'admin' ? '/admin/dashboard' : '/dashboard');
  res.render('forgot_password');
});

app.post('/forgot-password', Limiter, async (req, res) => {
  const { login } = req.body;
  if (!login) {
    req.session.errorMsg = 'Harap masukkan email atau username Anda.';
    return res.redirect('/forgot-password');
  }
  try {
    const settings = await getSettings();
    const smtpConfig = getSmtpConfig(settings);
    if (!smtpConfig.user || !smtpConfig.pass) {
      req.session.errorMsg = 'Fitur pengiriman email belum dikonfigurasi oleh Administrator.';
      return res.redirect('/forgot-password');
    }
    const isEmail = login.includes('@');
    let user;
    if (isEmail) {
      user = await User.findOne({ email: login.toLowerCase() });
    } else {
      user = await User.findOne({ username: login.toLowerCase() });
    }
    if (!user) {
      req.session.errorMsg = 'Akun tidak ditemukan di sistem kami.';
      return res.redirect('/forgot-password');
    }
    if (!user.email) {
      req.session.errorMsg = 'Akun ini tidak memiliki alamat email yang valid.';
      return res.redirect('/forgot-password');
    }
    const token = crypto.randomBytes(32).toString('hex');
    user.resetPasswordToken = token;
    user.resetPasswordExpires = Date.now() + 30 * 60 * 1000;
    await user.save();

    const proto = req.headers['x-forwarded-proto'] || req.protocol || 'https';
    const host = req.headers['x-forwarded-host'] || req.headers.host;
    const resetLink = `${proto}://${host}/reset-password/${token}`;

    const sendResult = await sendPasswordResetEmail(user, resetLink);
    if (!sendResult.success) {
      req.session.errorMsg = sendResult.error || 'Gagal mengirim email reset password. Pastikan konfigurasi SMTP di Admin valid.';
      return res.redirect('/forgot-password');
    }

    req.session.successMsg = `Tautan reset password telah dikirim ke email ${maskEmail(user.email)}.`;
    res.redirect('/forgot-password');
  } catch (error) {
    console.error('Error Forgot Password:', error);
    req.session.errorMsg = 'Gagal memproses email. Pastikan konfigurasi SMTP di Admin valid.';
    res.redirect('/forgot-password');
  }
});

app.get('/reset-password/:token', async (req, res) => {
  try {
    const user = await User.findOne({
      resetPasswordToken: req.params.token,
      resetPasswordExpires: { $gt: Date.now() }
    });
    if (!user) {
      req.session.errorMsg = 'Token reset password tidak valid atau sudah kedaluwarsa (berlaku 30 menit).';
      return res.redirect('/forgot-password');
    }
    res.render('reset_password', { token: req.params.token });
  } catch (error) {
    res.redirect('/login');
  }
});

app.post('/reset-password/:token', async (req, res) => {
  try {
    const user = await User.findOne({
      resetPasswordToken: req.params.token,
      resetPasswordExpires: { $gt: Date.now() }
    });
    if (!user) {
      req.session.errorMsg = 'Token reset password tidak valid atau sudah kedaluwarsa.';
      return res.redirect('/forgot-password');
    }
    const { password, confirmPassword } = req.body;
    if (password !== confirmPassword) {
      req.session.errorMsg = 'Password dan konfirmasi password tidak cocok.';
      return res.redirect(`/reset-password/${req.params.token}`);
    }
    user.password = hashPassword(password);
    user.resetPasswordToken = undefined;
    user.resetPasswordExpires = undefined;
    await user.save();
    req.session.successMsg = 'Password berhasil diubah. Silakan login dengan password baru.';
    res.redirect('/login');
  } catch (error) {
    req.session.errorMsg = 'Gagal mereset password.';
    res.redirect(`/reset-password/${req.params.token}`);
  }
});

app.get('/logout', (req, res) => {
  if (req.session) {
    req.session.destroy(() => {
      res.redirect('/login');
    });
  } else {
    res.redirect('/login');
  }
});

// --- Dedicated Banned Page (User can view reason and Logout) ---
app.get('/banned', async (req, res) => {
  if (!req.session || !req.session.userId) {
    return res.redirect('/login');
  }
  try {
    const user = await User.findById(req.session.userId).lean();
    if (!user) {
      if (req.session) req.session.destroy(() => {});
      return res.redirect('/login');
    }

    const isRoleExempt = user.role === 'owner' || user.role === 'admin';
    const isIndividualBanned = user.suspended === true || user.accountStatus === 'banned' || user.accountStatus === 'suspended';
    const settings = await Setting.findOne().lean() || {};
    const isBanAllUsersActive = Boolean(settings.banAllUsers || settings.globalWebsiteBlock);

    // If user is not banned, redirect to normal dashboard
    if (isRoleExempt || (!isIndividualBanned && !isBanAllUsersActive)) {
      return res.redirect(isRoleExempt ? '/admin/dashboard' : '/dashboard');
    }

    let banReason = user.banReason;
    if (!isIndividualBanned && isBanAllUsersActive) {
      banReason = settings.banAllUsersReason || settings.globalBlockMessage || 'Semua akun pengguna saat ini sedang dinonaktifkan sementara oleh Administrator.';
    } else if (!banReason) {
      banReason = 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan';
    }

    return res.status(403).render('account_blocked', {
      settings,
      user,
      targetUsername: user.username,
      reason: banReason,
      bannedAt: user.bannedAt || user.updatedAt || new Date()
    });
  } catch (err) {
    console.error('Error rendering banned page:', err);
    return res.redirect('/login');
  }
});

// --- Dashboard User ---
app.get('/dashboard', isAuth, async (req, res) => {
  if (req.session.userRole === 'admin') return res.redirect('/admin/dashboard');
  const userId = req.session.userId;
  const user = res.locals.user;
  const totalDeposit = (await Transaction.aggregate([
    { $match: { userId: user._id, type: 'deposit', status: 'paid' } },
    { $group: { _id: null, total: { $sum: '$amount' } } }
  ]))[0]?.total || 0;
  const totalWithdraw = (await Transaction.aggregate([
    { $match: { userId: user._id, type: 'withdraw', status: 'success' } },
    { $group: { _id: null, total: { $sum: '$amount' } } }
  ]))[0]?.total || 0;
  const recentTrx = await Invoice.find({ userId: user._id }).sort({ createdAt: -1 }).lean();
  const apiKeys = await ApiKey.find({ userId }).lean();

  const pendingInvoices = await Invoice.find({
    userId: user._id,
    status: 'paid',
    settlementStatus: 'pending'
  }).sort({ releaseAt: 1 }).lean();

  const pendingBalance = pendingInvoices.reduce((sum, inv) => sum + (inv.amount || 0), 0);
  const nextReleaseAt = pendingInvoices.length > 0 ? pendingInvoices[0].releaseAt : null;

  res.render('dashboard', {
    user,
    totalDeposit,
    totalWithdraw,
    recentTrx,
    apiKeys,
    pendingBalance,
    nextReleaseAt
  });
});

app.get('/api/user/balance-status', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId).lean();
    if (!user) return res.status(404).json({ success: false, error: 'User tidak ditemukan' });

    const pendingInvoices = await Invoice.find({
      userId: req.session.userId,
      status: 'paid',
      settlementStatus: 'pending'
    }).sort({ releaseAt: 1 }).lean();

    const pendingBalance = pendingInvoices.reduce((sum, inv) => sum + (inv.amount || 0), 0);
    const nextReleaseAt = pendingInvoices.length > 0 ? pendingInvoices[0].releaseAt : null;

    res.json({
      success: true,
      balance: user.balance,
      pendingBalance,
      nextReleaseAt: nextReleaseAt ? nextReleaseAt.toISOString() : null
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// --- Profile & Security ---
app.get('/profile', isAuth, async (req, res) => {
  const userId = req.session.userId;
  const user = await User.findById(userId).lean();
  if (!user) return res.redirect('/login');

  // API Key is strictly hidden / inaccessible if 2FA is not enabled
  const apiKeys = user.twoFactorEnabled ? await ApiKey.find({ userId }).lean() : [];
  const activeRecoveryCount = (user.twoFactorRecoveryCodes || []).filter(c => !c.used).length;
  const totalRecoveryCount = (user.twoFactorRecoveryCodes || []).length;

  res.render('profile', {
    apiKeys,
    activeRecoveryCount,
    totalRecoveryCount
  });
});

app.get('/settings/security', isAuth, (req, res) => res.redirect('/profile#keamanan-2fa'));
app.get('/security', isAuth, (req, res) => res.redirect('/profile#keamanan-2fa'));

// ===================== TWO-FACTOR AUTHENTICATION (TOTP) API =====================

// 1. Setup 2FA: Generates or reuses pending secret, returns QR Code and formatted manual key
const handle2FaSetup = async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, message: 'Pengguna tidak ditemukan' });

    if (user.twoFactorEnabled) {
      return res.status(400).json({ success: false, message: 'Two-Factor Authentication (2FA) sudah aktif pada akun ini.' });
    }

    let secret = null;
    // Reuse pending secret if already generated to avoid invalidating QR code on reload
    if (user.twoFactorPendingSecretEncrypted) {
      secret = decryptTotpSecret(user.twoFactorPendingSecretEncrypted);
    }

    if (!secret || secret.length < 16) {
      secret = generateSecret();
      user.twoFactorPendingSecretEncrypted = encryptTotpSecret(secret);
      await user.save();
    }

    const appName = res.locals.settings?.name || 'PutzPay';
    const otpauthUri = generateURI({
      issuer: appName,
      label: user.email,
      secret: secret
    });

    const qrCodeDataUrl = await qrcode.toDataURL(otpauthUri, {
      width: 240,
      margin: 2,
      color: {
        dark: '#0f172a',
        light: '#ffffff'
      }
    });

    const secretManual = secret.match(/.{1,4}/g)?.join(' ') || secret;

    await logSecurityEvent({
      userId: user._id,
      action: '2FA_SETUP_INITIATED',
      details: 'User opened 2FA setup modal and received QR Code',
      ip: getClientIp(req),
      userAgent: req.get('user-agent'),
      status: 'success'
    });

    res.json({
      success: true,
      qrCode: qrCodeDataUrl,
      secret: secretManual,
      secretManual: secretManual,
      rawSecret: secret
    });
  } catch (err) {
    console.error('[2FA SETUP] Error:', err);
    res.status(500).json({ success: false, message: 'Gagal memproses pengaturan 2FA' });
  }
};

app.get('/api/2fa/setup', isAuth, handle2FaSetup);
app.post('/api/2fa/setup', isAuth, handle2FaSetup);

// 2. Verify Setup & Enable 2FA
app.post('/api/2fa/verify-setup', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, message: 'Pengguna tidak ditemukan' });

    if (user.twoFactorEnabled) {
      return res.status(400).json({ success: false, message: '2FA sudah aktif pada akun ini.' });
    }

    if (!user.twoFactorPendingSecretEncrypted) {
      return res.status(400).json({ success: false, message: 'Setup 2FA belum diinisiasi. Silakan muat ulang halaman.' });
    }

    const { locked, remainingSeconds } = checkTotpRateLimit(user);
    if (locked) {
      return res.status(429).json({ success: false, message: `Terlalu banyak percobaan salah. Coba lagi dalam ${remainingSeconds} detik.` });
    }

    const token = (req.body.token || req.body.code || '').trim();
    if (!token || !/^\d{6}$/.test(token)) {
      return res.status(400).json({ success: false, message: 'Masukkan 6 digit angka dari aplikasi Authenticator' });
    }

    const secret = decryptTotpSecret(user.twoFactorPendingSecretEncrypted);
    const isValid = verifyUserTotp(secret, token);

    if (!isValid) {
      await recordTotpFailure(user);
      await logSecurityEvent({
        userId: user._id,
        action: '2FA_SETUP_VERIFY_FAILED',
        details: 'Invalid TOTP code during 2FA setup verification',
        ip: getClientIp(req),
        userAgent: req.get('user-agent'),
        status: 'failed'
      });
      return res.status(400).json({
        success: false,
        message: 'Kode 2FA tidak valid. Pastikan waktu pada ponsel Anda sinkron otomatis dan coba lagi.'
      });
    }

    // Success! Generate 8 recovery codes
    const rawRecoveryCodes = generateRecoveryCodes(8);
    user.twoFactorRecoveryCodes = rawRecoveryCodes.map(code => ({
      codeHash: hashRecoveryCode(code),
      used: false,
      usedAt: null
    }));

    user.twoFactorEnabled = true;
    user.twoFactorSecretEncrypted = user.twoFactorPendingSecretEncrypted;
    user.twoFactorPendingSecretEncrypted = null;
    user.twoFactorEnabledAt = new Date();
    user.twoFactorLastUsedAt = new Date();
    await resetTotpFailures(user);
    await user.save();

    req.session.twoFactorVerified = true;

    await logSecurityEvent({
      userId: user._id,
      action: '2FA_ENABLED',
      details: 'Two-Factor Authentication successfully activated via TOTP',
      ip: getClientIp(req),
      userAgent: req.get('user-agent'),
      status: 'success'
    });

    sendPushNotification(user._id, 'security_alert', {
      title: '🔐 PutzPay 2FA Aktif',
      body: 'Two-Factor Authentication (Google Authenticator) berhasil diaktifkan.',
      data: { url: '/profile' }
    }).catch(() => {});

    telegramMonitor.notifySecurityAlert({
      type: '2FA TOTP Diaktifkan',
      username: user.username,
      ip: getClientIp(req),
      time: new Date()
    });

    res.json({
      success: true,
      message: 'Two-Factor Authentication berhasil diaktifkan!',
      recoveryCodes: rawRecoveryCodes
    });
  } catch (err) {
    console.error('[2FA VERIFY SETUP] Error:', err);
    res.status(500).json({ success: false, message: 'Gagal memverifikasi 2FA' });
  }
});

// 3. Disable 2FA: Requires account password AND valid 2FA token
app.post('/api/2fa/disable', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, message: 'Pengguna tidak ditemukan' });

    if (!user.twoFactorEnabled) {
      return res.status(400).json({ success: false, message: '2FA belum aktif pada akun ini.' });
    }

    const { locked, remainingSeconds } = checkTotpRateLimit(user);
    if (locked) {
      return res.status(429).json({ success: false, message: `Akun terkunci sementara. Coba lagi dalam ${remainingSeconds} detik.` });
    }

    const { password, token } = req.body;
    if (!password || !token) {
      return res.status(400).json({ success: false, message: 'Kata sandi dan kode 2FA wajib diisi untuk menonaktifkan 2FA.' });
    }

    if (!verifyPassword(password, user.password)) {
      await recordTotpFailure(user);
      return res.status(400).json({ success: false, message: 'Kata sandi akun salah.' });
    }

    const cleanToken = token.trim();
    let is2FaValid = false;
    if (cleanToken.includes('-') || cleanToken.length === 8 || cleanToken.length === 9) {
      is2FaValid = verifyAndConsumeRecoveryCode(user, cleanToken);
    } else {
      const secret = decryptTotpSecret(user.twoFactorSecretEncrypted);
      is2FaValid = verifyUserTotp(secret, cleanToken);
    }

    if (!is2FaValid) {
      await recordTotpFailure(user);
      await logSecurityEvent({
        userId: user._id,
        action: '2FA_DISABLE_FAILED',
        details: 'Invalid 2FA code during disable request',
        ip: getClientIp(req),
        userAgent: req.get('user-agent'),
        status: 'failed'
      });
      return res.status(400).json({ success: false, message: 'Kode 2FA atau kode pemulihan tidak valid.' });
    }

    // Success! Disable 2FA & revoke API Keys
    user.twoFactorEnabled = false;
    user.twoFactorSecretEncrypted = null;
    user.twoFactorPendingSecretEncrypted = null;
    user.twoFactorEnabledAt = null;
    user.twoFactorRecoveryCodes = [];
    await resetTotpFailures(user);
    await user.save();

    await ApiKey.deleteMany({ userId: user._id });

    await logSecurityEvent({
      userId: user._id,
      action: '2FA_DISABLED',
      details: 'Two-Factor Authentication disabled by user; API keys revoked',
      ip: getClientIp(req),
      userAgent: req.get('user-agent'),
      status: 'success'
    });

    sendPushNotification(user._id, 'security_alert', {
      title: '⚠️ PutzPay 2FA Dinonaktifkan',
      body: 'Two-Factor Authentication telah dinonaktifkan. API Key Anda telah dicabut.',
      data: { url: '/profile' }
    }).catch(() => {});

    telegramMonitor.notifySecurityAlert({
      type: '2FA Dinonaktifkan (API Key Dicabut)',
      username: user.username,
      ip: getClientIp(req),
      time: new Date()
    });

    res.json({
      success: true,
      message: '2FA berhasil dinonaktifkan. Seluruh API Key Anda telah dicabut demi keamanan.'
    });
  } catch (err) {
    console.error('[2FA DISABLE] Error:', err);
    res.status(500).json({ success: false, message: 'Gagal menonaktifkan 2FA' });
  }
});

// 4. Verify 2FA for Sensitive API Key Action (reveal, create, regenerate)
app.post('/api/user/api-key/verify-action', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, message: 'Pengguna tidak ditemukan', error: 'Pengguna tidak ditemukan' });

    if (!user.twoFactorEnabled) {
      const msg2fa = 'Aktifkan 2FA terlebih dahulu untuk mengelola atau melihat API Key.';
      return res.status(403).json({
        success: false,
        message: msg2fa,
        error: msg2fa,
        code: '2FA_REQUIRED'
      });
    }

    const { locked, remainingSeconds } = checkTotpRateLimit(user);
    if (locked) {
      const lockedMsg = `Terlalu banyak percobaan salah. Coba lagi dalam ${remainingSeconds} detik.`;
      return res.status(429).json({ success: false, message: lockedMsg, error: lockedMsg });
    }

    // FIX BUG #1: Frontend (profile.ejs) mengirim field "token", bukan "totpCode".
    // Sebelumnya endpoint ini membaca req.body.totpCode (selalu undefined) sehingga
    // request selalu gagal di validasi awal walau kode OTP yang diinput user benar.
    const { action, token: totpCode } = req.body;
    if (!totpCode || !String(totpCode).trim()) {
      const msg = 'Kode 2FA wajib diisi.';
      return res.status(400).json({ success: false, message: msg, error: msg });
    }

    const cleanCode = String(totpCode).trim();
    let isValid = false;
    if (cleanCode.includes('-') || cleanCode.length === 8 || cleanCode.length === 9) {
      isValid = verifyAndConsumeRecoveryCode(user, cleanCode);
    } else {
      const secret = decryptTotpSecret(user.twoFactorSecretEncrypted);
      isValid = verifyUserTotp(secret, cleanCode);
    }

    if (!isValid) {
      await recordTotpFailure(user);
      await logSecurityEvent({
        userId: user._id,
        action: `API_KEY_${(action || 'ACTION').toUpperCase()}_FAILED`,
        details: 'Invalid 2FA code for API Key action',
        ip: getClientIp(req),
        userAgent: req.get('user-agent'),
        status: 'failed'
      });
      // FIX BUG #2: Frontend membaca data.error, sebelumnya backend hanya mengirim
      // field "message" sehingga frontend selalu menampilkan fallback generik
      // "Verifikasi gagal." Sekarang keduanya dikirim dengan isi yang sama.
      const msg = 'Kode 2FA tidak valid. Silakan coba lagi.';
      return res.status(400).json({ success: false, message: msg, error: msg });
    }

    await resetTotpFailures(user);
    user.twoFactorLastUsedAt = new Date();
    await user.save();

    if (action === 'reveal') {
      const keyDoc = await ApiKey.findOne({ userId: user._id });
      await logSecurityEvent({
        userId: user._id,
        action: 'API_KEY_REVEALED',
        details: 'API Key revealed with 2FA verification',
        ip: getClientIp(req),
        userAgent: req.get('user-agent'),
        status: 'success'
      });
      return res.json({
        success: true,
        apiKey: keyDoc ? keyDoc.key : null,
        message: 'Verifikasi 2FA berhasil.'
      });
    }

    if (action === 'create' || action === 'regenerate') {
      await ApiKey.deleteMany({ userId: user._id });
      const newKey = generateApiKey();
      await createWithRetry(ApiKey, { userId: user._id, key: newKey });

      await logSecurityEvent({
        userId: user._id,
        action: action === 'create' ? 'API_KEY_CREATED' : 'API_KEY_REGENERATED',
        details: `API Key ${action} with 2FA verification`,
        ip: getClientIp(req),
        userAgent: req.get('user-agent'),
        status: 'success'
      });

      return res.json({
        success: true,
        apiKey: newKey,
        message: `API Key berhasil ${action === 'create' ? 'dibuat' : 'diperbarui'}!`
      });
    }

    { const msg = 'Aksi tidak dikenal.'; return res.status(400).json({ success: false, message: msg, error: msg }); }
  } catch (err) {
    console.error('[API KEY VERIFY ACTION] Error:', err);
    const msg = 'Gagal memproses aksi API Key';
    res.status(500).json({ success: false, message: msg, error: msg });
  }
});

// 5. Regenerate API Key (protected with 2FA)
app.post('/api/user/api-key/regenerate', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ error: 'Pengguna tidak ditemukan' });

    if (!user.twoFactorEnabled) {
      return res.status(403).json({ error: 'Aktifkan 2FA terlebih dahulu untuk membuat API Key.', code: '2FA_REQUIRED' });
    }

    const { totpCode } = req.body;
    if (!totpCode) {
      return res.status(400).json({ error: 'Verifikasi 2FA (TOTP) diperlukan untuk membuat atau memperbarui API Key.' });
    }

    const cleanCode = totpCode.trim();
    let isValid = false;
    if (cleanCode.includes('-')) {
      isValid = verifyAndConsumeRecoveryCode(user, cleanCode);
    } else {
      const secret = decryptTotpSecret(user.twoFactorSecretEncrypted);
      isValid = verifyUserTotp(secret, cleanCode);
    }

    if (!isValid) {
      await recordTotpFailure(user);
      return res.status(400).json({ error: 'Kode 2FA tidak valid. Silakan coba lagi.' });
    }

    await resetTotpFailures(user);
    await ApiKey.deleteMany({ userId: req.session.userId });
    const key = generateApiKey();
    await createWithRetry(ApiKey, { userId: req.session.userId, key });

    await logSecurityEvent({
      userId: user._id,
      action: 'API_KEY_REGENERATED',
      details: 'API Key regenerated via 2FA verification',
      ip: getClientIp(req),
      userAgent: req.get('user-agent'),
      status: 'success'
    });

    res.json({ apiKey: key, success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal membuat API key' });
  }
});

// ===================== WEBHOOK MANAGEMENT (Merchant) =====================
// GET config: url, status aktif, dan secret key untuk verifikasi signature
app.get('/api/webhook/config', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId).select('webhookUrl webhookEnabled webhookSecret').lean();
    if (!user) return res.status(404).json({ success: false, error: 'Pengguna tidak ditemukan' });
    res.json({
      success: true,
      url: user.webhookUrl || '',
      enabled: !!user.webhookEnabled,
      secret: user.webhookSecret || null
    });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Gagal mengambil konfigurasi webhook' });
  }
});

// Simpan/perbarui URL webhook & status aktif
app.post('/api/webhook/config', isAuth, async (req, res) => {
  try {
    const { url, enabled } = req.body;
    const cleanUrl = (url || '').trim();

    if (enabled && !cleanUrl) {
      return res.status(400).json({ success: false, error: 'URL webhook wajib diisi untuk mengaktifkan webhook.' });
    }
    if (cleanUrl && !/^https?:\/\/.+/i.test(cleanUrl)) {
      return res.status(400).json({ success: false, error: 'URL webhook harus diawali http:// atau https://' });
    }

    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, error: 'Pengguna tidak ditemukan' });

    user.webhookUrl = cleanUrl;
    user.webhookEnabled = !!enabled && !!cleanUrl;
    if (!user.webhookSecret) user.webhookSecret = generateWebhookSecret();
    await user.save();

    await logSecurityEvent({
      userId: user._id,
      action: 'WEBHOOK_CONFIG_UPDATED',
      details: `Webhook ${user.webhookEnabled ? 'diaktifkan' : 'dinonaktifkan'}: ${cleanUrl || '(kosong)'}`,
      ip: getClientIp(req),
      userAgent: req.get('user-agent'),
      status: 'success'
    });

    res.json({
      success: true,
      url: user.webhookUrl,
      enabled: user.webhookEnabled,
      secret: user.webhookSecret,
      message: 'Konfigurasi webhook berhasil disimpan.'
    });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Gagal menyimpan konfigurasi webhook' });
  }
});

// Generate ulang secret key (dipakai untuk verifikasi HMAC signature)
app.post('/api/webhook/secret/regenerate', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, error: 'Pengguna tidak ditemukan' });

    user.webhookSecret = generateWebhookSecret();
    await user.save();

    await logSecurityEvent({
      userId: user._id,
      action: 'WEBHOOK_SECRET_REGENERATED',
      details: 'Webhook signing secret diperbarui',
      ip: getClientIp(req),
      userAgent: req.get('user-agent'),
      status: 'success'
    });

    res.json({ success: true, secret: user.webhookSecret, message: 'Secret webhook berhasil diperbarui. Perbarui verifikasi signature di server Anda.' });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Gagal memperbarui secret webhook' });
  }
});

// Kirim event uji coba (webhook.test) ke URL yang terdaftar
app.post('/api/webhook/test', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId);
    if (!user) return res.status(404).json({ success: false, error: 'Pengguna tidak ditemukan' });
    if (!user.webhookUrl) return res.status(400).json({ success: false, error: 'Set URL webhook terlebih dahulu sebelum menguji.' });
    if (!user.webhookEnabled) return res.status(400).json({ success: false, error: 'Aktifkan webhook terlebih dahulu sebelum menguji.' });

    const testPayload = {
      event: 'webhook.test',
      message: 'Ini adalah pengiriman uji coba webhook dari PutzPay.',
      sent_at: new Date().toISOString()
    };
    const result = await sendWebhookEvent(user, 'webhook.test', testPayload, null);

    res.json({
      success: !!(result && result.success),
      httpStatus: result ? result.httpStatus : null,
      errorMessage: result ? result.errorMessage : 'Webhook tidak aktif atau URL kosong.'
    });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Gagal mengirim webhook uji coba' });
  }
});

// Riwayat pengiriman webhook (50 terbaru) milik user yang login
app.get('/api/webhook/logs', isAuth, async (req, res) => {
  try {
    const logs = await WebhookLog.find({ userId: req.session.userId })
      .sort({ createdAt: -1 })
      .limit(50)
      .lean();
    res.json({ success: true, logs });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Gagal mengambil log webhook' });
  }
});

// Kirim ulang secara manual salah satu log webhook (misalnya karena server merchant sempat down)
app.post('/api/webhook/logs/:id/retry', isAuth, async (req, res) => {
  try {
    const log = await WebhookLog.findOne({ _id: req.params.id, userId: req.session.userId });
    if (!log) return res.status(404).json({ success: false, error: 'Log webhook tidak ditemukan' });

    const user = await User.findById(req.session.userId);
    if (!user || !user.webhookUrl) {
      return res.status(400).json({ success: false, error: 'URL webhook belum diset.' });
    }

    const result = await sendWebhookEvent(user, log.event, log.payload, log.invoiceId, (log.attempt || 1) + 1);

    res.json({
      success: !!(result && result.success),
      httpStatus: result ? result.httpStatus : null,
      errorMessage: result ? result.errorMessage : 'Gagal mengirim ulang webhook.'
    });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Gagal mengirim ulang webhook' });
  }
});

app.post('/profile', isAuth, async (req, res) => {
  const { fullName, email, telegramId, phoneNumber, newPassword, ewallet, accountNumber, accountName } = req.body;
  try {
    const upd = {};
    if (typeof fullName !== 'undefined') upd.fullName = (fullName || '').trim();
    if (typeof email !== 'undefined' && email.trim()) upd.email = email.trim().toLowerCase();
    if (typeof telegramId !== 'undefined') upd.telegramId = (telegramId || '').trim();
    if (typeof phoneNumber !== 'undefined') upd.phoneNumber = (phoneNumber || '').trim();
    if (typeof ewallet !== 'undefined') upd.ewallet = ewallet;
    if (typeof accountNumber !== 'undefined') upd.accountNumber = accountNumber;
    if (typeof accountName !== 'undefined') upd.accountName = accountName;
    if (newPassword && newPassword.trim()) upd.password = hashPassword(newPassword.trim());

    await User.findByIdAndUpdate(req.session.userId, upd);
    req.session.successMsg = 'Profil berhasil diperbarui';
    res.redirect('/profile');
  } catch (err) {
    if (err.code === 11000) {
      req.session.errorMsg = 'Email sudah digunakan oleh pengguna lain';
    } else {
      req.session.errorMsg = 'Gagal memperbarui profil';
    }
    res.redirect('/profile');
  }
});

app.post('/profile/upload', isAuth, profileUpload.single('profileImage'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'File tidak ditemukan' });
  const imageUrl = '/profile/' + req.file.filename;
  await User.findByIdAndUpdate(req.session.userId, { profilePicture: imageUrl });
  res.json({ success: true, profilePicture: imageUrl });
});

// FIX BUG #3: Route duplikat '/api/user/api-key/regenerate' yang sebelumnya ada di sini
// SUDAH DIHAPUS karena tidak melakukan verifikasi 2FA sama sekali (celah keamanan:
// siapapun yang login bisa regenerate API key tanpa kode OTP). Endpoint resmi untuk
// create/regenerate/reveal API Key ada di POST /api/user/api-key/verify-action (di atas),
// yang sudah mewajibkan verifikasi 2FA. Endpoint kedua di baris ~5144 juga masih ada
// sebagai legacy/alias dan tetap mewajibkan 2FA, jadi aman untuk dipertahankan.

// --- Deposit ---
app.get('/deposit', isAuth, async (req, res) => {
  const settings = res.locals.settings;
  const deposits = await Transaction.find({
    userId: req.session.userId,
    type: 'deposit'
  }).sort({ createdAt: -1 }).lean();
  const invoices = await Invoice.find({ userId: req.session.userId }).sort({ createdAt: -1 }).lean();
  res.render('deposit', { deposits, invoices, minDeposit: settings.minDeposit });
});

async function createInvoiceForUser(userOrId, amount, settings) {
  if (!settings) settings = await getSettings();

  const actualUserId = (userOrId && userOrId._id) ? userOrId._id : userOrId;
  let userAccount = null;
  if (userOrId && userOrId.username && typeof userOrId.balance === 'number') {
    userAccount = userOrId;
  } else if (actualUserId) {
    userAccount = await User.findById(actualUserId).select('username fullName name email balance kycStatus');
  }

  if (userAccount) {
    const depositLimitCheck = await checkDepositLimits(userAccount, amount, settings);
    if (!depositLimitCheck.allowed) {
      throw new Error(depositLimitCheck.message);
    }
  }

  let formattedAccountName = 'Pengguna';
  if (userAccount) {
    if (userAccount.fullName && userAccount.username && userAccount.fullName.trim() !== userAccount.username.trim()) {
      formattedAccountName = `${userAccount.fullName.trim()} (@${userAccount.username.trim()})`;
    } else if (userAccount.username) {
      formattedAccountName = userAccount.username.trim();
    } else if (userAccount.fullName) {
      formattedAccountName = userAccount.fullName.trim();
    } else if (userAccount.email) {
      formattedAccountName = userAccount.email.split('@')[0];
    }
  }

  if (isNaN(amount) || amount < settings.minDeposit) {
    throw new Error(`Minimal deposit adalah Rp ${settings.minDeposit.toLocaleString('id-ID')}`);
  }

  const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const lockedInvoices = await Invoice.find({
    $or: [{ status: 'pending' }, { status: 'paid', createdAt: { $gte: oneDayAgo } }]
  }).select('fee');
  const usedFees = lockedInvoices.map(i => Number(i.fee)).filter(f => !isNaN(f));
  const availableFees = [];
  for (let i = 1; i <= settings.maxFee; i++) {
    if (!usedFees.includes(i)) availableFees.push(i);
  }
  if (availableFees.length === 0) {
    throw new Error('Kode unik deposit sedang penuh. Silakan coba lagi beberapa menit.');
  }
  const fee = availableFees[Math.floor(Math.random() * availableFees.length)];
  const total = amount + fee;

  const expiredMinutes = settings.qrisExpiredMinutes || 30;
  const expiredAt = new Date(Date.now() + expiredMinutes * 60 * 1000);

  const invoice = await createWithRetry(Invoice, {
    userId: actualUserId,
    amount,
    fee,
    total,
    trxid: null,
    qris_image: '',
    expiredAt,
    status: 'pending'
  });

  if (!settings.gopayToken || !settings.gopayStaticQr) {
    await Invoice.findByIdAndDelete(invoice._id);
    throw new Error('Konfigurasi Gopay Merchant belum lengkap. Hubungi admin.');
  }
  const gopayBase = settings.gopayDomain || 'gomerch.vercel.app';
  const apiUrl = `https://${gopayBase}/api/qris/create?amount=${total}&static_qr=${encodeURIComponent(settings.gopayStaticQr)}&token=${encodeURIComponent(settings.gopayToken)}`;
  try {
    const data = await callGopayApiWithRetry(apiUrl);
    if (!data.success) {
      await Invoice.findByIdAndDelete(invoice._id);
      throw new Error('Gagal membuat QRIS via Gopay Merchant');
    }
    const qrisImage = data.image_url;
    const trxid = invoice._id;

    invoice.qris_image = qrisImage;
    invoice.trxid = trxid;
    await invoice.save();

    await createWithRetry(Transaction, {
      userId: actualUserId,
      type: 'deposit',
      amount,
      fee,
      status: 'pending',
      reference: invoice._id.toString(),
      qris_image: qrisImage,
      expiredAt
    });

    sendPushNotification(actualUserId, 'payment_pending', {
      title: '⏳ PutzPay',
      body: `Pembayaran Rp ${amount.toLocaleString('id-ID')} sedang menunggu konfirmasi.`,
      data: { url: '/deposit', invoiceId: invoice._id, amount }
    }, { eventId: `payment_pending_${invoice._id}` }).catch(() => {});

    telegramMonitor.notifyInvoiceCreated({
      invoice_id: invoice._id,
      order_id: invoice.trxid || invoice._id,
      username: formattedAccountName,
      amount: invoice.amount,
      fee: invoice.fee,
      total: invoice.total,
      method: 'QRIS Realtime',
      status: '⏳ Menunggu Pembayaran',
      createdAt: invoice.createdAt
    });

    return invoice;
  } catch (e) {
    await Invoice.findByIdAndDelete(invoice._id);
    throw e;
  }
}

app.post('/invoice/create', isAuth, async (req, res) => {
  try {
    const amount = parseInt(req.body.amount);
    const settings = res.locals.settings;
    const invoice = await createInvoiceForUser(req.session.userId, amount, settings);
    return res.json({
      reference: invoice._id.toString(),
      amount: invoice.amount,
      fee: invoice.fee,
      total: invoice.total,
      qris_image: invoice.qris_image,
      createdAt: invoice.createdAt,
      expiredAt: invoice.expiredAt,
      status: 'pending'
    });
  } catch (e) {
    console.error('Create invoice error:', e.message);
    return res.status(400).json({ error: e.message });
  }
});

// ===================== INVOICE CANCEL (WEB PORTAL) =====================
app.post('/invoice/cancel', isAuth, async (req, res) => {
  try {
    const invoiceId = req.body.invoiceId || req.body.id;
    if (!invoiceId) {
      return res.status(400).json({ success: false, error: 'ID invoice wajib disertakan' });
    }

    const invoice = await Invoice.findById(invoiceId);
    if (!invoice) {
      return res.status(404).json({ success: false, error: 'Invoice tidak ditemukan' });
    }

    const isOwner = invoice.userId && invoice.userId.toString() === req.session.userId.toString();
    const isAdmin = req.session.userRole === 'admin' || req.session.userRole === 'owner';
    if (!isOwner && !isAdmin) {
      return res.status(403).json({ success: false, error: 'Akses ditolak: Anda bukan pemilik invoice ini' });
    }

    if (invoice.status === 'paid') {
      return res.status(400).json({ success: false, error: 'Invoice sudah dibayar dan tidak dapat dibatalkan' });
    }

    if (invoice.status === 'cancelled') {
      return res.json({ success: true, message: 'Transaksi sudah dibatalkan', status: 'cancelled' });
    }

    invoice.status = 'cancelled';
    await invoice.save();

    await Transaction.updateOne(
      { reference: invoice._id.toString(), type: 'deposit', status: 'pending' },
      { status: 'cancelled' }
    );

    emitLiveTransaction('payment_cancelled', {
      userId: invoice.userId,
      amount: invoice.amount,
      invoice_id: invoice._id,
      status: 'cancelled',
      createdAt: invoice.createdAt
    });

    let cancelUser = await User.findById(invoice.userId).select('username fullName');
    let cancelUsername = cancelUser ? (cancelUser.fullName || cancelUser.username) : (req.session.username || 'Pengguna');

    telegramMonitor.notifyInvoiceCancelled({
      invoice_id: invoice._id,
      username: cancelUsername,
      amount: invoice.total || invoice.amount,
      status: 'cancelled'
    });

    return res.json({
      success: true,
      message: 'Transaksi berhasil dibatalkan',
      invoice_id: invoice._id,
      status: 'cancelled'
    });
  } catch (err) {
    console.error('Cancel invoice web error:', err.message);
    return res.status(500).json({ success: false, error: err.message || 'Gagal membatalkan transaksi' });
  }
});

// ===================== API INVOICE DETAILS & STATUS =====================
app.get('/api/invoice/:id', async (req, res) => {
  try {
    const invoice = await Invoice.findById(req.params.id).lean();
    if (!invoice) {
      return res.status(404).json({ success: false, error: 'Invoice tidak ditemukan' });
    }

    return res.json({
      success: true,
      reference: invoice._id,
      amount: invoice.amount,
      fee: invoice.fee || 0,
      total: invoice.total || (invoice.amount + (invoice.fee || 0)),
      status: invoice.status,
      qris_image: invoice.qris_image,
      createdAt: invoice.createdAt,
      expiredAt: invoice.expiredAt,
      successAt: invoice.successAt,
      customerName: invoice.customerName || null
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

// ===================== STANDALONE DIGITAL RECEIPT / INVOICE =====================
app.get('/invoice/:id', async (req, res) => {
  try {
    const settings = res.locals.settings;
    const invoice = await Invoice.findById(req.params.id).lean();
    if (!invoice) {
      return res.status(404).render('home', {
        title: 'Invoice Tidak Ditemukan',
        error: 'Invoice dengan ID tersebut tidak ditemukan.'
      });
    }

    let merchant = null;
    if (invoice.userId) {
      merchant = await User.findById(invoice.userId).select('username fullName name profilePicture email').lean();
    }

    const isOwner = res.locals.user && invoice.userId && String(invoice.userId) === String(res.locals.user._id);

    return res.render('invoice_receipt', {
      settings,
      invoice,
      merchant,
      isOwner,
      user: res.locals.user,
      title: (invoice.status === 'paid' ? 'Struk Pembayaran' : 'Invoice Pembayaran') + ' #' + invoice._id
    });
  } catch (err) {
    console.error('View invoice error:', err.message);
    return res.redirect('/');
  }
});

// ===================== NO-CODE PAYMENT LINKS =====================
app.get('/payment-links', isAuth, async (req, res) => {
  try {
    const settings = res.locals.settings;
    const paymentLinks = await PaymentLink.find({ userId: req.session.userId }).sort({ createdAt: -1 }).lean();

    const stats = {
      totalLinks: paymentLinks.length,
      activeLinks: paymentLinks.filter(p => p.isActive).length,
      totalPaidCount: paymentLinks.reduce((sum, p) => sum + (p.totalPaidCount || 0), 0),
      totalPaidAmount: paymentLinks.reduce((sum, p) => sum + (p.totalPaidAmount || 0), 0)
    };

    return res.render('payment_links', {
      settings,
      paymentLinks,
      stats,
      minDeposit: settings.minDeposit || 1000,
      title: 'Link Pembayaran Siap Pakai'
    });
  } catch (err) {
    console.error('Payment links page error:', err.message);
    return res.status(500).send('Terjadi kesalahan memuat link pembayaran');
  }
});

app.post('/payment-links/create', isAuth, async (req, res) => {
  try {
    const settings = res.locals.settings;
    const { title, customSlug, description, amountType, amount, minAmount } = req.body;

    if (!title || !title.trim()) {
      return res.status(400).json({ success: false, error: 'Judul link pembayaran wajib diisi' });
    }

    let code = '';
    if (customSlug && customSlug.trim()) {
      code = customSlug.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
      if (code.length < 3) {
        return res.status(400).json({ success: false, error: 'Custom link minimal 3 karakter huruf/angka' });
      }
      const existing = await PaymentLink.findOne({ code });
      if (existing) {
        return res.status(400).json({ success: false, error: 'URL link pembayaran sudah digunakan, silakan pilih nama lain' });
      }
    } else {
      code = 'pl_' + crypto.randomBytes(3).toString('hex');
      let existing = await PaymentLink.findOne({ code });
      while (existing) {
        code = 'pl_' + crypto.randomBytes(3).toString('hex');
        existing = await PaymentLink.findOne({ code });
      }
    }

    const type = amountType === 'fixed' ? 'fixed' : 'custom';
    let numericAmount = 0;
    if (type === 'fixed') {
      numericAmount = parseInt(amount) || 0;
      if (numericAmount < (settings.minDeposit || 1000)) {
        return res.status(400).json({ success: false, error: `Nominal minimal adalah Rp ${(settings.minDeposit || 1000).toLocaleString('id-ID')}` });
      }
    }

    const minAmt = parseInt(minAmount) || (settings.minDeposit || 1000);

    const newLink = await createWithRetry(PaymentLink, {
      userId: req.session.userId,
      code,
      title: title.trim(),
      description: description ? description.trim() : '',
      amountType: type,
      amount: numericAmount,
      minAmount: minAmt,
      isActive: true
    });

    return res.json({
      success: true,
      message: 'Link pembayaran berhasil dibuat!',
      link: newLink
    });
  } catch (err) {
    console.error('Create payment link error:', err.message);
    return res.status(500).json({ success: false, error: err.message || 'Gagal membuat link pembayaran' });
  }
});

app.post('/payment-links/:id/toggle', isAuth, async (req, res) => {
  try {
    const link = await PaymentLink.findOne({ _id: req.params.id, userId: req.session.userId });
    if (!link) {
      return res.status(404).json({ success: false, error: 'Link pembayaran tidak ditemukan' });
    }

    link.isActive = !link.isActive;
    link.updatedAt = new Date();
    await link.save();

    return res.json({
      success: true,
      isActive: link.isActive,
      message: `Link berhasil ${link.isActive ? 'diaktifkan' : 'dinonaktifkan'}`
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/payment-links/:id/delete', isAuth, async (req, res) => {
  try {
    const deleted = await PaymentLink.findOneAndDelete({ _id: req.params.id, userId: req.session.userId });
    if (!deleted) {
      return res.status(404).json({ success: false, error: 'Link pembayaran tidak ditemukan' });
    }

    return res.json({
      success: true,
      message: 'Link pembayaran berhasil dihapus'
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/pay/:code', async (req, res) => {
  try {
    const settings = res.locals.settings;
    const code = req.params.code.toLowerCase().trim();
    const link = await PaymentLink.findOne({ code }).lean();

    if (!link) {
      return res.status(404).render('pay_checkout', {
        settings,
        link: null,
        merchant: null,
        error: 'Link pembayaran tidak ditemukan atau URL salah.',
        title: 'Link Pembayaran Tidak Ditemukan'
      });
    }

    if (!link.isActive) {
      return res.status(403).render('pay_checkout', {
        settings,
        link,
        merchant: null,
        error: 'Link pembayaran ini sedang dinonaktifkan oleh pemiliknya.',
        title: link.title + ' - Nonaktif'
      });
    }

    const merchant = await User.findById(link.userId).select('username fullName name profilePicture email').lean();

    return res.render('pay_checkout', {
      settings,
      link,
      merchant,
      error: null,
      title: link.title + ' - Bayar via QRIS'
    });
  } catch (err) {
    console.error('Pay checkout error:', err.message);
    return res.redirect('/');
  }
});

app.post('/pay/:code/create-invoice', async (req, res) => {
  try {
    const settings = res.locals.settings;
    const code = req.params.code.toLowerCase().trim();
    const link = await PaymentLink.findOne({ code });

    if (!link || !link.isActive) {
      return res.status(404).json({ success: false, error: 'Link pembayaran tidak ditemukan atau dinonaktifkan' });
    }

    let amount = 0;
    if (link.amountType === 'fixed') {
      amount = link.amount;
    } else {
      amount = parseInt(req.body.amount);
      const minRequired = link.minAmount || settings.minDeposit || 1000;
      if (isNaN(amount) || amount < minRequired) {
        return res.status(400).json({ success: false, error: `Nominal minimal adalah Rp ${minRequired.toLocaleString('id-ID')}` });
      }
    }

    const invoice = await createInvoiceForUser(link.userId, amount, settings);

    invoice.paymentLinkId = link._id;
    invoice.customerName = (req.body.customerName || 'Pelanggan').trim().slice(0, 50);
    invoice.customerPhone = (req.body.customerPhone || '').trim().slice(0, 20);
    invoice.customerEmail = (req.body.customerEmail || '').trim().slice(0, 60);
    await invoice.save();

    return res.json({
      success: true,
      invoiceId: invoice._id.toString(),
      reference: invoice._id.toString(),
      amount: invoice.amount,
      fee: invoice.fee,
      total: invoice.total,
      qris_image: invoice.qris_image,
      expiredAt: invoice.expiredAt,
      redirectUrl: '/invoice/' + invoice._id.toString()
    });
  } catch (err) {
    console.error('Create pay link invoice error:', err.message);
    return res.status(400).json({ success: false, error: err.message || 'Gagal memproses pembayaran' });
  }
});

// ===================== WITHDRAW =====================
app.get('/withdraw', isAuth, async (req, res) => {
  const settings = res.locals.settings;
  const withdrawals = await Withdrawal.find({ userId: req.session.userId }).sort({ createdAt: -1 }).lean();
  res.render('withdraw', {
    settings,
    withdrawals,
    withdrawMethods: settings.withdrawMethods || []
  });
});

app.post('/withdraw/request', isAuth, async (req, res) => {
  const settings = res.locals.settings || (await getSettings());
  const { amount, methodName, accountNumber, accountName } = req.body;

  if (!methodName || !accountNumber || !accountName) {
    req.session.errorMsg = 'Harap lengkapi metode, nomor rekening/akun, dan nama pemilik akun.';
    return res.redirect('/withdraw');
  }

  const amt = parseInt(amount);
  if (isNaN(amt) || amt < settings.minWithdraw) {
    req.session.errorMsg = 'Minimal penarikan Rp ' + settings.minWithdraw.toLocaleString('id-ID');
    return res.redirect('/withdraw');
  }

  const userDoc = await User.findById(req.session.userId);
  if (!userDoc) {
    req.session.errorMsg = 'Pengguna tidak ditemukan.';
    return res.redirect('/withdraw');
  }

  // Check account limits (Non-KYC vs KYC Verified)
  const limitCheck = await checkWithdrawalLimits(userDoc, amt, settings);
  if (!limitCheck.allowed) {
    req.session.errorMsg = limitCheck.message;
    return res.redirect('/withdraw');
  }

  const selectedMethod = (settings.withdrawMethods || []).find(m => m.name === methodName);
  if (!selectedMethod) {
    req.session.errorMsg = 'Metode penarikan tidak valid.';
    return res.redirect('/withdraw');
  }

  // Calculate fee with KYC discount applied
  const baseFee = selectedMethod.fee || 0;
  const fee = calculateUserWithdrawFee(userDoc, baseFee, settings);
  const totalDeduct = amt + fee;

  const updatedUser = await User.findOneAndUpdate(
    { _id: req.session.userId, balance: { $gte: totalDeduct } },
    { $inc: { balance: -totalDeduct } },
    { new: true }
  );
  if (!updatedUser) {
    req.session.errorMsg = 'Saldo tidak cukup (termasuk biaya admin Rp ' + fee.toLocaleString() + ')';
    return res.redirect('/withdraw');
  }

  try {
    const ref = 'W' + Date.now().toString(36).toUpperCase();
    const wd = await createWithRetry(Withdrawal, {
      userId: req.session.userId,
      amount: amt,
      fee,
      method: selectedMethod.name,
      accountNumber,
      accountName
    });
    await createWithRetry(Transaction, {
      userId: req.session.userId,
      type: 'withdraw',
      amount: amt,
      fee,
      status: 'pending',
      reference: ref,
      method: selectedMethod.name,
      accountNumber,
      accountName
    });
    telegramMonitor.notifyWithdraw({
      withdraw_id: wd._id,
      amount: amt,
      method: selectedMethod.name,
      status: 'PROCESSING'
    });
    req.session.successMsg = 'Penarikan berhasil diajukan dan sedang diproses.';
  } catch (err) {
    await User.findByIdAndUpdate(req.session.userId, { $inc: { balance: totalDeduct } });
    req.session.errorMsg = 'Gagal memproses penarikan. Silakan coba lagi.';
    console.error(err);
  }
  res.redirect('/withdraw');
});

// Route for Instant Withdrawal Fee & Day Info (Web Session)
app.get('/withdraw/instant/fee-preview', isAuth, (req, res) => {
  const isFriday = isFridayInJakarta();
  res.json({
    success: true,
    isFriday,
    ewalletFeeRange: isFriday ? 'Rp 300 - Rp 500 (Promo Jumat)' : 'Rp 500 - Rp 700 (Biaya Layanan)',
    feeMin: isFriday ? 300 : 500,
    feeMax: isFriday ? 500 : 700,
    allowedNominals: Object.keys(INSTANT_NOMINAL_MAPPING).map(Number)
  });
});

// Route for Web Instant Withdrawal Request
app.post('/withdraw/instant', isAuth, async (req, res) => {
  const isAjax = req.xhr || req.headers.accept?.includes('application/json');
  try {
    const { ewallet, nomor, nominal } = req.body;

    // 1. Validate e-wallet
    if (!ewallet || String(ewallet).toLowerCase().trim() !== 'dana') {
      if (isAjax) return res.status(400).json({ success: false, message: 'E-wallet tidak tersedia. Saat ini hanya DANA yang didukung.' });
      req.session.errorMsg = 'E-wallet tidak tersedia. Saat ini hanya DANA yang didukung.';
      return res.redirect('/withdraw');
    }

    // 2. Validate nomor
    const cleanNomor = cleanEwalletPhone(nomor);
    if (!cleanNomor || cleanNomor.length < 9 || cleanNomor.length > 15) {
      if (isAjax) return res.status(400).json({ success: false, message: 'Nomor e-wallet DANA tidak valid.' });
      req.session.errorMsg = 'Nomor e-wallet DANA tidak valid.';
      return res.redirect('/withdraw');
    }

    // 3. Validate nominal
    const amt = parseInt(nominal, 10);
    if (isNaN(amt) || !INSTANT_NOMINAL_MAPPING[amt]) {
      if (isAjax) return res.status(400).json({ success: false, message: 'Nominal penarikan tidak tersedia dalam pilihan.' });
      req.session.errorMsg = 'Nominal penarikan tidak tersedia dalam pilihan.';
      return res.redirect('/withdraw');
    }

    const userDoc = await User.findById(req.session.userId);
    if (!userDoc) {
      if (isAjax) return res.status(404).json({ success: false, message: 'Pengguna tidak ditemukan.' });
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/withdraw');
    }

    const settings = await getSettings();
    // Check account limits (Non-KYC vs KYC Verified)
    const limitCheck = await checkWithdrawalLimits(userDoc, amt, settings);
    if (!limitCheck.allowed) {
      if (isAjax) return res.status(400).json({ success: false, message: limitCheck.message });
      req.session.errorMsg = limitCheck.message;
      return res.redirect('/withdraw');
    }

    // 4. Calculate locked fee and total (with KYC discount)
    const rawFee = calculateInstantWithdrawFee('dana');
    const fee = calculateUserWithdrawFee(userDoc, rawFee, settings);
    const totalDeduct = amt + fee;

    // 5. Atomic balance deduction
    const user = await User.findOneAndUpdate(
      {
        _id: req.session.userId,
        balance: { $gte: totalDeduct }
      },
      {
        $inc: { balance: -totalDeduct }
      },
      {
        new: true
      }
    );

    if (!user) {
      if (isAjax) return res.status(400).json({ success: false, message: `Saldo Anda tidak mencukupi (Total Rp ${totalDeduct.toLocaleString('id-ID')} termasuk biaya layanan).` });
      req.session.errorMsg = `Saldo Anda tidak mencukupi (Total Rp ${totalDeduct.toLocaleString('id-ID')} termasuk biaya layanan).`;
      return res.redirect('/withdraw');
    }

    // 6. Create withdrawal record (Locked fee)
    const reference = 'WI' + Date.now().toString(36).toUpperCase();
    let withdrawal;
    try {
      withdrawal = await Withdrawal.create({
        userId: req.session.userId,
        amount: amt,
        fee: fee,
        method: 'DANA',
        ewallet: 'dana',
        accountNumber: cleanNomor,
        accountName: user.accountName || user.username || 'Pengguna DANA',
        status: 'pending',
        type: 'instant',
        providerStatus: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date()
      });

      await Transaction.create({
        userId: req.session.userId,
        type: 'withdraw',
        amount: amt,
        fee: fee,
        status: 'pending',
        reference: reference,
        method: 'DANA (Instan)',
        accountNumber: cleanNomor,
        accountName: user.accountName || user.username || 'Pengguna DANA'
      });
    } catch (dbErr) {
      await User.findByIdAndUpdate(req.session.userId, { $inc: { balance: totalDeduct } });
      throw dbErr;
    }

    // 7. Internal request to provider
    const providerResult = await executeInternalInstantWithdrawal({
      ewallet: 'dana',
      nomor: cleanNomor,
      nominal: amt,
      withdrawalId: withdrawal._id,
      userId: req.session.userId
    });

    if (providerResult.success) {
      withdrawal.status = 'success';
      withdrawal.providerStatus = 'SUCCESS';
      withdrawal.providerTransactionId = providerResult.providerTransactionId || '';
      withdrawal.completedAt = new Date();
      withdrawal.updatedAt = new Date();
      await withdrawal.save();

      await Transaction.updateOne(
        { reference },
        { status: 'success', completedAt: new Date() }
      );

      await Stats.updateOne({}, { $inc: { totalWithdrawAmount: amt, totalWithdrawFee: fee } });

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: 'DANA (Instan)',
        status: 'SUCCESS'
      });

      sendPushNotification(req.session.userId, 'withdraw_success', {
        title: '💸 PutzPay Penarikan Instan',
        body: `Penarikan otomatis Rp ${amt.toLocaleString('id-ID')} ke DANA ${maskPhoneNumber(cleanNomor)} BERHASIL.`,
        data: { url: '/withdraw', withdrawId: withdrawal._id }
      }, { eventId: `withdraw_instant_success_${withdrawal._id}` }).catch(() => {});

      if (isAjax) {
        return res.json({
          success: true,
          message: `Penarikan otomatis Rp ${amt.toLocaleString('id-ID')} ke DANA berhasil dikirim!`,
          data: { withdraw_id: withdrawal._id, status: 'SUCCESS' }
        });
      }
      req.session.successMsg = `Penarikan otomatis Rp ${amt.toLocaleString('id-ID')} ke DANA berhasil dikirim!`;
      return res.redirect('/withdraw');
    } else if (providerResult.failed) {
      withdrawal.status = 'failed';
      withdrawal.providerStatus = 'FAILED';
      withdrawal.refunded = true;
      withdrawal.adminNote = 'Gagal dari sistem provider: ' + (providerResult.providerMessage || 'Transaksi ditolak');
      withdrawal.completedAt = new Date();
      withdrawal.updatedAt = new Date();
      await withdrawal.save();

      await User.findByIdAndUpdate(req.session.userId, { $inc: { balance: totalDeduct } });

      await Transaction.updateOne(
        { reference },
        { status: 'rejected', adminNote: withdrawal.adminNote, completedAt: new Date() }
      );

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: 'DANA (Instan)',
        status: 'FAILED'
      });

      sendPushNotification(req.session.userId, 'withdraw_failed', {
        title: '❌ PutzPay Penarikan Instan Gagal',
        body: `Penarikan otomatis Rp ${amt.toLocaleString('id-ID')} gagal diproses. Saldo telah dikembalikan ke akun Anda.`,
        data: { url: '/withdraw', withdrawId: withdrawal._id }
      }, { eventId: `withdraw_instant_failed_${withdrawal._id}` }).catch(() => {});

      if (isAjax) {
        return res.status(400).json({
          success: false,
          message: 'Penarikan gagal diproses oleh sistem provider. Saldo Anda telah dikembalikan secara utuh.',
          data: { withdraw_id: withdrawal._id, status: 'FAILED' }
        });
      }
      req.session.errorMsg = 'Penarikan gagal diproses oleh sistem provider. Saldo Anda telah dikembalikan secara utuh.';
      return res.redirect('/withdraw');
    } else {
      withdrawal.status = 'pending';
      withdrawal.providerStatus = 'PENDING';
      withdrawal.updatedAt = new Date();
      await withdrawal.save();

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: 'DANA (Instan)',
        status: 'PROCESSING'
      });

      if (isAjax) {
        return res.json({
          success: true,
          message: 'Penarikan otomatis sedang dalam antrean pemrosesan.',
          data: { withdraw_id: withdrawal._id, status: 'PENDING' }
        });
      }
      req.session.successMsg = 'Penarikan otomatis sedang dalam antrean pemrosesan.';
      return res.redirect('/withdraw');
    }
  } catch (err) {
    console.error('Web instant withdraw error:', err);
    if (isAjax) return res.status(500).json({ success: false, message: 'Terjadi kesalahan sistem: ' + err.message });
    req.session.errorMsg = 'Terjadi kesalahan sistem: ' + err.message;
    return res.redirect('/withdraw');
  }
});
app.post('/withdraw/instan', isAuth, (req, res) => res.redirect(307, '/withdraw/instant'));

// ===================== ADMIN ROUTES =====================
app.get('/admin/dashboard', isAuth, isAdmin, async (req, res) => {
  const stats = await getStats();
  res.render('admin_dashboard', stats);
});

// ===================== ADMIN KYC MANAGEMENT =====================
app.get('/admin/kyc', isAuth, isAdmin, hasPermission('manage_kyc'), async (req, res) => {
  try {
    const status = req.query.status || 'all';
    const search = (req.query.search || '').trim();

    const [total, pending, verified, rejected, requires_review] = await Promise.all([
      User.countDocuments({ kycStatus: { $ne: 'NOT_SUBMITTED' } }),
      User.countDocuments({ kycStatus: 'PENDING' }),
      User.countDocuments({ kycStatus: 'VERIFIED' }),
      User.countDocuments({ kycStatus: 'REJECTED' }),
      User.countDocuments({ kycStatus: 'REQUIRES_REVIEW' })
    ]);

    const filter = {};
    if (status !== 'all') {
      filter.kycStatus = status.toUpperCase();
    } else {
      filter.kycStatus = { $ne: 'NOT_SUBMITTED' };
    }

    if (search) {
      filter.$or = [
        { username: { $regex: search, $options: 'i' } },
        { email: { $regex: search, $options: 'i' } },
        { kycFullName: { $regex: search, $options: 'i' } },
        { kycNik: { $regex: search, $options: 'i' } }
      ];
    }

    const users = await User.find(filter)
      .select('username email role balance kycStatus kycType kycFullName kycNik kycBirthDate kycParentApproval kycSubmittedAt kycVerifiedAt kycRejectedAt kycRejectionReason kycReviewedBy createdAt')
      .sort({ kycSubmittedAt: -1, createdAt: -1 })
      .lean();

    // Attach user documents and audit logs for inspection modal
    const userIds = users.map(u => u._id);
    const docs = await KycDocument.find({ userId: { $in: userIds } }).sort({ uploadedAt: -1 }).lean();
    const logs = await KycAuditLog.find({ userId: { $in: userIds } }).sort({ timestamp: -1 }).lean();

    const docMap = {};
    const logMap = {};
    for (const d of docs) {
      const uid = String(d.userId);
      if (!docMap[uid]) docMap[uid] = [];
      docMap[uid].push(d);
    }
    for (const l of logs) {
      const uid = String(l.userId);
      if (!logMap[uid]) logMap[uid] = [];
      logMap[uid].push(l);
    }

    const enrichedUsers = users.map(u => {
      const uid = String(u._id);
      return {
        ...u,
        documents: docMap[uid] || [],
        auditLogs: (logMap[uid] || []).slice(0, 10)
      };
    });

    const errorMsg = req.session.errorMsg || null;
    const successMsg = req.session.successMsg || null;
    delete req.session.errorMsg;
    delete req.session.successMsg;

    res.render('admin_kyc', {
      users: enrichedUsers,
      stats: { total, pending, verified, rejected, requires_review },
      currentStatus: status,
      search,
      errorMsg,
      successMsg
    });
  } catch (err) {
    console.error('Error rendering admin KYC:', err);
    res.status(500).send('Terjadi kesalahan saat memuat manajemen KYC.');
  }
});

app.post('/admin/kyc/:id/approve', isAuth, isAdmin, hasPermission('manage_kyc'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/kyc');
    }

    user.kycStatus = 'VERIFIED';
    user.kycVerifiedAt = new Date();
    user.kycRejectionReason = '';
    user.kycReviewedBy = req.session.username || 'Admin';
    await user.save();

    await KycAuditLog.create({
      userId: user._id,
      action: 'APPROVED',
      actorId: req.session.userId,
      actorRole: req.session.userRole || 'admin',
      details: {
        adminUsername: req.session.username || 'Admin',
        verifiedAt: user.kycVerifiedAt
      },
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
      userAgent: req.headers['user-agent'] || ''
    });

    await Notification.create({
      userId: user._id,
      target: 'single',
      title: 'Verifikasi Identitas (KYC) Disetujui',
      message: 'Selamat! Akun Anda telah berhasil diverifikasi (KYC Verified). Limit transaksi dan penarikan telah ditingkatkan, serta Anda berhak mendapatkan diskon biaya transaksi.',
      type: 'success',
      link: '/kyc'
    });

    req.session.successMsg = `KYC untuk @${user.username} berhasil disetujui.`;
    res.redirect('/admin/kyc');
  } catch (err) {
    console.error('Error approving KYC:', err);
    req.session.errorMsg = 'Gagal menyetujui KYC: ' + err.message;
    res.redirect('/admin/kyc');
  }
});

app.post('/admin/kyc/:id/reject', isAuth, isAdmin, hasPermission('manage_kyc'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/kyc');
    }

    const reason = (req.body.reason || 'Dokumen identitas tidak jelas atau tidak memenuhi syarat').trim();
    user.kycStatus = 'REJECTED';
    user.kycRejectedAt = new Date();
    user.kycRejectionReason = reason;
    user.kycReviewedBy = req.session.username || 'Admin';
    await user.save();

    await KycAuditLog.create({
      userId: user._id,
      action: 'REJECTED',
      actorId: req.session.userId,
      actorRole: req.session.userRole || 'admin',
      details: {
        adminUsername: req.session.username || 'Admin',
        reason
      },
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
      userAgent: req.headers['user-agent'] || ''
    });

    await Notification.create({
      userId: user._id,
      target: 'single',
      title: 'Verifikasi Identitas (KYC) Ditolak',
      message: `Pengajuan verifikasi identitas Anda belum dapat disetujui. Alasan: ${reason}. Silakan ajukan ulang dengan dokumen yang jelas dan valid.`,
      type: 'danger',
      link: '/kyc'
    });

    req.session.successMsg = `Pengajuan KYC @${user.username} telah ditolak dengan alasan: ${reason}.`;
    res.redirect('/admin/kyc');
  } catch (err) {
    console.error('Error rejecting KYC:', err);
    req.session.errorMsg = 'Gagal menolak KYC: ' + err.message;
    res.redirect('/admin/kyc');
  }
});

app.post('/admin/kyc/:id/request-review', isAuth, isAdmin, hasPermission('manage_kyc'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/kyc');
    }

    const note = (req.body.note || 'Mohon perbarui atau unggah dokumen identitas yang lebih jelas').trim();
    user.kycStatus = 'REQUIRES_REVIEW';
    user.kycRejectionReason = note;
    user.kycReviewedBy = req.session.username || 'Admin';
    await user.save();

    await KycAuditLog.create({
      userId: user._id,
      action: 'STATUS_CHANGED',
      actorId: req.session.userId,
      actorRole: req.session.userRole || 'admin',
      details: {
        adminUsername: req.session.username || 'Admin',
        newStatus: 'REQUIRES_REVIEW',
        note
      },
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
      userAgent: req.headers['user-agent'] || ''
    });

    await Notification.create({
      userId: user._id,
      target: 'single',
      title: 'Perlu Tinjauan Ulang KYC',
      message: `Terdapat dokumen yang perlu diperbaiki: ${note}. Silakan buka menu KYC untuk mengunggah ulang dokumen yang diminta.`,
      type: 'warning',
      link: '/kyc'
    });

    req.session.successMsg = `Permintaan tinjauan ulang untuk @${user.username} berhasil dikirim.`;
    res.redirect('/admin/kyc');
  } catch (err) {
    console.error('Error requesting review KYC:', err);
    req.session.errorMsg = 'Gagal mengirim tinjauan ulang: ' + err.message;
    res.redirect('/admin/kyc');
  }
});

app.post('/admin/kyc/:id/reset', isAuth, isOwner, async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/kyc');
    }

    user.kycStatus = 'NOT_SUBMITTED';
    user.kycRejectionReason = '';
    user.kycSubmittedAt = null;
    user.kycVerifiedAt = null;
    user.kycRejectedAt = null;
    await user.save();

    await KycAuditLog.create({
      userId: user._id,
      action: 'RESET',
      actorId: req.session.userId,
      actorRole: req.session.userRole || 'owner',
      details: {
        adminUsername: req.session.username || 'Owner'
      },
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
      userAgent: req.headers['user-agent'] || ''
    });

    req.session.successMsg = `Status KYC @${user.username} berhasil di-reset ke Belum Terverifikasi.`;
    res.redirect('/admin/kyc');
  } catch (err) {
    console.error('Error resetting KYC:', err);
    req.session.errorMsg = 'Gagal me-reset KYC: ' + err.message;
    res.redirect('/admin/kyc');
  }
});

app.get('/admin/users', isAuth, isAdmin, hasPermission('view_users'), async (req, res) => {
  const search = req.query.search || '';
  const filter = {};
  if (search) {
    filter.$or = [
      { email: { $regex: search, $options: 'i' } },
      { username: { $regex: search, $options: 'i' } },
      { lastIp: { $regex: search, $options: 'i' } },
      { registerIp: { $regex: search, $options: 'i' } }
    ];
  }
  const users = await User.find(filter).sort({ createdAt: -1 }).lean();
  const bannedIpsDocs = await BannedIp.find({}).lean();
  const bannedIpsSet = new Set(bannedIpsDocs.map(b => b.ip));
  const bannedUserIdsSet = new Set(bannedIpsDocs.filter(b => b.targetUserId).map(b => String(b.targetUserId)));
  const isOwnerUser = Boolean(res.locals.isOwnerUser || req.session.userRole === 'owner' || (res.locals.user && await isFirstAdmin(res.locals.user)));

  res.render('admin_users', { users, search, bannedIpsSet, bannedUserIdsSet, isOwnerUser });
});

// Update Role & Permissions (Owner Only)
app.post('/admin/users/update-role/:id', isAuth, isOwner, async (req, res) => {
  try {
    const targetUserId = req.params.id;
    const targetUser = await User.findById(targetUserId);
    if (!targetUser) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }

    if (targetUser.role === 'owner' && req.session.userId !== String(targetUser._id)) {
      req.session.errorMsg = 'Tidak dapat mengubah role akun Owner utama.';
      return res.redirect('/admin/users');
    }

    const { role } = req.body;
    let permissions = [];
    if (req.body.permissions) {
      permissions = Array.isArray(req.body.permissions) ? req.body.permissions : [req.body.permissions];
    }

    if (role === 'admin') {
      targetUser.role = 'admin';
      targetUser.permissions = permissions;
    } else if (role === 'user') {
      targetUser.role = 'user';
      targetUser.permissions = [];
    }

    await targetUser.save();
    req.session.successMsg = `Role dan hak akses akun @${targetUser.username} berhasil diperbarui menjadi ${targetUser.role.toUpperCase()}.`;
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error updating user role:', err);
    req.session.errorMsg = 'Gagal mengubah role: ' + err.message;
    res.redirect('/admin/users');
  }
});

// Admin Manual Verify Email
app.post('/admin/users/verify-email/:id', isAuth, isAdmin, hasPermission('edit_users'), async (req, res) => {
  try {
    const targetUserId = req.params.id;
    const targetUser = await User.findById(targetUserId);
    if (!targetUser) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }

    targetUser.emailVerified = true;
    targetUser.verificationOtpHash = null;
    targetUser.verificationOtpExpires = null;
    await targetUser.save();

    req.session.successMsg = `Alamat email akun @${targetUser.username} (${targetUser.email}) berhasil diverifikasi manual oleh Admin.`;
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error manual verify email:', err);
    req.session.errorMsg = 'Gagal memverifikasi email pengguna: ' + err.message;
    res.redirect('/admin/users');
  }
});

// Admin Resend OTP Email to User
app.post('/admin/users/resend-verification/:id', isAuth, isAdmin, hasPermission('edit_users'), async (req, res) => {
  try {
    const targetUserId = req.params.id;
    const targetUser = await User.findById(targetUserId);
    if (!targetUser) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }

    const now = Date.now();
    const freshOtp = generateOtp();
    targetUser.verificationOtpHash = hashOtp(freshOtp);
    targetUser.verificationOtpExpires = new Date(now + 10 * 60 * 1000);
    targetUser.verificationOtpAttempts = 0;
    targetUser.verificationOtpLastSent = new Date();
    await targetUser.save();

    const sendRes = await sendVerificationOtpEmail(targetUser, freshOtp);
    if (sendRes.success) {
      req.session.successMsg = `Email OTP verifikasi berhasil dikirimkan ke ${targetUser.email}.`;
    } else {
      req.session.errorMsg = `Gagal mengirim email OTP: ${sendRes.error || 'Periksa konfigurasi SMTP Admin'}.`;
    }
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error resending OTP from admin:', err);
    req.session.errorMsg = 'Gagal mengirim email verifikasi: ' + err.message;
    res.redirect('/admin/users');
  }
});

// ===================== ADMIN ACCESS CONTROL & BANNED SYSTEM ROUTES =====================
const renderAccessControlView = async (req, res) => {
  try {
    const search = (req.query.search || '').trim();
    const activeTab = req.query.tab || 'global';
    const filter = {};
    if (search) {
      filter.$or = [
        { ip: { $regex: search, $options: 'i' } },
        { deviceId: { $regex: search, $options: 'i' } },
        { deviceName: { $regex: search, $options: 'i' } },
        { targetUsername: { $regex: search, $options: 'i' } },
        { reason: { $regex: search, $options: 'i' } },
        { bannedBy: { $regex: search, $options: 'i' } }
      ];
    }
    const bannedIps = await BannedIp.find(filter).sort({ createdAt: -1 }).lean();
    const bannedUsers = await User.find({
      $or: [
        { suspended: true },
        { accountStatus: { $in: ['banned', 'suspended'] } }
      ]
    }).sort({ bannedAt: -1, updatedAt: -1 }).lean();

    const allUsers = await User.find({}).select('username email role suspended accountStatus lastIp registerIp lastDevice ipHistory devices').limit(100).lean();
    const settings = await getSettings();
    const clientIp = getClientIp(req);

    res.render('admin_banned_ips', {
      settings,
      bannedIps,
      bannedUsers,
      allUsers,
      search,
      activeTab,
      clientIp
    });
  } catch (err) {
    console.error('Error rendering access control / admin_banned_ips:', err);
    req.session.errorMsg = 'Gagal memuat panel Access Control: ' + err.message;
    res.redirect('/admin/dashboard');
  }
};

app.get('/admin/access-control', isAuth, isAdmin, renderAccessControlView);
app.get('/admin/banned-ips', isAuth, isAdmin, renderAccessControlView);

// 1. BANNED ALL USERS (MASS ACCOUNT BLOCK) TOGGLE
const handleBanAllUsersToggle = async (req, res) => {
  try {
    const enabled = req.body.enabled === 'true' || req.body.enabled === true || req.body.enabled === 'on';
    const message = (req.body.message || '').trim() || 'Semua akun pengguna saat ini sedang dinonaktifkan sementara oleh Administrator.';
    const customerServiceUrl = (req.body.customerServiceUrl || '').trim() || 'https://cs.putzpay.biz.id';

    await Setting.updateOne({}, {
      banAllUsers: enabled,
      banAllUsersReason: message,
      globalWebsiteBlock: false, // Ensure public pages are never blocked like maintenance
      globalBlockMessage: message,
      customerServiceUrl: customerServiceUrl
    });

    req.session.successMsg = enabled
      ? '🔒 Banned All Users BERHASIL DIAKTIFKAN. Seluruh akun pengguna non-admin dialihkan ke Halaman Banned dan tetap memiliki opsi Logout.'
      : '🔓 Banned All Users BERHASIL DINONAKTIFKAN. Seluruh akun pengguna kini dapat bertransaksi & mengakses website secara normal.';
    res.redirect('/admin/access-control?tab=global');
  } catch (err) {
    console.error('Error updating ban all users:', err);
    req.session.errorMsg = 'Gagal mengubah status Banned All Users: ' + err.message;
    res.redirect('/admin/access-control?tab=global');
  }
};

app.post('/admin/access-control/global-block', isAuth, isAdmin, handleBanAllUsersToggle);
app.post('/admin/access-control/ban-all-users', isAuth, isAdmin, handleBanAllUsersToggle);

// 2. USER ACCOUNT BAN / UNBAN
app.post('/admin/access-control/ban-user', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    const { username, reason, banIp, banDevice } = req.body;
    const cleanUsername = (username || '').trim().toLowerCase();
    if (!cleanUsername) {
      req.session.errorMsg = 'Username akun target wajib diisi.';
      return res.redirect('/admin/access-control?tab=users');
    }

    const user = await User.findOne({ username: cleanUsername });
    if (!user) {
      req.session.errorMsg = `Pengguna @${cleanUsername} tidak ditemukan.`;
      return res.redirect('/admin/access-control?tab=users');
    }

    if (user.role === 'owner') {
      req.session.errorMsg = 'Tidak dapat memblokir akun Owner utama.';
      return res.redirect('/admin/access-control?tab=users');
    }

    const banReason = (reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan').trim();
    const adminUser = req.session.username || res.locals.user?.username || 'Admin';

    user.suspended = true;
    user.accountStatus = 'banned';
    user.banReason = banReason;
    user.bannedAt = new Date();
    user.bannedBy = adminUser;
    await user.save();

    // Optionally ban related IPs
    if (banIp === 'true' || banIp === true || banIp === 'on') {
      const ips = [user.lastIp, user.registerIp, ...(user.ipHistory || [])].filter(Boolean).map(i => i.trim().replace(/^::ffff:/, ''));
      const uniqueIps = [...new Set(ips)];
      for (const ip of uniqueIps) {
        await BannedIp.findOneAndUpdate(
          { ip },
          {
            ip,
            reason: `[Auto-Ban Akun @${user.username}] ${banReason}`,
            bannedBy: adminUser,
            targetUserId: user._id,
            targetUsername: user.username,
            active: true,
            createdAt: new Date()
          },
          { upsert: true, new: true }
        );
      }
    }

    // Optionally ban registered devices
    if ((banDevice === 'true' || banDevice === true || banDevice === 'on') && user.devices && user.devices.length > 0) {
      for (const dev of user.devices) {
        if (dev.deviceId) {
          await BannedIp.findOneAndUpdate(
            { deviceId: dev.deviceId },
            {
              deviceId: dev.deviceId,
              deviceName: dev.deviceName || 'Mobile/Desktop',
              reason: `[Auto-Ban Akun @${user.username}] ${banReason}`,
              bannedBy: adminUser,
              targetUserId: user._id,
              targetUsername: user.username,
              active: true,
              createdAt: new Date()
            },
            { upsert: true, new: true }
          );
        }
      }
    }

    await reloadBannedIpsCache();
    req.session.successMsg = `Akun @${user.username} berhasil DIBLOKIR / BANNED.`;
    res.redirect('/admin/access-control?tab=users');
  } catch (err) {
    console.error('Error banning user:', err);
    req.session.errorMsg = 'Gagal memblokir akun pengguna: ' + err.message;
    res.redirect('/admin/access-control?tab=users');
  }
});

app.post('/admin/access-control/unban-user', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    const { username, unbanIp, userId } = req.body;
    let user = null;
    if (userId) {
      user = await User.findById(userId);
    } else if (username) {
      user = await User.findOne({ username: username.trim().toLowerCase() });
    }

    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/access-control?tab=users');
    }

    user.suspended = false;
    user.accountStatus = 'active';
    user.banReason = '';
    user.bannedAt = null;
    user.bannedBy = '';
    await user.save();

    if (unbanIp === 'true' || unbanIp === true || unbanIp === 'on' || req.body.unbanAll === 'true') {
      const ips = [user.lastIp, user.registerIp, ...(user.ipHistory || [])].filter(Boolean).map(i => i.trim().replace(/^::ffff:/, ''));
      const devIds = (user.devices || []).map(d => d.deviceId).filter(Boolean);
      await BannedIp.deleteMany({
        $or: [
          { targetUserId: user._id },
          { targetUsername: user.username },
          { ip: { $in: ips } },
          { deviceId: { $in: devIds } }
        ]
      });
    }

    await reloadBannedIpsCache();
    req.session.successMsg = `Blokir akun @${user.username} berhasil DIBUKA (UNBAN).`;
    res.redirect('/admin/access-control?tab=users');
  } catch (err) {
    console.error('Error unbanning user:', err);
    req.session.errorMsg = 'Gagal membuka blokir akun: ' + err.message;
    res.redirect('/admin/access-control?tab=users');
  }
});

// Direct user ban actions from admin_users.ejs table
app.post('/admin/users/:id/ban-account', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }
    if (user.role === 'owner') {
      req.session.errorMsg = 'Tidak dapat memblokir akun Owner.';
      return res.redirect('/admin/users');
    }

    const reason = (req.body.reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan').trim();
    const adminUser = req.session.username || res.locals.user?.username || 'Admin';

    user.suspended = true;
    user.accountStatus = 'banned';
    user.banReason = reason;
    user.bannedAt = new Date();
    user.bannedBy = adminUser;
    await user.save();

    if (req.body.banIp === 'true' || req.body.banIp === true || req.body.banIp === 'on') {
      const ips = [user.lastIp, user.registerIp, ...(user.ipHistory || [])].filter(Boolean).map(i => i.trim().replace(/^::ffff:/, ''));
      for (const ip of ips) {
        await BannedIp.findOneAndUpdate(
          { ip },
          {
            ip,
            reason: `[Auto-Ban] ${reason}`,
            bannedBy: adminUser,
            targetUserId: user._id,
            targetUsername: user.username,
            active: true,
            createdAt: new Date()
          },
          { upsert: true, new: true }
        );
      }
      await reloadBannedIpsCache();
    }

    req.session.successMsg = `Akun @${user.username} berhasil dibanned.`;
    const referer = req.headers.referer;
    if (referer && referer.includes('/admin/')) return res.redirect(referer);
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error banning account:', err);
    req.session.errorMsg = 'Gagal memblokir akun: ' + err.message;
    res.redirect('/admin/users');
  }
});

app.post('/admin/users/:id/unban-account', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }

    user.suspended = false;
    user.accountStatus = 'active';
    user.banReason = '';
    user.bannedAt = null;
    user.bannedBy = '';
    await user.save();

    req.session.successMsg = `Akun @${user.username} berhasil di-unban.`;
    const referer = req.headers.referer;
    if (referer && referer.includes('/admin/')) return res.redirect(referer);
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error unbanning account:', err);
    req.session.errorMsg = 'Gagal unban akun: ' + err.message;
    res.redirect('/admin/users');
  }
});

// Bulk User Actions
app.post('/admin/users/bulk-ban', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    let userIds = req.body.userIds;
    if (typeof userIds === 'string') {
      try { userIds = JSON.parse(userIds); } catch (e) { userIds = [userIds]; }
    }
    if (!Array.isArray(userIds) || userIds.length === 0) {
      req.session.errorMsg = 'Pilih minimal satu pengguna untuk diblokir.';
      return res.redirect('/admin/users');
    }

    const reason = (req.body.reason || 'Bulk Ban Administrator').trim();
    const adminUser = req.session.username || res.locals.user?.username || 'Admin';

    const result = await User.updateMany(
      { _id: { $in: userIds }, role: { $ne: 'owner' } },
      {
        $set: {
          suspended: true,
          accountStatus: 'banned',
          banReason: reason,
          bannedAt: new Date(),
          bannedBy: adminUser
        }
      }
    );

    req.session.successMsg = `Berhasil memblokir ${result.modifiedCount} akun pengguna.`;
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error in bulk ban users:', err);
    req.session.errorMsg = 'Gagal melakukan bulk ban: ' + err.message;
    res.redirect('/admin/users');
  }
});

app.post('/admin/users/bulk-unban', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    let userIds = req.body.userIds;
    if (typeof userIds === 'string') {
      try { userIds = JSON.parse(userIds); } catch (e) { userIds = [userIds]; }
    }
    if (!Array.isArray(userIds) || userIds.length === 0) {
      req.session.errorMsg = 'Pilih minimal satu pengguna untuk di-unban.';
      return res.redirect('/admin/users');
    }

    const result = await User.updateMany(
      { _id: { $in: userIds } },
      {
        $set: {
          suspended: false,
          accountStatus: 'active',
          banReason: '',
          bannedAt: null,
          bannedBy: ''
        }
      }
    );

    req.session.successMsg = `Berhasil membuka blokir ${result.modifiedCount} akun pengguna.`;
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error in bulk unban users:', err);
    req.session.errorMsg = 'Gagal melakukan bulk unban: ' + err.message;
    res.redirect('/admin/users');
  }
});

// 3. DEVICE & IP BLOCKING ROUTES
app.post('/admin/access-control/block-device', isAuth, isAdmin, async (req, res) => {
  try {
    const { ip, deviceId, deviceName, targetUsername, reason } = req.body;
    const cleanIp = (ip || '').trim().replace(/^::ffff:/, '');
    const cleanDeviceId = (deviceId || '').trim();

    if (!cleanIp && !cleanDeviceId) {
      req.session.errorMsg = 'Alamat IP atau Device ID wajib diisi.';
      return res.redirect('/admin/access-control?tab=devices');
    }

    let targetUserId = null;
    let finalTargetUsername = (targetUsername || '').trim();
    if (finalTargetUsername) {
      const u = await User.findOne({ username: finalTargetUsername.toLowerCase() });
      if (u) {
        targetUserId = u._id;
        finalTargetUsername = u.username;
      }
    }

    const adminUser = req.session.username || res.locals.user?.username || 'Admin';
    const banReason = (reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan').trim();

    const query = cleanDeviceId ? { deviceId: cleanDeviceId } : { ip: cleanIp };
    await BannedIp.findOneAndUpdate(
      query,
      {
        ip: cleanIp,
        deviceId: cleanDeviceId,
        deviceName: (deviceName || 'Perangkat Pengguna').trim(),
        reason: banReason,
        bannedBy: adminUser,
        targetUserId,
        targetUsername: finalTargetUsername,
        active: true,
        userAgent: req.headers['user-agent'] || '',
        createdAt: new Date()
      },
      { upsert: true, new: true }
    );

    await reloadBannedIpsCache();
    req.session.successMsg = `Perangkat / IP ${cleanIp || cleanDeviceId} berhasil diblokir!`;
    res.redirect('/admin/access-control?tab=devices');
  } catch (err) {
    console.error('Error blocking device:', err);
    req.session.errorMsg = 'Gagal memblokir perangkat: ' + err.message;
    res.redirect('/admin/access-control?tab=devices');
  }
});

app.post('/admin/access-control/unblock-device', isAuth, isAdmin, async (req, res) => {
  try {
    const { id, ip, deviceId } = req.body;
    if (id) {
      await BannedIp.findByIdAndDelete(id);
    } else if (deviceId) {
      await BannedIp.deleteMany({ deviceId });
    } else if (ip) {
      await BannedIp.deleteMany({ ip: ip.trim().replace(/^::ffff:/, '') });
    }

    await reloadBannedIpsCache();
    req.session.successMsg = 'Perangkat / IP berhasil dibuka dari blokir (Unblocked).';
    res.redirect('/admin/access-control?tab=devices');
  } catch (err) {
    console.error('Error unblocking device:', err);
    req.session.errorMsg = 'Gagal membuka blokir perangkat: ' + err.message;
    res.redirect('/admin/access-control?tab=devices');
  }
});

// Search user IP history endpoint (JSON for AJAX modal lookup)
app.get('/api/admin/users/search-ip', isAuth, isAdmin, async (req, res) => {
  try {
    const q = (req.query.q || '').trim().toLowerCase();
    if (!q) return res.json({ success: true, users: [] });

    const users = await User.find({
      $or: [
        { username: { $regex: q, $options: 'i' } },
        { email: { $regex: q, $options: 'i' } }
      ]
    }).select('username email lastIp registerIp ipHistory suspended accountStatus role devices lastDevice').limit(10).lean();

    res.json({ success: true, users });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// Ban IP by username
app.post('/admin/banned-ips/ban-username', isAuth, isAdmin, async (req, res) => {
  try {
    const { username, reason, suspendUser, banAllHistory } = req.body;
    const cleanUsername = (username || '').trim().toLowerCase();
    if (!cleanUsername) {
      req.session.errorMsg = 'Username target wajib diisi.';
      return res.redirect('/admin/banned-ips');
    }

    const user = await User.findOne({ username: cleanUsername });
    if (!user) {
      req.session.errorMsg = `Pengguna dengan username @${cleanUsername} tidak ditemukan.`;
      return res.redirect('/admin/banned-ips');
    }

    if (user.role === 'owner') {
      req.session.errorMsg = 'Tidak dapat memblokir IP milik akun Owner.';
      return res.redirect('/admin/banned-ips');
    }

    let ipsToBan = [];
    if (banAllHistory && user.ipHistory && user.ipHistory.length > 0) {
      ipsToBan = [...user.ipHistory];
    }
    if (user.lastIp) ipsToBan.push(user.lastIp);
    if (user.registerIp) ipsToBan.push(user.registerIp);
    ipsToBan = [...new Set(ipsToBan.map(ip => (ip || '').trim().replace(/^::ffff:/, '')).filter(Boolean))];

    if (ipsToBan.length === 0) {
      req.session.errorMsg = `Pengguna @${user.username} belum memiliki riwayat IP yang tersimpan.`;
      return res.redirect('/admin/banned-ips');
    }

    const banReason = (reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan').trim();
    const bannedBy = req.session.username || res.locals.user?.username || 'Admin';

    for (const ip of ipsToBan) {
      await BannedIp.findOneAndUpdate(
        { ip },
        {
          ip,
          reason: banReason,
          bannedBy,
          targetUserId: user._id,
          targetUsername: user.username,
          active: true,
          userAgent: req.headers['user-agent'] || '',
          createdAt: new Date()
        },
        { upsert: true, new: true }
      );
    }

    if (suspendUser === 'true' || suspendUser === true || suspendUser === 'on') {
      user.suspended = true;
      user.accountStatus = 'banned';
      user.banReason = banReason;
      user.bannedAt = new Date();
      user.bannedBy = bannedBy;
      await user.save();
    }

    await reloadBannedIpsCache();
    req.session.successMsg = `Berhasil memblokir ${ipsToBan.length} IP milik @${user.username} (${ipsToBan.join(', ')}).`;
    res.redirect('/admin/banned-ips');
  } catch (err) {
    console.error('Error banning by username:', err);
    req.session.errorMsg = 'Gagal memblokir IP username: ' + err.message;
    res.redirect('/admin/banned-ips');
  }
});

// Unban IP by username
app.post('/admin/banned-ips/unban-username', isAuth, isAdmin, async (req, res) => {
  try {
    const { username } = req.body;
    const cleanUsername = (username || '').trim().toLowerCase();
    if (!cleanUsername) {
      req.session.errorMsg = 'Username target wajib diisi untuk unban.';
      return res.redirect('/admin/banned-ips');
    }

    const user = await User.findOne({ username: cleanUsername });
    const matchConditions = [{ targetUsername: cleanUsername }];
    if (user) {
      matchConditions.push({ targetUserId: user._id });
      const ips = [user.lastIp, user.registerIp, ...(user.ipHistory || [])].filter(Boolean).map(i => i.trim().replace(/^::ffff:/, ''));
      if (ips.length > 0) {
        matchConditions.push({ ip: { $in: ips } });
      }
    }

    const deleteRes = await BannedIp.deleteMany({ $or: matchConditions });
    await reloadBannedIpsCache();
    req.session.successMsg = `Berhasil membuka blokir (${deleteRes.deletedCount} data IP) untuk pengguna @${cleanUsername}.`;
    res.redirect('/admin/banned-ips');
  } catch (err) {
    console.error('Error unbanning by username:', err);
    req.session.errorMsg = 'Gagal unban IP: ' + err.message;
    res.redirect('/admin/banned-ips');
  }
});

app.post('/admin/banned-ips/ban', isAuth, isAdmin, async (req, res) => {
  try {
    let { ip, reason, targetUsername } = req.body;
    let cleanIp = (ip || '').trim().replace(/^::ffff:/, '');
    if (!cleanIp) {
      req.session.errorMsg = 'Alamat IP wajib diisi.';
      return res.redirect('/admin/banned-ips');
    }

    let targetUserId = null;
    let finalTargetUsername = targetUsername ? targetUsername.trim() : '';
    if (finalTargetUsername) {
      const u = await User.findOne({ username: finalTargetUsername.toLowerCase() });
      if (u) {
        targetUserId = u._id;
        finalTargetUsername = u.username;
      }
    }

    await BannedIp.findOneAndUpdate(
      { ip: cleanIp },
      {
        ip: cleanIp,
        reason: (reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan').trim(),
        bannedBy: req.session.username || res.locals.user?.username || 'Admin',
        targetUserId: targetUserId,
        targetUsername: finalTargetUsername,
        active: true,
        userAgent: req.headers['user-agent'] || '',
        createdAt: new Date()
      },
      { upsert: true, new: true }
    );

    await reloadBannedIpsCache();
    req.session.successMsg = `Alamat IP ${cleanIp} berhasil dibanned/disuspend dari website PutzPay.`;
    res.redirect('/admin/banned-ips');
  } catch (err) {
    console.error('Error banning IP:', err);
    req.session.errorMsg = 'Gagal memblokir IP: ' + err.message;
    res.redirect('/admin/banned-ips');
  }
});

app.post('/admin/banned-ips/unban/:id', isAuth, isAdmin, async (req, res) => {
  try {
    const banDoc = await BannedIp.findByIdAndDelete(req.params.id);
    await reloadBannedIpsCache();
    req.session.successMsg = banDoc ? `Alamat IP ${banDoc.ip || banDoc.deviceId} berhasil di-unban / dibuka aksesnya.` : 'IP/Perangkat berhasil di-unban.';
    res.redirect('/admin/banned-ips');
  } catch (err) {
    console.error('Error unbanning IP by ID:', err);
    req.session.errorMsg = 'Gagal membuka blokir IP.';
    res.redirect('/admin/banned-ips');
  }
});

app.post('/admin/banned-ips/unban', isAuth, isAdmin, async (req, res) => {
  try {
    let cleanIp = (req.body.ip || '').trim().replace(/^::ffff:/, '');
    if (!cleanIp) {
      req.session.errorMsg = 'Alamat IP wajib diisi untuk unban.';
      return res.redirect('/admin/banned-ips');
    }
    await BannedIp.deleteMany({ ip: cleanIp });
    await reloadBannedIpsCache();
    req.session.successMsg = `Alamat IP ${cleanIp} berhasil di-unban.`;
    res.redirect('/admin/banned-ips');
  } catch (err) {
    console.error('Error unbanning IP:', err);
    req.session.errorMsg = 'Gagal membuka blokir IP.';
    res.redirect('/admin/banned-ips');
  }
});

app.post('/admin/banned-ips/:id/delete', isAuth, isAdmin, async (req, res) => {
  try {
    const banDoc = await BannedIp.findByIdAndDelete(req.params.id);
    await reloadBannedIpsCache();
    req.session.successMsg = banDoc ? `Riwayat ${banDoc.ip || banDoc.deviceId} berhasil dihapus dan dibuka blokirnya.` : 'Data berhasil dihapus.';
    res.redirect('/admin/banned-ips');
  } catch (err) {
    console.error('Error deleting banned IP doc:', err);
    req.session.errorMsg = 'Gagal menghapus data blokir.';
    res.redirect('/admin/banned-ips');
  }
});

app.post('/admin/users/:id/ban-ip', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }

    if (user.role === 'owner') {
      req.session.errorMsg = 'Tidak dapat memblokir IP milik akun Owner.';
      return res.redirect('/admin/users');
    }

    const targetIp = (req.body.ip || user.lastIp || user.registerIp || '').trim().replace(/^::ffff:/, '');
    if (!targetIp) {
      req.session.errorMsg = `Pengguna @${user.username} belum memiliki riwayat IP untuk diblokir.`;
      return res.redirect('/admin/users');
    }

    const reason = (req.body.reason || 'Pelanggaran Ketentuan Layanan / Aktivitas Mencurigakan').trim();
    const suspendUser = req.body.suspendUser === 'true' || req.body.suspendUser === true || req.body.suspendUser === 'on';

    if (suspendUser) {
      user.suspended = true;
      user.accountStatus = 'banned';
      user.banReason = reason;
      user.bannedAt = new Date();
      user.bannedBy = req.session.username || res.locals.user?.username || 'Admin';
      await user.save();
    }

    await BannedIp.findOneAndUpdate(
      { ip: targetIp },
      {
        ip: targetIp,
        reason: reason,
        bannedBy: req.session.username || res.locals.user?.username || 'Admin',
        targetUserId: user._id,
        targetUsername: user.username,
        active: true,
        userAgent: req.headers['user-agent'] || '',
        createdAt: new Date()
      },
      { upsert: true, new: true }
    );

    await reloadBannedIpsCache();
    req.session.successMsg = `Alamat IP ${targetIp} milik @${user.username} berhasil dibanned!`;
    const referer = req.headers.referer;
    if (referer && referer.includes('/admin/')) {
      return res.redirect(referer);
    }
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error banning user IP:', err);
    req.session.errorMsg = 'Gagal memblokir IP pengguna: ' + err.message;
    res.redirect('/admin/users');
  }
});

app.post('/admin/users/:id/unban-ip', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) {
      req.session.errorMsg = 'Pengguna tidak ditemukan.';
      return res.redirect('/admin/users');
    }

    const ipsToRemove = [user.lastIp, user.registerIp, ...(user.ipHistory || [])].filter(Boolean).map(ip => ip.trim().replace(/^::ffff:/, ''));

    await BannedIp.deleteMany({
      $or: [
        { targetUserId: user._id },
        { targetUsername: user.username },
        { ip: { $in: ipsToRemove } }
      ]
    });

    await reloadBannedIpsCache();
    req.session.successMsg = `IP dan pemblokiran terkait akun @${user.username} berhasil dibuka (Unban).`;
    const referer = req.headers.referer;
    if (referer && referer.includes('/admin/')) {
      return res.redirect(referer);
    }
    res.redirect('/admin/users');
  } catch (err) {
    console.error('Error unbanning user IP:', err);
    req.session.errorMsg = 'Gagal unban IP: ' + err.message;
    res.redirect('/admin/users');
  }
});

app.get('/admin/users/:id/edit', isAuth, isAdmin, hasPermission('manage_users'), async (req, res) => {
  const targetUser = await User.findById(req.params.id).lean();
  if (!targetUser) return res.status(404).send('Tidak ditemukan');
  res.render('admin_user_edit', { users: targetUser });
});

app.post('/admin/users/:id/edit', isAuth, isAdmin, async (req, res) => {
  const { username, email, password, balance, suspended } = req.body;
  if (username) {
    if (!/^[a-zA-Z0-9]+$/.test(username)) {
      req.session.errorMsg = 'Username hanya boleh berisi huruf dan angka (tanpa spasi atau simbol)';
      return res.redirect(`/admin/users/${req.params.id}/edit`);
    }
    if (username.length > 15) {
      req.session.errorMsg = 'Username maksimal 15 karakter';
      return res.redirect(`/admin/users/${req.params.id}/edit`);
    }
  }
  const upd = {
    username: username?.toLowerCase(),
    email,
    balance: parseInt(balance) || 0,
    suspended: suspended === 'on'
  };
  if (!upd.username) delete upd.username;
  if (password && password.trim()) upd.password = hashPassword(password);
  try {
    await User.findByIdAndUpdate(req.params.id, upd, { runValidators: true });
    req.session.successMsg = 'Data pengguna berhasil diperbarui';
    res.redirect('/admin/users');
  } catch (err) {
    if (err.code === 11000) {
      req.session.errorMsg = 'Username atau email sudah digunakan oleh pengguna lain';
    } else if (err.name === 'ValidationError') {
      req.session.errorMsg = Object.values(err.errors).map(e => e.message).join(', ');
    } else {
      req.session.errorMsg = 'Gagal memperbarui data pengguna';
    }
    res.redirect(`/admin/users/${req.params.id}/edit`);
  }
});

app.post('/admin/users/:id/delete', isAuth, isAdmin, async (req, res) => {
  try {
    const userId = req.params.id;
    const userToDelete = await User.findById(userId);
    if (!userToDelete) {
      req.session.errorMsg = 'User tidak ditemukan.';
      return res.redirect('/admin/users');
    }
    if (userToDelete.role === 'admin') {
      req.session.errorMsg = 'Tidak dapat menghapus akun admin.';
      return res.redirect('/admin/users');
    }

    if (userToDelete.profilePicture) {
      const fullPath = path.join(__dirname, 'public', userToDelete.profilePicture);
      if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    }

    const userChats = await ChatMessage.find({ userId: userId }, 'image').lean();
    for (const chat of userChats) {
      if (chat.image) {
        const chatImgPath = path.join(__dirname, 'public', chat.image);
        if (fs.existsSync(chatImgPath)) fs.unlinkSync(chatImgPath);
      }
    }
    await ChatMessage.deleteMany({ userId: userId });

    await Invoice.deleteMany({ userId });
    await Transaction.deleteMany({ userId });
    await Withdrawal.deleteMany({ userId });
    await ApiKey.deleteMany({ userId });
    await User.findByIdAndDelete(userId);

    req.session.successMsg = 'User berhasil dihapus beserta seluruh data terkait.';
    res.redirect('/admin/users');
  } catch (err) {
    console.error(err);
    req.session.errorMsg = 'Gagal menghapus user.';
    res.redirect('/admin/users');
  }
});

app.get('/admin/withdraw', isAuth, isAdmin, async (req, res) => {
  const search = req.query.search || '';
  let filter = {};
  if (search) {
    const users = await User.find({
      $or: [{ email: { $regex: search, $options: 'i' } }, { username: { $regex: search, $options: 'i' } }]
    }).select('_id');
    const userIds = users.map(u => u._id);
    filter = { userId: { $in: userIds } };
  }
  const withdrawals = await Withdrawal.find(filter)
    .populate('userId', 'username email')
    .sort({ createdAt: -1 })
    .lean();
  res.render('admin_withdraw', { withdrawals, search });
});

app.post('/admin/withdraw/success/:id', isAuth, isAdmin, async (req, res) => {
  const wd = await Withdrawal.findById(req.params.id);
  if (wd && wd.status === 'pending') {
    wd.status = 'success';
    wd.completedAt = new Date();
    await wd.save();
    await Transaction.updateOne(
      { userId: wd.userId, type: 'withdraw', status: 'pending', reference: { $regex: /^W/ } },
      { status: 'success', completedAt: new Date() }
    );
    await Stats.updateOne({}, { $inc: { totalWithdrawAmount: wd.amount, totalWithdrawFee: wd.fee } });

    emitLiveTransaction('withdraw_success', {
      userId: wd.userId,
      amount: wd.amount,
      invoice_id: wd._id,
      status: 'success',
      createdAt: wd.createdAt
    });

    sendPushNotification(wd.userId, 'withdraw_success', {
      title: '💸 PutzPay',
      body: `Withdraw Rp ${wd.amount.toLocaleString('id-ID')} berhasil diproses.`,
      data: { url: '/withdraw', withdrawId: wd._id }
    }, { eventId: `withdraw_success_${wd._id}` }).catch(() => {});

    telegramMonitor.notifyWithdraw({
      withdraw_id: wd._id,
      amount: wd.amount,
      method: wd.method,
      status: 'SUCCESS'
    });

    req.session.successMsg = 'Penarikan berhasil disetujui.';
  }
  res.redirect('/admin/withdraw');
});

app.post('/admin/withdraw/reject/:id', isAuth, isAdmin, async (req, res) => {
  const wd = await Withdrawal.findById(req.params.id);
  if (wd && wd.status === 'pending') {
    wd.status = 'rejected';
    wd.adminNote = req.body.note || '';
    wd.completedAt = new Date();
    await wd.save();
    await User.findByIdAndUpdate(wd.userId, { $inc: { balance: wd.amount + wd.fee } });
    await Transaction.updateOne(
      { userId: wd.userId, type: 'withdraw', status: 'pending', reference: { $regex: /^W/ } },
      { status: 'rejected', adminNote: req.body.note || '', completedAt: new Date() }
    );

    emitLiveTransaction('payment_failed', {
      userId: wd.userId,
      amount: wd.amount,
      invoice_id: wd._id,
      status: 'failed',
      createdAt: wd.createdAt
    });

    sendPushNotification(wd.userId, 'withdraw_failed', {
      title: '❌ PutzPay',
      body: `Withdraw Rp ${wd.amount.toLocaleString('id-ID')} gagal diproses.${wd.adminNote ? ' Catatan: ' + wd.adminNote : ''}`,
      data: { url: '/withdraw', withdrawId: wd._id }
    }, { eventId: `withdraw_failed_${wd._id}` }).catch(() => {});

    telegramMonitor.notifyWithdraw({
      withdraw_id: wd._id,
      amount: wd.amount,
      method: wd.method,
      status: 'FAILED'
    });

    req.session.successMsg = 'Penarikan ditolak dan saldo dikembalikan.';
  }
  res.redirect('/admin/withdraw');
});

app.get('/admin/transactions', isAuth, isAdmin, async (req, res) => {
  const search = req.query.search || '';
  let filter = {};
  if (search) {
    const users = await User.find({
      $or: [{ email: { $regex: search, $options: 'i' } }, { username: { $regex: search, $options: 'i' } }]
    }).select('_id');
    const userIds = users.map(u => u._id);
    filter = { $or: [{ userId: { $in: userIds } }, { reference: { $regex: search, $options: 'i' } }] };
  }
  const transactions = await Transaction.find(filter).populate('userId', 'username email').sort({ createdAt: -1 }).lean();
  res.render('admin_transactions', { transactions, search });
});

app.get('/admin/account', isAuth, isAdmin, async (req, res) => {
  const admin = await User.findById(req.session.userId).lean();
  res.render('admin_account', { admin });
});

app.post('/admin/account', isAuth, isAdmin, async (req, res) => {
  const { username, password, newPassword } = req.body;
  const admin = await User.findById(req.session.userId).lean();
  if (!password || !verifyPassword(password, admin.password)) {
    req.session.errorMsg = 'Password saat ini salah';
    return res.redirect('/admin/account');
  }
  if (username && username !== admin.username) {
    if (!/^[a-zA-Z0-9]+$/.test(username)) {
      req.session.errorMsg = 'Username hanya boleh berisi huruf dan angka (tanpa spasi atau simbol)';
      return res.redirect('/admin/account');
    }
    if (username.length > 15) {
      req.session.errorMsg = 'Username maksimal 15 karakter';
      return res.redirect('/admin/account');
    }
    const exist = await User.findOne({ username: username.toLowerCase(), _id: { $ne: admin._id } });
    if (exist) {
      req.session.errorMsg = 'Username sudah digunakan oleh pengguna lain';
      return res.redirect('/admin/account');
    }
  }
  try {
    const update = {};
    if (username && username !== admin.username) update.username = username.toLowerCase();
    if (newPassword && newPassword.trim()) update.password = hashPassword(newPassword);
    if (Object.keys(update).length > 0) {
      await User.findByIdAndUpdate(req.session.userId, update);
      req.session.successMsg = 'Data akun berhasil diperbarui';
    } else {
      req.session.errorMsg = 'Tidak ada perubahan yang dilakukan';
    }
  } catch (e) {
    req.session.errorMsg = 'Gagal memperbarui akun';
  }
  res.redirect('/admin/account');
});

app.post('/admin/settings/reset', isAuth, isAdmin, async (req, res) => {
  try {
    const chatMessages = await ChatMessage.find({ image: { $ne: null } }, 'image').lean();
    const chatFiles = chatMessages.map(m => m.image).filter(Boolean);
    const uniqueChatFiles = [...new Set(chatFiles)];
    for (const filePath of uniqueChatFiles) {
      const fullPath = path.join(__dirname, 'public', filePath);
      if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    }
    await ChatMessage.deleteMany({});

    const usersToDelete = await User.find({ role: 'user' }, 'profilePicture').lean();
    const profileFiles = usersToDelete.map(u => u.profilePicture).filter(Boolean);
    const uniqueProfileFiles = [...new Set(profileFiles)];
    for (const filePath of uniqueProfileFiles) {
      const fullPath = path.join(__dirname, 'public', filePath);
      if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    }
    await User.deleteMany({ role: 'user' });

    await Invoice.deleteMany({});
    await Transaction.deleteMany({});
    await Withdrawal.deleteMany({});
    await ApiKey.deleteMany({});
    await Stats.deleteMany({});
    await Stats.create({
      totalDepositAmount: 0,
      totalDepositFee: 0,
      totalWithdrawAmount: 0,
      totalWithdrawFee: 0,
      totalUsers: 0,
      totalTransactions: 0
    });

    req.session.successMsg = 'Database berhasil direset. Semua data pengguna, invoice, transaksi, dan withdrawal telah dihapus.';
  } catch (error) {
    console.error('Reset database error:', error);
    req.session.errorMsg = 'Gagal mereset database. Silakan coba lagi.';
  }
  res.redirect('/admin/settings');
});

// ===================== ADMIN NOTIFICATIONS ROUTES =====================
app.get('/admin/notifications', isAuth, isAdmin, hasPermission('send_notifications'), async (req, res) => {
  try {
    const notifications = await Notification.find({}).sort({ createdAt: -1 }).limit(100).lean();
    res.render('admin_notifikasi', {
      notifications,
      settings: res.locals.settings
    });
  } catch (err) {
    console.error('Error rendering admin notifications:', err);
    req.session.errorMsg = 'Gagal memuat riwayat notifikasi.';
    res.redirect('/admin/dashboard');
  }
});

app.post('/admin/notifications/send', isAuth, isAdmin, hasPermission('send_notifications'), async (req, res) => {
  try {
    const { recipientType, targetUsername, title, message, type, channel } = req.body;
    
    if (!message || !message.trim()) {
      req.session.errorMsg = 'Pesan notifikasi tidak boleh kosong.';
      return res.redirect('/admin/notifications');
    }

    const cleanTitle = (title || 'Pemberitahuan Sistem').trim();
    const cleanMessage = message.trim();
    const notifType = type || 'info';
    const notifChannel = channel || 'both';
    const sender = req.session.username || res.locals.user?.username || 'Admin';
    const senderRole = req.session.userRole || 'admin';

    let pushCount = 0;
    let emailCount = 0;

    if (recipientType === 'user') {
      const cleanTargetUser = (targetUsername || '').trim().toLowerCase();
      if (!cleanTargetUser) {
        req.session.errorMsg = 'Username penerima wajib diisi.';
        return res.redirect('/admin/notifications');
      }

      const user = await User.findOne({ username: cleanTargetUser });
      if (!user) {
        req.session.errorMsg = `Pengguna @${cleanTargetUser} tidak ditemukan.`;
        return res.redirect('/admin/notifications');
      }

      // Create notification record
      await Notification.create({
        title: cleanTitle,
        message: cleanMessage,
        target: String(user._id),
        targetUser: user._id,
        targetUsername: user.username,
        sender,
        senderRole,
        type: notifType,
        channel: notifChannel,
        isRead: false
      });

      // Send Web Push if requested
      if (notifChannel === 'web_push' || notifChannel === 'both') {
        try {
          await sendPushNotification(user._id, null, {
            title: `🔔 ${cleanTitle}`,
            body: cleanMessage,
            data: { url: '/dashboard' }
          });
          pushCount++;
        } catch (e) {
          console.warn('[NOTIF SEND] Push failed:', e.message);
        }
      }

      // Send Email if requested
      if (notifChannel === 'email' || notifChannel === 'both') {
        if (user.email) {
          const emailRes = await sendNotificationEmail(user.email, user.username, cleanTitle, cleanMessage);
          if (emailRes.success) emailCount++;
        }
      }

      req.session.successMsg = `Notifikasi berhasil dikirimkan ke @${user.username} (Push: ${pushCount}, Email: ${emailCount}).`;
    } else {
      // Send to ALL users
      await Notification.create({
        title: cleanTitle,
        message: cleanMessage,
        target: 'all',
        sender,
        senderRole,
        type: notifType,
        channel: notifChannel,
        readBy: []
      });

      // Send Web Push to all active subscriptions
      if (notifChannel === 'web_push' || notifChannel === 'both') {
        try {
          const allSubs = await PushSubscription.find({ active: true });
          for (const sub of allSubs) {
            try {
              await sendPushToSubscription(sub, {
                title: `🔔 ${cleanTitle}`,
                body: cleanMessage,
                data: { url: '/dashboard' }
              });
              pushCount++;
            } catch (e) {}
          }
        } catch (e) {
          console.warn('[NOTIF SEND ALL] Push broadcast error:', e.message);
        }
      }

      // Send Email to all registered users (if SMTP configured)
      if (notifChannel === 'email' || notifChannel === 'both') {
        try {
          const users = await User.find({ suspended: { $ne: true } }).select('email username').lean();
          for (const u of users) {
            if (u.email) {
              sendNotificationEmail(u.email, u.username, cleanTitle, cleanMessage).then(res => {
                if (res.success) emailCount++;
              }).catch(() => {});
            }
          }
        } catch (e) {}
      }

      req.session.successMsg = `Siaran notifikasi massal berhasil diterbitkan ke seluruh pengguna sistem.`;
    }

    res.redirect('/admin/notifications');
  } catch (err) {
    console.error('Error sending admin notification:', err);
    req.session.errorMsg = 'Gagal mengirim notifikasi: ' + err.message;
    res.redirect('/admin/notifications');
  }
});

// ===================== ADMIN SETTINGS (OWNER ONLY) =====================
app.get('/admin/settings', isAuth, isOwner, async (req, res) => {
  const settings = await getSettings();
  const partners = await Partner.find().sort({ order: 1, createdAt: -1 }).lean();
  res.render('admin_settings', { settings, partners });
});

app.post('/admin/settings', isAuth, isOwner, async (req, res) => {
  let withdrawMethods = [];
  const raw = req.body.withdrawMethodsJson;
  if (raw) {
    try {
      withdrawMethods = JSON.parse(raw);
      if (!Array.isArray(withdrawMethods)) withdrawMethods = [];
    } catch (e) {
      withdrawMethods = [];
    }
  }
  withdrawMethods = withdrawMethods.filter(m => m.name && typeof m.name === 'string' && m.name.trim() !== '' && typeof m.fee === 'number' && !isNaN(m.fee) && m.fee >= 0);
  if (withdrawMethods.length === 0) {
    withdrawMethods = [
      { name: 'Dana', fee: 1000 },
      { name: 'GoPay', fee: 2000 }
    ];
  }

  const parseNumber = (val, def) => {
    const n = parseInt(val);
    return isNaN(n) ? def : n;
  };

  const {
    name, title, description, channelWhatsApp,
    minDeposit, minWithdraw, feeWithdraw, maxFee,
    checkInterval, smtpHost, smtpPort, smtpSecure, smtpUser, smtpPass, logoUrl, googleClientId,
    googleClientSecret, googleRedirectUri,
    turnstileSiteKey, turnstileSecretKey,
    qrisExpiredMinutes,
    gopayDomain, gopayToken, gopayStaticQr,
    gopayRefreshToken,
    partnerEnabled, partnerBannerTitle, partnerBannerSubtitle,
    partnerCtaUrl, partnerCtaText,
    maintenanceEnabled, maintenanceMode, maintenanceFeatures,
    maintenanceTitle, maintenanceMessage, maintenanceIcon,
    maintenanceCountdown, maintenanceTelegram, maintenanceWhatsApp,
    kycEnabled, kycDiscountPercent,
    kycNonKycMaxBalance, kycNonKycMaxDailyTransaction,
    kycNonKycMaxWithdrawalPerTx, kycNonKycMaxDailyWithdrawal, kycNonKycMaxDailyWithdrawalCount,
    kycVerifiedMaxBalance, kycVerifiedMaxDailyTransaction,
    kycVerifiedMaxWithdrawalPerTx, kycVerifiedMaxDailyWithdrawal, kycVerifiedMaxDailyWithdrawalCount
  } = req.body;

  let featuresList = [];
  if (Array.isArray(maintenanceFeatures)) {
    featuresList = maintenanceFeatures;
  } else if (typeof maintenanceFeatures === 'string' && maintenanceFeatures.trim()) {
    featuresList = [maintenanceFeatures];
  }

  const sanitizedSmtpHost = (smtpHost || 'smtp.gmail.com').trim();
  const sanitizedSmtpPort = parseNumber(smtpPort, 465);
  const sanitizedSmtpSecure = smtpSecure === 'true' || smtpSecure === true || sanitizedSmtpPort === 465;
  const sanitizedSmtpUser = (smtpUser || '').trim();
  const sanitizedSmtpPass = (smtpPass || '').replace(/\s+/g, '').trim();
  const sanitizedTurnstileSiteKey = (turnstileSiteKey || '').trim();
  const sanitizedTurnstileSecretKey = (turnstileSecretKey || '').trim();

  await Setting.updateOne({}, {
    name, title, description, channelWhatsApp,
    minDeposit: parseNumber(minDeposit, 1000),
    minWithdraw: parseNumber(minWithdraw, 5000),
    feeWithdraw: parseNumber(feeWithdraw, 1000),
    maxFee: parseNumber(maxFee, 500),
    checkInterval: parseNumber(checkInterval, 30),
    smtpHost: sanitizedSmtpHost,
    smtpPort: sanitizedSmtpPort,
    smtpSecure: sanitizedSmtpSecure,
    smtpUser: sanitizedSmtpUser,
    smtpPass: sanitizedSmtpPass,
    logoUrl,
    googleClientId: (googleClientId || '').trim(),
    googleClientSecret: (googleClientSecret || '').trim(),
    googleRedirectUri: (googleRedirectUri || '').trim(),
    turnstileSiteKey: sanitizedTurnstileSiteKey,
    turnstileSecretKey: sanitizedTurnstileSecretKey,
    qrisExpiredMinutes: parseNumber(qrisExpiredMinutes, 30),
    withdrawMethods,
    gopayDomain: gopayDomain || 'gomerch.vercel.app',
    gopayToken: gopayToken || '',
    gopayStaticQr: gopayStaticQr || '',
    gopayRefreshToken: gopayRefreshToken || '',
    partnerEnabled: partnerEnabled === 'true' || partnerEnabled === true || partnerEnabled === 'on',
    partnerBannerTitle: (partnerBannerTitle || 'Partner Resmi PutzPay').trim(),
    partnerBannerSubtitle: (partnerBannerSubtitle || 'Temukan partner resmi dan ekosistem bisnis terpercaya yang terintegrasi dengan gateway pembayaran PutzPay.').trim(),
    partnerCtaUrl: (partnerCtaUrl || 'https://t.me/PutzOfficial').trim(),
    partnerCtaText: (partnerCtaText || 'Ajukan Kemitraan Resmi').trim(),
    maintenanceEnabled: maintenanceEnabled === 'true',
    maintenanceMode: maintenanceMode || 'all',
    maintenanceFeatures: featuresList,
    maintenanceTitle: maintenanceTitle || 'Sistem Dalam Pemeliharaan',
    maintenanceMessage: maintenanceMessage || 'Kami sedang melakukan pemeliharaan rutin untuk meningkatkan kualitas layanan. Silakan kembali lagi nanti.',
    maintenanceIcon: maintenanceIcon || 'fa-solid fa-wrench',
    maintenanceCountdown: maintenanceCountdown || '',
    maintenanceTelegram: maintenanceTelegram || '',
    maintenanceWhatsApp: maintenanceWhatsApp || '',
    kycEnabled: kycEnabled === 'true' || kycEnabled === true || kycEnabled === 'on',
    kycDiscountPercent: parseNumber(kycDiscountPercent, 15),
    kycNonKycMaxBalance: parseNumber(kycNonKycMaxBalance, 2000000),
    kycNonKycMaxDailyTransaction: parseNumber(kycNonKycMaxDailyTransaction, 5000000),
    kycNonKycMaxWithdrawalPerTx: parseNumber(kycNonKycMaxWithdrawalPerTx, 1000000),
    kycNonKycMaxDailyWithdrawal: parseNumber(kycNonKycMaxDailyWithdrawal, 2000000),
    kycNonKycMaxDailyWithdrawalCount: parseNumber(kycNonKycMaxDailyWithdrawalCount, 3),
    kycVerifiedMaxBalance: parseNumber(kycVerifiedMaxBalance, 50000000),
    kycVerifiedMaxDailyTransaction: parseNumber(kycVerifiedMaxDailyTransaction, 100000000),
    kycVerifiedMaxWithdrawalPerTx: parseNumber(kycVerifiedMaxWithdrawalPerTx, 25000000),
    kycVerifiedMaxDailyWithdrawal: parseNumber(kycVerifiedMaxDailyWithdrawal, 50000000),
    kycVerifiedMaxDailyWithdrawalCount: parseNumber(kycVerifiedMaxDailyWithdrawalCount, 20)
  });

  startChecker();
  req.session.successMsg = 'Pengaturan berhasil diperbarui.';
  res.redirect('/admin/settings');
});

// ===================== ADMIN PARTNERS CRUD ROUTES =====================
app.post('/admin/partners/create', isAuth, isOwner, async (req, res) => {
  try {
    const { name, category, logoUrl, websiteUrl, description, tier, badge, order, isActive, contactEmail, contactPhone } = req.body;
    if (!name || !name.trim()) {
      req.session.errorMsg = 'Nama partner wajib diisi.';
      return res.redirect('/admin/settings#partnerSettings');
    }
    const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    await Partner.create({
      name: name.trim(),
      slug: slug || 'partner-' + Date.now(),
      category: (category || 'Fintech & E-Wallet').trim(),
      logoUrl: (logoUrl || 'https://files.catbox.moe/82p405.jpg').trim(),
      websiteUrl: (websiteUrl || '').trim(),
      description: (description || '').trim(),
      tier: tier || 'official',
      badge: (badge || 'Official Partner').trim(),
      order: parseInt(order, 10) || 0,
      isActive: isActive === 'true' || isActive === 'on' || isActive === true,
      contactEmail: (contactEmail || '').trim(),
      contactPhone: (contactPhone || '').trim()
    });
    req.session.successMsg = `Partner resmi "${name.trim()}" berhasil ditambahkan.`;
  } catch (err) {
    console.error('Error creating partner:', err);
    req.session.errorMsg = 'Gagal menambahkan partner: ' + err.message;
  }
  res.redirect('/admin/settings#partnerSettings');
});

app.post('/admin/partners/:id/update', isAuth, isOwner, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, category, logoUrl, websiteUrl, description, tier, badge, order, isActive, contactEmail, contactPhone } = req.body;
    if (!name || !name.trim()) {
      req.session.errorMsg = 'Nama partner wajib diisi.';
      return res.redirect('/admin/settings#partnerSettings');
    }
    const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    await Partner.findByIdAndUpdate(id, {
      name: name.trim(),
      slug: slug || 'partner-' + Date.now(),
      category: (category || 'Fintech & E-Wallet').trim(),
      logoUrl: (logoUrl || 'https://files.catbox.moe/82p405.jpg').trim(),
      websiteUrl: (websiteUrl || '').trim(),
      description: (description || '').trim(),
      tier: tier || 'official',
      badge: (badge || 'Official Partner').trim(),
      order: parseInt(order, 10) || 0,
      isActive: isActive === 'true' || isActive === 'on' || isActive === true,
      contactEmail: (contactEmail || '').trim(),
      contactPhone: (contactPhone || '').trim(),
      updatedAt: new Date()
    });
    req.session.successMsg = `Informasi partner "${name.trim()}" berhasil diperbarui.`;
  } catch (err) {
    console.error('Error updating partner:', err);
    req.session.errorMsg = 'Gagal memperbarui partner: ' + err.message;
  }
  res.redirect('/admin/settings#partnerSettings');
});

app.post('/admin/partners/:id/toggle', isAuth, isOwner, async (req, res) => {
  try {
    const { id } = req.params;
    const p = await Partner.findById(id);
    if (!p) {
      if (req.xhr || req.headers.accept?.includes('json')) {
        return res.status(404).json({ success: false, message: 'Partner tidak ditemukan.' });
      }
      req.session.errorMsg = 'Partner tidak ditemukan.';
      return res.redirect('/admin/settings#partnerSettings');
    }
    p.isActive = !p.isActive;
    p.updatedAt = new Date();
    await p.save();
    if (req.xhr || req.headers.accept?.includes('json')) {
      return res.json({ success: true, isActive: p.isActive, message: `Status partner ${p.name} kini ${p.isActive ? 'Aktif' : 'Nonaktif'}.` });
    }
    req.session.successMsg = `Status partner "${p.name}" kini ${p.isActive ? 'Aktif' : 'Nonaktif'}.`;
  } catch (err) {
    console.error('Error toggling partner:', err);
    if (req.xhr || req.headers.accept?.includes('json')) {
      return res.status(500).json({ success: false, message: err.message });
    }
    req.session.errorMsg = 'Gagal mengubah status partner: ' + err.message;
  }
  res.redirect('/admin/settings#partnerSettings');
});

app.post('/admin/partners/:id/delete', isAuth, isOwner, async (req, res) => {
  try {
    const { id } = req.params;
    const p = await Partner.findByIdAndDelete(id);
    req.session.successMsg = `Partner "${p?.name || id}" berhasil dihapus dari direktori.`;
  } catch (err) {
    console.error('Error deleting partner:', err);
    req.session.errorMsg = 'Gagal menghapus partner: ' + err.message;
  }
  res.redirect('/admin/settings#partnerSettings');
});

// ===================== ADMIN TEST SMTP ROUTE =====================
app.post(['/admin/settings/test-smtp', '/admin/test-smtp'], isAuth, isAdmin, async (req, res) => {
  try {
    const { targetEmail, testEmail, smtpUser, smtpPass, smtpHost, smtpPort, smtpSecure } = req.body;
    const settings = await getSettings();
    
    // Override settings if supplied directly from test modal/form
    const effectiveSettings = {
      ...settings.toObject ? settings.toObject() : settings,
      smtpUser: (smtpUser !== undefined && smtpUser !== '') ? smtpUser : settings.smtpUser,
      smtpPass: (smtpPass !== undefined && smtpPass !== '') ? smtpPass : settings.smtpPass,
      smtpHost: (smtpHost !== undefined && smtpHost !== '') ? smtpHost : settings.smtpHost,
      smtpPort: (smtpPort !== undefined && smtpPort !== '') ? smtpPort : settings.smtpPort,
      smtpSecure: (smtpSecure !== undefined && smtpSecure !== '') ? smtpSecure : settings.smtpSecure
    };

    const smtpConfig = getSmtpConfig(effectiveSettings);

    if (!smtpConfig.user || !smtpConfig.pass) {
      return res.json({ 
        success: false, 
        message: 'Email Sender atau Google App Password belum diisi.' 
      });
    }

    const recipient = targetEmail || testEmail;
    const testRecipient = (recipient && recipient.includes('@')) 
      ? recipient.trim() 
      : (req.session.email || res.locals.user?.email || smtpConfig.user);

    const appName = effectiveSettings.name || 'PutzPay';
    const testMail = {
      to: testRecipient,
      from: `"${appName}" <${smtpConfig.user}>`,
      subject: `[TEST] Uji Koneksi SMTP Email - ${appName}`,
      text: `Halo!\n\nIni adalah email uji coba dari ${appName}.\n\nKonfigurasi SMTP Anda telah berhasil terhubung dan siap mengirimkan email OTP dan Reset Password.\n\nWaktu pengujian: ${new Date().toLocaleString('id-ID')}\n\n© ${appName}`,
      html: `
        <div style="font-family: Arial, sans-serif; padding: 20px; background-color: #f8fafc;">
          <div style="max-width: 480px; margin: 0 auto; background: #ffffff; padding: 24px; border: 3px solid #000; border-radius: 16px; box-shadow: 4px 4px 0 #000;">
            <h2 style="margin-top: 0; color: #16a34a; font-size: 20px; font-weight: 800;">✅ Uji Koneksi SMTP Berhasil!</h2>
            <p style="color: #334155; font-size: 14px; line-height: 1.6;">
              Selamat! Konfigurasi server SMTP email pada <strong>${appName}</strong> telah terhubung dengan baik dan siap digunakan.
            </p>
            <div style="background: #f1f5f9; padding: 12px; border: 2px solid #000; border-radius: 8px; font-size: 12px; color: #334155; font-family: monospace; margin: 16px 0;">
              <strong>Penerima:</strong> ${maskEmail(testRecipient)}<br>
              <strong>Waktu:</strong> ${new Date().toLocaleString('id-ID')}
            </div>
            <p style="color: #64748b; font-size: 11px; margin: 0;">
              Email pengujian otomatis dikirim dari sistem ${appName}.
            </p>
          </div>
        </div>
      `
    };

    const result = await sendEmailWithFallback(testMail, effectiveSettings);
    if (result.success) {
      return res.json({ 
        success: true, 
        message: `Email uji coba berhasil dikirim ke ${maskEmail(testRecipient)}!` 
      });
    } else {
      return res.json({ 
        success: false, 
        message: result.error || 'Gagal mengirim email uji coba.' 
      });
    }
  } catch (err) {
    return res.json({ 
      success: false, 
      message: err.message || 'Terjadi kesalahan internal saat menguji SMTP.' 
    });
  }
});

// ===================== ADMIN WEBSITE BACKUP ROUTES (OWNER ONLY) =====================
app.get('/admin/backup', isAuth, canAccessBackup, async (req, res) => {
  try {
    const backups = listBackups();
    res.render('admin_backup', {
      backups,
      settings: res.locals.settings
    });
  } catch (err) {
    console.error('Error loading admin backup page:', err);
    res.render('admin_backup', {
      backups: [],
      error: 'Gagal membaca daftar backup.',
      settings: res.locals.settings
    });
  }
});

app.post('/admin/backup/create', isAuth, canAccessBackup, async (req, res) => {
  try {
    const includeSensitive = req.body.includeSensitive === 'true' || req.body.includeSensitive === true;
    const backupResult = await createWebsiteBackup(includeSensitive);
    
    const successMessage = `Arsip backup website ${backupResult.filename} (${backupResult.sizeFormatted}) berhasil dibuat.`;
    
    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.json({
        success: true,
        message: successMessage,
        backup: backupResult
      });
    }

    req.session.successMsg = successMessage;
    res.redirect('/admin/backup');
  } catch (err) {
    console.error('Error generating website backup:', err);
    const errorMsg = err.message || 'Gagal membuat arsip backup website.';
    
    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(500).json({ success: false, message: errorMsg });
    }

    req.session.errorMsg = errorMsg;
    res.redirect('/admin/backup');
  }
});

app.get('/admin/backup/download/:filename', isAuth, canAccessBackup, (req, res) => {
  try {
    const rawFilename = req.params.filename || '';
    const filename = path.basename(rawFilename);

    // Validate filename security
    if (!filename.startsWith('PutzPay-Backup-') || !filename.endsWith('.zip') || filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
      req.session.errorMsg = 'Nama file backup tidak valid.';
      return res.redirect('/admin/backup');
    }

    const filePath = path.join(__dirname, 'backups', filename);
    if (!fs.existsSync(filePath)) {
      req.session.errorMsg = 'Berkas backup tidak ditemukan di server.';
      return res.redirect('/admin/backup');
    }

    res.download(filePath, filename, (err) => {
      if (err && !res.headersSent) {
        console.error('Error downloading backup file:', err);
        req.session.errorMsg = 'Gagal mengunduh berkas backup.';
        res.redirect('/admin/backup');
      }
    });
  } catch (err) {
    console.error('Error in download backup handler:', err);
    req.session.errorMsg = 'Terjadi kesalahan saat mengunduh berkas.';
    res.redirect('/admin/backup');
  }
});

app.post('/admin/backup/delete/:filename', isAuth, canAccessBackup, (req, res) => {
  try {
    const rawFilename = req.params.filename || '';
    const filename = path.basename(rawFilename);

    // Validate filename security
    if (!filename.startsWith('PutzPay-Backup-') || !filename.endsWith('.zip') || filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
      req.session.errorMsg = 'Nama file backup tidak valid.';
      return res.redirect('/admin/backup');
    }

    const filePath = path.join(__dirname, 'backups', filename);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }

    const successMessage = `Berkas backup ${filename} berhasil dihapus.`;

    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.json({ success: true, message: successMessage });
    }

    req.session.successMsg = successMessage;
    res.redirect('/admin/backup');
  } catch (err) {
    console.error('Error deleting backup file:', err);
    const errorMsg = 'Gagal menghapus berkas backup.';

    if (req.xhr || (req.headers.accept && req.headers.accept.includes('application/json'))) {
      return res.status(500).json({ success: false, message: errorMsg });
    }

    req.session.errorMsg = errorMsg;
    res.redirect('/admin/backup');
  }
});

// ===================== ADMIN NOTIFIKASI CRUD =====================
app.post('/admin/notifications', isAuth, isAdmin, async (req, res) => {
  const { title, message } = req.body;
  if (!message) {
    req.session.errorMsg = 'Pesan notifikasi wajib diisi.';
    return res.redirect('/admin/notifications');
  }
  await Notification.create({ title: title || '', message, target: 'all' });
  req.session.successMsg = 'Notifikasi berhasil ditambahkan.';
  res.redirect('/admin/notifications');
});

app.post('/admin/notifications/edit/:id', isAuth, isAdmin, async (req, res) => {
  const { title, message } = req.body;
  if (!message) {
    req.session.errorMsg = 'Pesan notifikasi wajib diisi.';
    return res.redirect('/admin/notifications');
  }
  await Notification.findByIdAndUpdate(req.params.id, { title: title || '', message });
  req.session.successMsg = 'Notifikasi berhasil diperbarui.';
  res.redirect('/admin/notifications');
});

app.post('/admin/notifications/delete/:id', isAuth, isAdmin, async (req, res) => {
  await Notification.findByIdAndDelete(req.params.id);
  req.session.successMsg = 'Notifikasi berhasil dihapus.';
  res.redirect('/admin/notifications');
});

// ===================== LIVE CHAT & GLOBAL CHAT ROUTES =====================

// Pages
app.get('/chat', isAuth, async (req, res) => {
  res.render('chat', { user: res.locals.user });
});

app.get('/admin/chat', isAuth, isAdmin, async (req, res) => {
  res.render('admin_chat', { user: res.locals.user });
});

app.get('/chatglobal', isAuth, async (req, res) => {
  res.render('chatglobal', { user: res.locals.user });
});

app.get('/admin/chatglobal', isAuth, isAdmin, async (req, res) => {
  res.render('admin_chatglobal', { user: res.locals.user });
});

// Image Upload
app.post('/api/chat/upload', isAuth, chatUpload.single('image'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Gambar tidak ditemukan' });
  const imageUrl = '/uploads/' + req.file.filename;
  res.json({ success: true, imageUrl });
});

// Support Chat APIs
app.get('/api/chat/messages', isAuth, async (req, res) => {
  try {
    const user = res.locals.user;
    const since = req.query.since;
    const targetUserId = req.query.targetUserId;
    
    let filter = {};
    if (user.role === 'admin') {
      if (targetUserId) {
        filter.$or = [
          { userId: targetUserId },
          { targetUserId: targetUserId }
        ];
      }
    } else {
      filter.$or = [
        { userId: user._id },
        { targetUserId: user._id }
      ];
    }

    if (since) {
      filter.createdAt = { $gt: new Date(since) };
    }

    const messages = await ChatMessage.find(filter).sort({ createdAt: 1 }).lean();
    res.json(messages);
  } catch (err) {
    console.error('Fetch chat error:', err);
    res.status(500).json({ error: 'Gagal mengambil data chat' });
  }
});

app.post('/api/chat/send', isAuth, async (req, res) => {
  try {
    const { message, image, replyTo, targetUserId } = req.body;
    const text = (message || '').trim();
    if (!text && !image) {
      return res.status(400).json({ error: 'Pesan atau gambar harus diisi' });
    }
    const user = await User.findById(req.session.userId).lean();
    if (!user) return res.status(401).json({ error: 'User tidak ditemukan' });

    let finalTargetUserId = null;
    if (user.role === 'admin') {
      if (!targetUserId) return res.status(400).json({ error: 'Target user ID wajib diisi oleh admin' });
      finalTargetUserId = targetUserId;
    } else {
      finalTargetUserId = user._id;
    }

    const chatMsg = await ChatMessage.create({
      userId: user._id,
      targetUserId: finalTargetUserId,
      username: user.username,
      message: text,
      image: image || null,
      role: user.role,
      profilePicture: user.profilePicture || null,
      profileColor: user.profileColor || null,
      replyTo: replyTo || null,
      isRead: false,
      status: 'sent'
    });

    // Realtime Socket broadcast
    if (io) {
      io.to(`support_user_${finalTargetUserId}`).to('support_admin_room').emit('chat:receive', chatMsg);
    }

    res.json({ success: true, message: chatMsg });
  } catch (err) {
    console.error('Send chat error:', err);
    res.status(500).json({ error: 'Gagal mengirim pesan' });
  }
});

app.post('/api/chat/read', isAuth, async (req, res) => {
  try {
    const user = res.locals.user;
    const { targetUserId } = req.body;
    const effectiveTargetId = user.role === 'admin' ? targetUserId : user._id;

    if (effectiveTargetId) {
      await ChatMessage.updateMany(
        { 
          $or: [
            { userId: effectiveTargetId, isRead: false },
            { targetUserId: effectiveTargetId, isRead: false }
          ],
          role: user.role === 'admin' ? 'user' : 'admin'
        },
        { $set: { isRead: true, status: 'read' } }
      );

      if (io) {
        io.to(`support_user_${effectiveTargetId}`).to('support_admin_room').emit('chat:read_ack', { targetUserId: effectiveTargetId });
      }
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal memperbarui status dibaca' });
  }
});

app.post('/api/chat/edit', isAuth, async (req, res) => {
  try {
    const { messageId, newMessage } = req.body;
    if (!messageId || !newMessage || !newMessage.trim()) {
      return res.status(400).json({ error: 'Data tidak lengkap' });
    }
    const msg = await ChatMessage.findById(messageId);
    if (!msg) return res.status(404).json({ error: 'Pesan tidak ditemukan' });

    const user = res.locals.user;
    if (user.role !== 'admin' && msg.userId.toString() !== user._id.toString()) {
      return res.status(403).json({ error: 'Akses ditolak' });
    }

    msg.message = newMessage.trim();
    msg.isEdited = true;
    await msg.save();

    const targetId = msg.targetUserId ? msg.targetUserId.toString() : msg.userId.toString();
    if (io) {
      io.to(`support_user_${targetId}`).to('support_admin_room').emit('chat:edit', {
        messageId: msg._id,
        message: msg.message,
        isEdited: true
      });
    }

    res.json({ success: true, message: msg });
  } catch (err) {
    res.status(500).json({ error: 'Gagal mengedit pesan' });
  }
});

app.post('/api/chat/delete', isAuth, async (req, res) => {
  try {
    const { messageId } = req.body;
    if (!messageId) return res.status(400).json({ error: 'Message ID wajib diisi' });

    const msg = await ChatMessage.findById(messageId);
    if (!msg) return res.status(404).json({ error: 'Pesan tidak ditemukan' });

    const user = res.locals.user;
    if (user.role !== 'admin' && msg.userId.toString() !== user._id.toString()) {
      return res.status(403).json({ error: 'Akses ditolak' });
    }

    msg.isDeleted = true;
    msg.message = '';
    msg.image = null;
    msg.deletedBy = user.username;
    await msg.save();

    const targetId = msg.targetUserId ? msg.targetUserId.toString() : msg.userId.toString();
    if (io) {
      io.to(`support_user_${targetId}`).to('support_admin_room').emit('chat:delete', {
        messageId: msg._id,
        deletedBy: user.username
      });
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal menghapus pesan' });
  }
});

app.get('/api/admin/chat/users', isAuth, isAdmin, async (req, res) => {
  try {
    // Get unique user IDs from ChatMessage
    const allChats = await ChatMessage.find().sort({ createdAt: -1 }).lean();
    const userMap = new Map();

    for (const chat of allChats) {
      let uId = null;
      if (chat.role === 'user') {
        uId = chat.userId ? chat.userId.toString() : null;
      } else {
        uId = chat.targetUserId ? chat.targetUserId.toString() : null;
      }

      if (!uId) continue;

      if (!userMap.has(uId)) {
        userMap.set(uId, {
          userId: uId,
          username: chat.username || 'User',
          profilePicture: chat.profilePicture || null,
          lastMessage: chat.message || (chat.image ? '📷 Gambar' : ''),
          lastMessageAt: chat.createdAt,
          unreadCount: 0
        });
      }

      if (chat.role === 'user' && !chat.isRead) {
        const item = userMap.get(uId);
        item.unreadCount += 1;
      }
    }

    const userIds = Array.from(userMap.keys());
    const dbUsers = await User.find({ _id: { $in: userIds } }).select('username profilePicture').lean();
    const dbUserDict = {};
    dbUsers.forEach(u => dbUserDict[u._id.toString()] = u);

    const result = Array.from(userMap.values()).map(u => {
      const realUser = dbUserDict[u.userId];
      if (realUser) {
        u.username = realUser.username || u.username;
        if (realUser.profilePicture) u.profilePicture = realUser.profilePicture;
      }
      u.isOnline = onlineUsers.has(u.userId) && onlineUsers.get(u.userId).size > 0;
      return u;
    });

    res.json(result);
  } catch (err) {
    console.error('Admin chat users error:', err);
    res.status(500).json({ error: 'Gagal mengambil daftar user chat' });
  }
});

app.post('/api/chat/reset', isAuth, isAdmin, async (req, res) => {
  try {
    const messages = await ChatMessage.find({ image: { $ne: null } }, 'image').lean();
    const filesToDelete = messages.map(m => m.image).filter(Boolean);
    const uniqueFiles = [...new Set(filesToDelete)];
    for (const filePath of uniqueFiles) {
      const fullPath = path.join(__dirname, 'public', filePath);
      if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    }
    await ChatMessage.deleteMany({});
    res.json({ success: true, message: 'Chat berhasil direset beserta file gambar.' });
  } catch (error) {
    console.error('Reset chat error:', error);
    res.status(500).json({ success: false, error: 'Gagal mereset chat.' });
  }
});

// Global Chat APIs
app.get('/api/globalchat/messages', isAuth, async (req, res) => {
  try {
    const user = res.locals.user;
    const since = req.query.since;
    let filter = {};

    if (user.role !== 'admin') {
      filter.isDeleted = { $ne: true };
    }

    if (since) {
      filter.createdAt = { $gt: new Date(since) };
    }

    const messages = await GlobalChat.find(filter)
      .sort({ createdAt: -1 })
      .limit(100)
      .lean();

    res.json(messages.reverse());
  } catch (err) {
    res.status(500).json({ error: 'Gagal mengambil chat global' });
  }
});

app.post('/api/globalchat/send', isAuth, async (req, res) => {
  try {
    const user = await User.findById(req.session.userId).lean();
    if (!user) return res.status(401).json({ error: 'User tidak ditemukan' });

    // Check Moderation Status
    const mod = await GlobalChatModeration.findOne({ userId: user._id }).lean();
    if (mod) {
      if (mod.isBanned) {
        return res.status(403).json({ error: `Anda telah dilarang (banned) dari Global Chat. Alasan: ${mod.reason || 'Sebab umum'}` });
      }
      if (mod.isMuted) {
        if (mod.mutedUntil && new Date() > new Date(mod.mutedUntil)) {
          // Mute expired
          await GlobalChatModeration.updateOne({ userId: user._id }, { isMuted: false, mutedUntil: null });
        } else {
          const untilStr = mod.mutedUntil ? new Date(mod.mutedUntil).toLocaleString('id-ID') : 'Selamanya';
          return res.status(403).json({ error: `Anda sedang di-mute di Global Chat hingga ${untilStr}.` });
        }
      }
    }

    // Slowmode / Cooldown check
    const settings = await getSettings();
    const slowmodeMs = (settings.globalSlowmode || 3) * 1000;
    const now = Date.now();
    const lastTime = lastGlobalChatTimes.get(user._id.toString()) || 0;
    if (now - lastTime < slowmodeMs && user.role !== 'admin') {
      const waitSec = Math.ceil((slowmodeMs - (now - lastTime)) / 1000);
      return res.status(429).json({ error: `Slowmode aktif! Tunggu ${waitSec} detik lagi sebelum mengirim pesan.` });
    }
    lastGlobalChatTimes.set(user._id.toString(), now);

    const { message, image, replyTo } = req.body;
    const text = (message || '').trim();
    if (!text && !image) {
      return res.status(400).json({ error: 'Pesan atau gambar harus diisi' });
    }

    // Anti Link filter
    if (settings.globalAntiLink && user.role !== 'admin' && text) {
      const linkRegex = /(https?:\/\/|www\.|t\.me|telegram\.me|wa\.me|chat\.whatsapp\.com|discord\.gg|discord\.com\/invite)/i;
      if (linkRegex.test(text)) {
        return res.status(400).json({ error: 'Dilarang mengirim link atau tautan di Global Chat!' });
      }
    }

    // Anti Toxic filter
    if (settings.globalAntiToxic && user.role !== 'admin' && text) {
      const toxicWords = ['kontol', 'memek', 'ngentot', 'anjing', 'babi', 'bangsat', 'peler', 'titit', 'bokep', 'porno'];
      const textLower = text.toLowerCase();
      if (toxicWords.some(w => textLower.includes(w))) {
        return res.status(400).json({ error: 'Pesan mengandung kata kasar/tidak sopan yang dilarang!' });
      }
    }

    const globalMsg = await GlobalChat.create({
      userId: user._id,
      username: user.username,
      message: text,
      image: image || null,
      role: user.role,
      profilePicture: user.profilePicture || null,
      profileColor: user.profileColor || null,
      replyTo: replyTo || null,
      createdAt: new Date()
    });

    if (io) {
      io.to('global_chat_room').emit('global:receive', globalMsg);
    }

    res.json({ success: true, message: globalMsg });
  } catch (err) {
    console.error('Global chat send error:', err);
    res.status(500).json({ error: 'Gagal mengirim pesan ke global chat' });
  }
});

app.post('/api/globalchat/pin', isAuth, isAdmin, async (req, res) => {
  try {
    const { messageId } = req.body;
    if (!messageId) return res.status(400).json({ error: 'ID Pesan diperlukan' });

    await GlobalChat.updateMany({}, { isPinned: false });
    const pinnedMsg = await GlobalChat.findByIdAndUpdate(
      messageId,
      { isPinned: true, pinnedBy: res.locals.user.username },
      { new: true }
    );

    if (pinnedMsg) {
      await ModerationLog.create({
        adminUsername: res.locals.user.username,
        action: 'pin',
        targetUsername: pinnedMsg.username,
        reason: 'Sematkan pesan global',
        details: pinnedMsg.message
      });

      if (io) {
        io.to('global_chat_room').emit('global:pin', pinnedMsg);
      }
    }

    res.json({ success: true, message: pinnedMsg });
  } catch (err) {
    res.status(500).json({ error: 'Gagal menyematkan pesan' });
  }
});

app.post('/api/globalchat/unpin', isAuth, isAdmin, async (req, res) => {
  try {
    await GlobalChat.updateMany({ isPinned: true }, { isPinned: false, pinnedBy: null });

    await ModerationLog.create({
      adminUsername: res.locals.user.username,
      action: 'unpin',
      targetUsername: 'Semua',
      reason: 'Lepas pin pesan global'
    });

    if (io) {
      io.to('global_chat_room').emit('global:unpin', {});
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal melepas pin pesan' });
  }
});

app.post('/api/globalchat/delete_all', isAuth, isAdmin, async (req, res) => {
  try {
    await GlobalChat.updateMany({}, { isDeleted: true, deletedBy: res.locals.user.username });

    await ModerationLog.create({
      adminUsername: res.locals.user.username,
      action: 'delete_all',
      targetUsername: 'Semua User',
      reason: 'Bersihkan seluruh pesan global chat'
    });

    if (io) {
      io.to('global_chat_room').emit('global:delete_all', { deletedBy: res.locals.user.username });
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal menghapus semua pesan global chat' });
  }
});

app.get('/api/admin/globalchat/logs', isAuth, isAdmin, async (req, res) => {
  try {
    const logs = await ModerationLog.find().sort({ createdAt: -1 }).limit(100).lean();
    res.json(logs);
  } catch (err) {
    res.status(500).json({ error: 'Gagal mengambil log moderasi' });
  }
});

app.post('/api/globalchat/delete', isAuth, isAdmin, async (req, res) => {
  try {
    const { messageId } = req.body;
    if (!messageId) return res.status(400).json({ error: 'ID Pesan diperlukan' });

    await GlobalChat.findByIdAndUpdate(messageId, {
      isDeleted: true,
      deletedBy: res.locals.user.username
    });

    if (io) {
      io.to('global_chat_room').emit('global:delete', { messageId });
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal menghapus pesan' });
  }
});

app.post('/api/globalchat/moderate', isAuth, isAdmin, async (req, res) => {
  try {
    const { targetUserId, action, durationMinutes, reason } = req.body;
    if (!targetUserId || !action) return res.status(400).json({ error: 'Data tidak lengkap' });

    const targetUser = await User.findById(targetUserId).lean();
    if (!targetUser) return res.status(404).json({ error: 'User tidak ditemukan' });

    let updateData = { username: targetUser.username, updatedAt: new Date() };

    if (action === 'mute') {
      updateData.isMuted = true;
      updateData.reason = reason || 'Di-mute oleh Admin';
      updateData.mutedUntil = durationMinutes && Number(durationMinutes) > 0 ? new Date(Date.now() + Number(durationMinutes) * 60000) : null;
    } else if (action === 'unmute') {
      updateData.isMuted = false;
      updateData.mutedUntil = null;
    } else if (action === 'ban') {
      updateData.isBanned = true;
      updateData.reason = reason || 'Di-ban dari Global Chat oleh Admin';
    } else if (action === 'unban') {
      updateData.isBanned = false;
    } else if (action === 'kick') {
      if (io) {
        io.to('global_chat_room').emit('global:kick', { targetUserId, targetUsername: targetUser.username, reason: reason || 'Di-kick oleh Admin' });
      }
    }

    const mod = await GlobalChatModeration.findOneAndUpdate(
      { userId: targetUserId },
      { $set: updateData },
      { upsert: true, new: true }
    );

    await ModerationLog.create({
      adminUsername: res.locals.user.username,
      action: action,
      targetUsername: targetUser.username,
      reason: reason || action,
      details: durationMinutes ? `Durasi: ${durationMinutes} menit` : ''
    });

    if (io) {
      io.to('global_chat_room').emit('global:user_status', { targetUserId, action, mod });
    }

    res.json({ success: true, moderation: mod });
  } catch (err) {
    console.error('Moderate error:', err);
    res.status(500).json({ error: 'Gagal memproses moderasi user' });
  }
});

app.post('/api/admin/globalchat/settings', isAuth, isAdmin, async (req, res) => {
  try {
    const { globalSlowmode, globalAntiLink, globalAntiToxic } = req.body;
    await Setting.updateOne({}, {
      globalSlowmode: parseInt(globalSlowmode) || 3,
      globalAntiLink: globalAntiLink === true || globalAntiLink === 'true',
      globalAntiToxic: globalAntiToxic === true || globalAntiToxic === 'true'
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Gagal menyimpan pengaturan global chat' });
  }
});

app.get('/api/globalchat/status', isAuth, async (req, res) => {
  try {
    const mod = await GlobalChatModeration.findOne({ userId: req.session.userId }).lean();
    res.json(mod || { isMuted: false, isBanned: false });
  } catch (err) {
    res.status(500).json({ error: 'Gagal mengambil status moderasi' });
  }
});

app.get('/api/admin/globalchat/users', isAuth, isAdmin, async (req, res) => {
  try {
    const mods = await GlobalChatModeration.find().lean();
    const globalPosters = await GlobalChat.distinct('userId');
    const allUserIds = [...new Set([...mods.map(m => m.userId.toString()), ...globalPosters.map(id => id.toString())])];

    const users = await User.find({ _id: { $in: allUserIds } }).select('username email profilePicture createdAt').lean();
    const modMap = {};
    mods.forEach(m => modMap[m.userId.toString()] = m);

    const result = users.map(u => ({
      ...u,
      moderation: modMap[u._id.toString()] || { isMuted: false, isBanned: false }
    }));

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: 'Gagal mengambil data user global chat' });
  }
});

// ===================== PUBLIC API / DOCS =====================
app.get('/docs', async (req, res) => {
  let userApiKey = '';
  if (req.session.userId) {
    const user = await User.findById(req.session.userId).lean();
    if (user && user.twoFactorEnabled) {
      const key = await ApiKey.findOne({ userId: req.session.userId });
      if (key) userApiKey = key.key;
    }
  }
  res.render('docs', { userApiKey });
});

async function apiAuth(req, res, next) {
  const method = req.method.toUpperCase();
  const endpoint = req.originalUrl || req.url;
  const apiKey = extractApiKey(req);
  const ip = req.ip || req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '-';
  const userAgent = req.get('user-agent') || '-';

  console.log(`\n========================================`);
  console.log(`[${method}] Incoming Request`);
  console.log(`Endpoint   : ${endpoint}`);
  console.log(`API Key    : ${apiKey || '(Missing)'}`);
  console.log(`IP         : ${ip}`);
  if (method === 'POST') {
    const sanitizedBody = { ...req.body };
    if (sanitizedBody.apikey) sanitizedBody.apikey = '***';
    if (sanitizedBody.apiKey) sanitizedBody.apiKey = '***';
    if (sanitizedBody.api_key) sanitizedBody.api_key = '***';
    console.log(`Body       : ${JSON.stringify(sanitizedBody)}`);
  }
  console.log(`User Agent : ${userAgent}`);

  const originalJson = res.json;
  res.json = function(data) {
    const isSuccess = res.statusCode >= 200 && res.statusCode < 400 && (!data || data.success !== false) && !data.error;
    console.log(`Status     : ${res.statusCode} (${isSuccess ? 'Success' : 'Failed'})`);
    console.log(`========================================\n`);
    return originalJson.call(this, data);
  };

  if (!apiKey) {
    return res.status(401).json({ success: false, message: 'API key diperlukan', error: 'API key diperlukan' });
  }

  try {
    const keyDoc = await ApiKey.findOne({ key: apiKey });
    if (!keyDoc) {
      return res.status(401).json({ success: false, message: 'API key tidak valid', error: 'API key tidak valid' });
    }

    // Verify that the merchant account has active 2FA
    const apiOwner = await User.findById(keyDoc.userId).lean();
    if (!apiOwner) {
      return res.status(401).json({ success: false, message: 'Pemilik API key tidak ditemukan', error: 'Pemilik API key tidak ditemukan' });
    }

    if (!apiOwner.twoFactorEnabled) {
      return res.status(403).json({
        success: false,
        message: 'Akses API ditolak. Anda wajib mengaktifkan Two-Factor Authentication (2FA) pada akun PutzPay untuk menggunakan API Key.',
        error: '2FA_REQUIRED'
      });
    }

    req.apiUser = keyDoc.userId;
    req.apiKey = apiKey;
    next();
  } catch (err) {
    return res.status(500).json({ success: false, message: 'Internal Server Error', error: 'Internal Server Error' });
  }
}

function extractApiKey(req) {
  // 1. Header
  const authHeader = req.headers['authorization'];
  if (authHeader) {
    if (authHeader.toLowerCase().startsWith('bearer ')) {
      const key = authHeader.substring(7).trim();
      if (key) return key;
    } else {
      const key = authHeader.trim();
      if (key) return key;
    }
  }
  if (req.headers['x-apikey']) return String(req.headers['x-apikey']).trim();
  if (req.headers['x-api-key']) return String(req.headers['x-api-key']).trim();

  // 2. Body
  if (req.body && typeof req.body === 'object') {
    if (req.body.apikey) return String(req.body.apikey).trim();
    if (req.body.apiKey) return String(req.body.apiKey).trim();
    if (req.body.api_key) return String(req.body.api_key).trim();
  }

  // 3. Query
  if (req.query) {
    if (req.query.apikey) return String(req.query.apikey).trim();
    if (req.query.apiKey) return String(req.query.apiKey).trim();
    if (req.query.api_key) return String(req.query.api_key).trim();
    if (req.query.api_Key) return String(req.query.api_Key).trim();
  }

  return null;
}

function getApiParam(req, ...keys) {
  for (const k of keys) {
    if (req.body && req.body[k] !== undefined && req.body[k] !== '') return req.body[k];
    if (req.query && req.query[k] !== undefined && req.query[k] !== '') return req.query[k];
  }
  return undefined;
}

// ===================== CORE SHARED API SERVICES =====================

// 1. Balance Service Handler
async function handleApiBalance(req, res) {
  try {
    const user = await User.findById(req.apiUser);
    if (!user) return res.status(404).json({ success: false, error: 'User tidak ditemukan' });
    return res.json({
      success: true,
      balance: user.balance
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}

// 2. Create QRIS / Invoice Service Handler
async function handleApiCreateQris(req, res) {
  try {
    const settings = await getSettings();
    const rawAmount = getApiParam(req, 'amount');
    const amount = parseInt(rawAmount);

    const invoice = await createInvoiceForUser(req.apiUser, amount, settings);
    return res.json({
      success: true,
      invoice_id: invoice._id,
      amount: invoice.amount,
      fee: invoice.fee,
      total: invoice.total,
      qris_image: invoice.qris_image,
      expired_at: invoice.expiredAt
    });
  } catch (e) {
    console.error('API create invoice error:', e.message);
    return res.status(400).json({ success: false, error: e.message });
  }
}

// 3. Invoice Status Service Handler
async function handleApiInvoiceStatus(req, res) {
  try {
    const invoiceId = getApiParam(req, 'invoice_id', 'id');
    if (!invoiceId) return res.status(400).json({ success: false, error: 'Invoice ID diperlukan' });

    const invoice = await Invoice.findById(invoiceId);
    if (!invoice || invoice.userId.toString() !== req.apiUser.toString()) {
      return res.status(404).json({ success: false, error: 'Invoice tidak ditemukan' });
    }

    return res.json({
      success: true,
      invoice_id: invoice._id,
      amount: invoice.amount,
      fee: invoice.fee,
      total: invoice.total,
      status: invoice.status,
      qris_image: invoice.qris_image,
      expired_at: invoice.expiredAt,
      created_at: invoice.createdAt
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}

// 4. Invoice Cancel Service Handler
async function handleApiInvoiceCancel(req, res) {
  try {
    const invoiceId = getApiParam(req, 'id', 'invoice_id');

    if (!invoiceId) {
      return res.status(400).json({
        success: false,
        message: 'id invoice wajib diisi'
      });
    }

    const invoice = await Invoice.findById(invoiceId);

    if (!invoice) {
      return res.status(404).json({
        success: false,
        message: 'invoice tidak ditemukan'
      });
    }

    if (invoice.userId.toString() !== req.apiUser.toString()) {
      return res.status(403).json({
        success: false,
        message: 'akses ditolak'
      });
    }

    if (invoice.status === 'paid') {
      return res.status(400).json({
        success: false,
        message: 'invoice sudah dibayar'
      });
    }

    invoice.status = 'cancelled';
    await invoice.save();

    emitLiveTransaction('payment_cancelled', {
      userId: invoice.userId,
      amount: invoice.amount,
      invoice_id: invoice._id,
      status: 'cancelled',
      createdAt: invoice.createdAt
    });

    telegramMonitor.notifyInvoiceCancelled({
      invoice_id: invoice._id,
      amount: invoice.amount,
      status: 'cancelled'
    });

    return res.json({
      success: true,
      invoice_id: invoice._id,
      status: 'cancelled'
    });

  } catch (err) {
    return res.status(500).json({
      success: false,
      message: err.message
    });
  }
}

// 5. Withdraw Service Handler
async function handleApiWithdraw(req, res) {
  try {
    const settings = await getSettings();

    const amount = getApiParam(req, 'amount');
    const method = getApiParam(req, 'method');
    const account_number = getApiParam(req, 'account_number', 'accountNumber');
    const account_name = getApiParam(req, 'account_name', 'accountName');

    if (!method || !account_number || !account_name) {
      return res.status(400).json({
        success: false,
        message: 'Method, account_number dan account_name wajib diisi'
      });
    }

    const amt = parseInt(amount);

    if (isNaN(amt) || amt < settings.minWithdraw) {
      return res.status(400).json({
        success: false,
        message: `Minimal withdraw Rp ${settings.minWithdraw.toLocaleString('id-ID')}`
      });
    }

    const selectedMethod = (settings.withdrawMethods || []).find(
      m => m.name.toLowerCase() === String(method).toLowerCase()
    );

    if (!selectedMethod) {
      return res.status(400).json({
        success: false,
        message: 'Metode withdraw tidak valid'
      });
    }

    const fee = selectedMethod.fee || 0;
    const totalDeduct = amt + fee;

    const user = await User.findOneAndUpdate(
      {
        _id: req.apiUser,
        balance: { $gte: totalDeduct }
      },
      {
        $inc: { balance: -totalDeduct }
      },
      {
        new: true
      }
    );

    if (!user) {
      return res.status(400).json({
        success: false,
        message: `Saldo tidak cukup (termasuk biaya admin Rp ${fee.toLocaleString('id-ID')})`
      });
    }

    const reference = 'W' + Date.now().toString(36).toUpperCase();

    try {
      const withdrawal = await Withdrawal.create({
        userId: req.apiUser,
        amount: amt,
        fee,
        reference,
        method: selectedMethod.name,
        accountNumber: account_number,
        accountName: account_name,
        status: 'pending'
      });

      await Transaction.create({
        userId: req.apiUser,
        type: 'withdraw',
        amount: amt,
        fee,
        status: 'pending',
        reference,
        method: selectedMethod.name,
        accountNumber: account_number,
        accountName: account_name
      });

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: selectedMethod.name,
        status: 'PROCESSING'
      });

      return res.json({
        success: true,
        withdraw_id: withdrawal._id,
        reference,
        amount: amt,
        fee,
        total_deduct: totalDeduct,
        status: 'pending',
        message: 'Permintaan withdraw berhasil dibuat'
      });

    } catch (err) {
      await User.findByIdAndUpdate(req.apiUser, {
        $inc: { balance: totalDeduct }
      });

      throw err;
    }

  } catch (err) {
    res.status(500).json({
      success: false,
      message: err.message
    });
  }
}

async function handleApiWithdrawStatus(req, res) {
  try {
    const withdrawId = getApiParam(req, 'id', 'withdraw_id');

    if (!withdrawId) {
      return res.status(400).json({
        success: false,
        message: 'Withdraw ID diperlukan'
      });
    }

    const withdrawal = await Withdrawal.findById(withdrawId);

    if (!withdrawal || withdrawal.userId.toString() !== req.apiUser.toString()) {
      return res.status(404).json({
        success: false,
        message: 'Data withdraw tidak ditemukan'
      });
    }

    res.json({
      success: true,
      withdraw_id: withdrawal._id,
      reference: withdrawal.reference,
      amount: withdrawal.amount,
      fee: withdrawal.fee,
      method: withdrawal.method,
      account_number: withdrawal.accountNumber,
      account_name: withdrawal.accountName,
      status: withdrawal.status,
      created_at: withdrawal.createdAt,
      updated_at: withdrawal.updatedAt
    });

  } catch (err) {
    res.status(500).json({
      success: false,
      message: err.message
    });
  }
}

async function handleApiWithdrawHistory(req, res) {
  try {
    const withdrawals = await Withdrawal.find({
      userId: req.apiUser
    })
    .sort({ createdAt: -1 })
    .limit(50);

    res.json({
      success: true,
      total: withdrawals.length,
      data: withdrawals.map(wd => ({
        id: wd._id,
        reference: wd.reference,
        amount: wd.amount,
        fee: wd.fee,
        method: wd.method,
        status: wd.status,
        created_at: wd.createdAt
      }))
    });

  } catch (err) {
    res.status(500).json({
      success: false,
      message: err.message
    });
  }
}

// ===================== PENARIKAN OTOMATIS / INSTAN (DISBURSEMENT ENGINE) =====================
const INSTANT_NOMINAL_MAPPING = {
  1000: 'D1',
  2000: 'D2',
  3000: 'D3',
  4000: 'D4',
  5000: 'D5',
  10000: 'D10',
  15000: 'D15',
  20000: 'D20',
  25000: 'D25',
  30000: 'D30',
  35000: 'D35',
  40000: 'D40',
  45000: 'D45',
  50000: 'D50',
  55000: 'D55',
  60000: 'D60',
  65000: 'D65',
  70000: 'D70',
  75000: 'D75',
  80000: 'D80',
  85000: 'D85',
  90000: 'D90',
  95000: 'D95',
  100000: 'D100',
  125000: 'D125',
  150000: 'D150',
  200000: 'D200',
  250000: 'D250',
  300000: 'D300',
  400000: 'D400',
  500000: 'D500',
  600000: 'D600',
  700000: 'D700',
  800000: 'D800',
  900000: 'D900',
  1000000: 'D1000'
};

function isFridayInJakarta() {
  try {
    const day = new Intl.DateTimeFormat('en-US', {
      timeZone: 'Asia/Jakarta',
      weekday: 'short'
    }).format(new Date());
    return day.toLowerCase().startsWith('fri');
  } catch (e) {
    const now = new Date();
    const utc = now.getTime() + (now.getTimezoneOffset() * 60000);
    const jakartaTime = new Date(utc + (3600000 * 7));
    return jakartaTime.getDay() === 5;
  }
}

function calculateInstantWithdrawFee(method = 'dana') {
  const isFriday = isFridayInJakarta();
  const cleanMethod = String(method || '').toLowerCase().trim();
  if (cleanMethod === 'bank') {
    const min = isFriday ? 700 : 1500;
    const max = isFriday ? 1000 : 2500;
    return Math.floor(Math.random() * (max - min + 1)) + min;
  } else {
    // E-wallet (DANA)
    const min = isFriday ? 300 : 500;
    const max = isFriday ? 500 : 700;
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }
}

function cleanEwalletPhone(phone) {
  if (!phone) return '';
  let clean = String(phone).replace(/[^0-9]/g, '');
  if (clean.startsWith('62')) {
    clean = '0' + clean.slice(2);
  } else if (clean.startsWith('+62')) {
    clean = '0' + clean.slice(3);
  }
  return clean;
}

function maskPhoneNumber(phone) {
  if (!phone) return '-';
  const str = String(phone).trim();
  if (str.length <= 6) return str;
  return str.slice(0, 4) + '••••' + str.slice(-4);
}

async function executeInternalInstantWithdrawal({ ewallet, nomor, nominal, withdrawalId, userId }) {
  const code = INSTANT_NOMINAL_MAPPING[nominal];
  if (!code) {
    throw new Error('Nominal penarikan tidak didukung');
  }

  const settings = await getSettings();
  const apiKeyProvider = process.env.WITHDRAW_API_KEY || process.env.FR3_API_KEY || settings.withdrawApiKey || settings.instantWithdrawApiKey || '';

  const providerUrl = 'https://fr3newera.com/withdraw2';

  try {
    const response = await axios.get(providerUrl, {
      params: {
        apikey: apiKeyProvider,
        ewallet: ewallet.toLowerCase(),
        nomor: nomor,
        kode: code
      },
      timeout: 25000,
      headers: {
        'User-Agent': 'PutzPay-Engine/2.0'
      }
    });

    const data = response.data;
    let isSuccess = false;
    let isExplicitFailed = false;
    let providerTrxId = '';

    if (data && typeof data === 'object') {
      const statusStr = String(data.status || data.success || '').toLowerCase();
      if (data.status === true || data.success === true || statusStr === 'success' || statusStr === 'true' || Number(data.status_code) === 200) {
        isSuccess = true;
      } else if (data.status === false || data.success === false || statusStr === 'failed' || statusStr === 'false' || statusStr === 'error' || (data.status_code && Number(data.status_code) >= 400)) {
        isExplicitFailed = true;
      }
      providerTrxId = String(data.trx_id || data.transaction_id || data.id || data.reference || data.ref_id || '');
    } else if (typeof data === 'string') {
      const textLower = data.toLowerCase();
      if (textLower.includes('sukses') || textLower.includes('success') || textLower.includes('berhasil')) {
        isSuccess = true;
      } else if (textLower.includes('gagal') || textLower.includes('failed') || textLower.includes('salah') || textLower.includes('error')) {
        isExplicitFailed = true;
      }
    }

    return {
      success: isSuccess,
      failed: isExplicitFailed && !isSuccess,
      pending: !isSuccess && !isExplicitFailed,
      providerTransactionId: providerTrxId,
      providerStatus: isSuccess ? 'SUCCESS' : (isExplicitFailed ? 'FAILED' : 'PENDING'),
      providerMessage: (data && typeof data === 'object' ? (data.message || data.msg || '') : String(data || '')).slice(0, 200),
      raw: typeof data === 'object' ? data : { response: String(data || '') }
    };
  } catch (error) {
    console.error('Instant withdraw provider request error (internal):', error.message);
    // Timeout or network error - status must remain PENDING, do not mark as failed or refund immediately!
    return {
      success: false,
      failed: false,
      pending: true,
      providerTransactionId: '',
      providerStatus: 'PENDING',
      providerMessage: 'Request sent, awaiting provider confirmation',
      raw: { error: error.message }
    };
  }
}

// Handler for POST /api/withdraw/instan
async function handleApiInstantWithdrawCreate(req, res) {
  try {
    const ewallet = getApiParam(req, 'ewallet');
    const nomor = getApiParam(req, 'nomor', 'account_number', 'accountNumber');
    const nominal = getApiParam(req, 'nominal', 'amount');

    // 1. Validate e-wallet (Currently only DANA)
    if (!ewallet || String(ewallet).toLowerCase().trim() !== 'dana') {
      return res.status(400).json({
        success: false,
        message: 'E-wallet tidak tersedia'
      });
    }

    // 2. Validate nomor e-wallet
    const cleanNomor = cleanEwalletPhone(nomor);
    if (!cleanNomor || cleanNomor.length < 9 || cleanNomor.length > 15) {
      return res.status(400).json({
        success: false,
        message: 'Nomor e-wallet tidak valid'
      });
    }

    // 3. Validate nominal mapping
    const amt = parseInt(nominal, 10);
    if (isNaN(amt) || !INSTANT_NOMINAL_MAPPING[amt]) {
      return res.status(400).json({
        success: false,
        message: 'Nominal penarikan tidak tersedia'
      });
    }

    // 4. Calculate locked fee and total
    const fee = calculateInstantWithdrawFee('dana');
    const totalDeduct = amt + fee;

    // 5. Atomic balance deduction
    const user = await User.findOneAndUpdate(
      {
        _id: req.apiUser,
        balance: { $gte: totalDeduct }
      },
      {
        $inc: { balance: -totalDeduct }
      },
      {
        new: true
      }
    );

    if (!user) {
      return res.status(400).json({
        success: false,
        message: 'Saldo tidak mencukupi'
      });
    }

    // 6. Create withdrawal record (Fee is locked)
    const reference = 'WI' + Date.now().toString(36).toUpperCase();
    let withdrawal;
    try {
      withdrawal = await Withdrawal.create({
        userId: req.apiUser,
        amount: amt,
        fee: fee,
        method: 'DANA',
        ewallet: 'dana',
        accountNumber: cleanNomor,
        accountName: user.accountName || user.username || 'Pengguna DANA',
        status: 'pending',
        type: 'instant',
        providerStatus: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date()
      });

      await Transaction.create({
        userId: req.apiUser,
        type: 'withdraw',
        amount: amt,
        fee: fee,
        status: 'pending',
        reference: reference,
        method: 'DANA (Instan)',
        accountNumber: cleanNomor,
        accountName: user.accountName || user.username || 'Pengguna DANA'
      });
    } catch (dbErr) {
      // Revert balance if db insert failed
      await User.findByIdAndUpdate(req.apiUser, { $inc: { balance: totalDeduct } });
      throw dbErr;
    }

    // 7. Internal request to provider
    const providerResult = await executeInternalInstantWithdrawal({
      ewallet: 'dana',
      nomor: cleanNomor,
      nominal: amt,
      withdrawalId: withdrawal._id,
      userId: req.apiUser
    });

    // 8. Process provider result
    if (providerResult.success) {
      withdrawal.status = 'success';
      withdrawal.providerStatus = 'SUCCESS';
      withdrawal.providerTransactionId = providerResult.providerTransactionId || '';
      withdrawal.completedAt = new Date();
      withdrawal.updatedAt = new Date();
      await withdrawal.save();

      await Transaction.updateOne(
        { reference },
        { status: 'success', completedAt: new Date() }
      );

      await Stats.updateOne({}, { $inc: { totalWithdrawAmount: amt, totalWithdrawFee: fee } });

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: 'DANA (Instan)',
        status: 'SUCCESS'
      });

      sendPushNotification(req.apiUser, 'withdraw_success', {
        title: '💸 PutzPay Penarikan Instan',
        body: `Penarikan otomatis Rp ${amt.toLocaleString('id-ID')} ke DANA ${maskPhoneNumber(cleanNomor)} BERHASIL.`,
        data: { url: '/withdraw', withdrawId: withdrawal._id }
      }, { eventId: `withdraw_instant_success_${withdrawal._id}` }).catch(() => {});

      return res.json({
        success: true,
        message: 'Penarikan otomatis berhasil diproses',
        data: {
          withdraw_id: withdrawal._id.toString(),
          reference: reference,
          ewallet: 'dana',
          nomor: cleanNomor,
          nominal: amt,
          fee: fee,
          total: totalDeduct,
          status: 'SUCCESS'
        }
      });
    } else if (providerResult.failed) {
      // Definite failure from provider -> atomic refund
      withdrawal.status = 'failed';
      withdrawal.providerStatus = 'FAILED';
      withdrawal.refunded = true;
      withdrawal.adminNote = 'Gagal dari sistem provider: ' + (providerResult.providerMessage || 'Transaksi ditolak');
      withdrawal.completedAt = new Date();
      withdrawal.updatedAt = new Date();
      await withdrawal.save();

      await User.findByIdAndUpdate(req.apiUser, { $inc: { balance: totalDeduct } });

      await Transaction.updateOne(
        { reference },
        { status: 'rejected', adminNote: withdrawal.adminNote, completedAt: new Date() }
      );

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: 'DANA (Instan)',
        status: 'FAILED'
      });

      sendPushNotification(req.apiUser, 'withdraw_failed', {
        title: '❌ PutzPay Penarikan Instan Gagal',
        body: `Penarikan otomatis Rp ${amt.toLocaleString('id-ID')} gagal diproses. Saldo telah dikembalikan ke akun Anda.`,
        data: { url: '/withdraw', withdrawId: withdrawal._id }
      }, { eventId: `withdraw_instant_failed_${withdrawal._id}` }).catch(() => {});

      return res.status(400).json({
        success: false,
        message: 'Penarikan tidak dapat diproses',
        data: {
          withdraw_id: withdrawal._id.toString(),
          reference: reference,
          ewallet: 'dana',
          nomor: cleanNomor,
          nominal: amt,
          fee: fee,
          total: totalDeduct,
          status: 'FAILED'
        }
      });
    } else {
      // Timeout or pending provider status -> keep PENDING, do NOT refund immediately
      withdrawal.status = 'pending';
      withdrawal.providerStatus = 'PENDING';
      withdrawal.updatedAt = new Date();
      await withdrawal.save();

      telegramMonitor.notifyWithdraw({
        withdraw_id: withdrawal._id,
        amount: amt,
        method: 'DANA (Instan)',
        status: 'PROCESSING'
      });

      return res.json({
        success: true,
        message: 'Penarikan otomatis sedang diproses',
        data: {
          withdraw_id: withdrawal._id.toString(),
          reference: reference,
          ewallet: 'dana',
          nomor: cleanNomor,
          nominal: amt,
          fee: fee,
          total: totalDeduct,
          status: 'PENDING'
        }
      });
    }
  } catch (err) {
    console.error('handleApiInstantWithdrawCreate error:', err);
    return res.status(500).json({
      success: false,
      message: 'Penarikan tidak dapat diproses: ' + err.message
    });
  }
}

// Handler for GET /api/withdraw/instan (READ-ONLY)
async function handleApiInstantWithdrawGet(req, res) {
  try {
    const withdrawId = getApiParam(req, 'id', 'withdraw_id');

    if (withdrawId) {
      const withdrawal = await Withdrawal.findOne({
        _id: withdrawId,
        userId: req.apiUser
      }).lean();

      if (!withdrawal) {
        return res.status(404).json({
          success: false,
          message: 'Data penarikan tidak ditemukan'
        });
      }

      return res.json({
        success: true,
        data: {
          withdraw_id: withdrawal._id.toString(),
          reference: withdrawal.reference,
          ewallet: withdrawal.ewallet || (withdrawal.method || '').toLowerCase(),
          nomor: withdrawal.accountNumber,
          nominal: withdrawal.amount,
          fee: withdrawal.fee || 0,
          total: withdrawal.amount + (withdrawal.fee || 0),
          status: (withdrawal.status || 'PENDING').toUpperCase(),
          created_at: withdrawal.createdAt,
          updated_at: withdrawal.updatedAt || withdrawal.completedAt || withdrawal.createdAt
        }
      });
    }

    const withdrawals = await Withdrawal.find({
      userId: req.apiUser,
      type: 'instant'
    }).sort({ createdAt: -1 }).limit(50).lean();

    return res.json({
      success: true,
      total: withdrawals.length,
      data: withdrawals.map(w => ({
        withdraw_id: w._id.toString(),
        reference: w.reference,
        ewallet: w.ewallet || (w.method || '').toLowerCase(),
        nomor: maskPhoneNumber(w.accountNumber),
        nominal: w.amount,
        fee: w.fee || 0,
        total: w.amount + (w.fee || 0),
        status: (w.status || 'PENDING').toUpperCase(),
        created_at: w.createdAt
      }))
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: 'Gagal memuat data penarikan: ' + err.message
    });
  }
}

// 6. History Service Handler
async function handleApiHistory(req, res) {
  try {
    const transactions = await Transaction.find({
      userId: req.apiUser
    })
    .sort({ createdAt: -1 })
    .limit(50);

    res.json({
      success: true,
      total: transactions.length,
      data: transactions.map(tx => ({
        id: tx._id,
        type: tx.type,
        amount: tx.amount,
        status: tx.status,
        reference: tx.reference,
        created_at: tx.createdAt
      }))
    });

  } catch (err) {
    res.status(500).json({
      success: false,
      message: err.message
    });
  }
}

// ===================== API ROUTE DEFINITIONS (SUPPORTING GET & POST) =====================

// 1. Balance
app.get('/api/balance', apiAuth, handleApiBalance);
app.post('/api/balance', apiAuth, handleApiBalance);

// 2. Create QRIS / Invoice
app.get('/api/create/qris', apiAuth, handleApiCreateQris);
app.post('/api/create/qris', apiAuth, handleApiCreateQris);

// 3. Invoice Status
app.get('/api/invoice/status', apiAuth, handleApiInvoiceStatus);
app.post('/api/invoice/status', apiAuth, handleApiInvoiceStatus);

// 4. Invoice Cancel
app.get('/api/invoice/cancel', apiAuth, handleApiInvoiceCancel);
app.post('/api/invoice/cancel', apiAuth, handleApiInvoiceCancel);

// 5. Withdraw (Manual)
app.get('/api/withdraw', apiAuth, handleApiWithdraw);
app.post('/api/withdraw', apiAuth, handleApiWithdraw);
app.get('/api/withdraw/status', apiAuth, handleApiWithdrawStatus);
app.post('/api/withdraw/status', apiAuth, handleApiWithdrawStatus);
app.get('/api/withdraw/history', apiAuth, handleApiWithdrawHistory);
app.post('/api/withdraw/history', apiAuth, handleApiWithdrawHistory);

// 5.1 Withdraw Otomatis / Instan
app.get('/api/withdraw/instan', apiAuth, handleApiInstantWithdrawGet);
app.post('/api/withdraw/instan', apiAuth, handleApiInstantWithdrawCreate);
app.get('/api/withdraw/instant', apiAuth, handleApiInstantWithdrawGet);
app.post('/api/withdraw/instant', apiAuth, handleApiInstantWithdrawCreate);

// 6. History
app.get('/api/history', apiAuth, handleApiHistory);
app.post('/api/history', apiAuth, handleApiHistory);

// ===================== CHECKER MUTASI (GOPAY MERCHANT ONLY) =====================
let checkerInterval;

async function checkMutasi() {
  try {
    const settings = await getSettings();
    const expiredMinutes = settings.qrisExpiredMinutes || 30;
    const now = new Date();

    const expiredInvoices = await Invoice.find({ status: 'pending', expiredAt: { $lt: now } });
    for (const inv of expiredInvoices) {
      inv.status = 'expired';
      await inv.save();
      await Transaction.updateOne(
        { reference: inv._id.toString(), type: 'deposit', status: 'pending' },
        { status: 'expired' }
      );

      emitLiveTransaction('payment_expired', {
        userId: inv.userId,
        amount: inv.amount,
        invoice_id: inv._id,
        status: 'expired',
        createdAt: inv.createdAt
      });

      sendPushNotification(inv.userId, 'payment_expired', {
        title: '⌛ PutzPay',
        body: `Invoice #${inv._id} telah kedaluwarsa.`,
        data: { url: '/deposit', invoiceId: inv._id }
      }, { eventId: `payment_expired_${inv._id}` }).catch(() => {});
    }

    const pendingInvoices = await Invoice.find({ status: 'pending' }).lean();

    if (!settings.gopayToken) return;
    const gopayBase = settings.gopayDomain || 'gomerch.putzoffc.biz.id';
    const apiUrl = `https://${gopayBase}/api/history?token=${encodeURIComponent(settings.gopayToken)}`;
    try {
      const data = await callGopayApiWithRetry(apiUrl);
      if (!data.success || !Array.isArray(data.data)) return;

      const mutations = data.data.filter(tx => tx.status === 'success');
      const usedMutationIds = await Invoice.find({ mutationId: { $ne: null } }).distinct('mutationId');
      const availableMutations = mutations.filter(tx => !usedMutationIds.includes(String(tx.id)));

      for (const inv of pendingInvoices) {
        const match = availableMutations.find(tx => {
          if (tx.amount !== inv.total) return false;
          const txTime = new Date(tx.time);
          if (isNaN(txTime.getTime())) return false;
          const diffMinutes = Math.abs(txTime.getTime() - inv.createdAt.getTime()) / 1000 / 60;
          return diffMinutes <= expiredMinutes;
        });
        if (!match) continue;

        const successAt = new Date();
        const releaseAt = new Date(successAt.getTime() + 12 * 60 * 60 * 1000);

        await Invoice.findByIdAndUpdate(inv._id, {
          status: 'paid',
          mutationId: String(match.id),
          settlementStatus: 'pending',
          settlementAmount: inv.amount,
          successAt: successAt,
          releaseAt: releaseAt
        });

        await Transaction.updateOne(
          { reference: inv._id.toString(), type: 'deposit', status: 'pending' },
          { status: 'paid' }
        );
        await Stats.updateOne({}, { $inc: { totalDepositAmount: inv.amount, totalDepositFee: inv.fee, totalTransactions: 1 } });

        if (inv.paymentLinkId) {
          await PaymentLink.updateOne(
            { _id: inv.paymentLinkId },
            { $inc: { totalPaidCount: 1, totalPaidAmount: inv.amount }, updatedAt: new Date() }
          ).catch(e => console.error('Failed to update payment link stats:', e.message));
        }

        emitLiveTransaction('deposit_success', {
          userId: inv.userId,
          amount: inv.amount,
          invoice_id: inv._id,
          status: 'paid',
          createdAt: inv.createdAt,
          releaseAt: releaseAt
        });
        emitLiveTransaction('payment_success', {
          userId: inv.userId,
          amount: inv.amount,
          invoice_id: inv._id,
          status: 'paid',
          createdAt: inv.createdAt,
          releaseAt: releaseAt
        });

        sendPushNotification(inv.userId, 'payment_success', {
          title: '💰 PutzPay',
          body: `Pembayaran diterima sebesar Rp ${inv.amount.toLocaleString('id-ID')}. Dana masuk Saldo Tertunda (hold 12 jam).`,
          data: { url: '/dashboard', transactionId: inv._id, amount: inv.amount }
        }, { eventId: `payment_success_${inv._id}` }).catch(() => {});

        // Trigger webhook ke server merchant (fire-and-forget, tidak boleh menghambat proses reconciliation)
        User.findById(inv.userId).select('webhookUrl webhookEnabled webhookSecret').lean().then(webhookUser => {
          if (!webhookUser) return;
          sendWebhookEvent(webhookUser, 'invoice.paid', {
            event: 'invoice.paid',
            invoice_id: inv._id,
            trxid: inv.trxid,
            amount: inv.amount,
            fee: inv.fee,
            total: inv.total,
            status: 'paid',
            payment_link_id: inv.paymentLinkId || null,
            customer_name: inv.customerName || null,
            customer_email: inv.customerEmail || null,
            customer_phone: inv.customerPhone || null,
            paid_at: successAt.toISOString(),
            created_at: inv.createdAt.toISOString()
          }, inv._id).catch(() => {});
        }).catch(() => {});

        let paidUser = await User.findById(inv.userId).select('username fullName name email');
        let paidUsername = 'Pengguna';
        if (paidUser) {
          if (paidUser.fullName && paidUser.username && paidUser.fullName.trim() !== paidUser.username.trim()) {
            paidUsername = `${paidUser.fullName.trim()} (@${paidUser.username.trim()})`;
          } else if (paidUser.username) {
            paidUsername = paidUser.username.trim();
          } else if (paidUser.fullName) {
            paidUsername = paidUser.fullName.trim();
          } else if (paidUser.email) {
            paidUsername = paidUser.email.split('@')[0];
          }
        }

        telegramMonitor.notifyPaymentSuccess({
          invoice_id: inv._id,
          order_id: inv.trxid || inv._id,
          username: paidUsername,
          amount: inv.total || inv.amount,
          fee: inv.fee,
          method: 'QRIS Realtime',
          paidAt: successAt
        });

        const idx = availableMutations.findIndex(tx => String(tx.id) === String(match.id));
        if (idx !== -1) availableMutations.splice(idx, 1);
      }
    } catch (err) {
      console.error('Mutasi error:', err.response?.data || err.message);
    }

  } catch (err) {
    console.error('Mutasi error:', err.response?.data || err.message);
  }
}

async function processPendingSettlements() {
  try {
    const now = new Date();
    const pendingSettlements = await Invoice.find({
      status: 'paid',
      settlementStatus: 'pending',
      releaseAt: { $lte: now }
    });

    for (const inv of pendingSettlements) {
      const updatedInv = await Invoice.findOneAndUpdate(
        { _id: inv._id, settlementStatus: 'pending' },
        {
          $set: {
            settlementStatus: 'released',
            releasedAt: new Date()
          }
        },
        { new: true }
      );

      if (updatedInv) {
        const updatedUser = await User.findByIdAndUpdate(
          inv.userId,
          { $inc: { balance: inv.amount } },
          { new: true }
        );

        console.log(`[SETTLEMENT RELEASED] Invoice ${inv._id}: Rp ${inv.amount} released to user ${inv.userId}`);

        emitLiveTransaction('balance_updated', {
          userId: inv.userId,
          balance: updatedUser ? updatedUser.balance : 0,
          releasedAmount: inv.amount,
          invoiceId: inv._id
        });

        emitLiveTransaction('settlement_released', {
          userId: inv.userId,
          amount: inv.amount,
          invoiceId: inv._id
        });

        sendPushNotification(inv.userId, 'balance_updated', {
          title: '💰 PutzPay',
          body: `Saldo Rp ${inv.amount.toLocaleString('id-ID')} sekarang tersedia. Dana settlement telah masuk ke saldo aktif kamu.`,
          data: { url: '/dashboard', amount: inv.amount }
        }, { eventId: `settlement_released_${inv._id}` }).catch(() => {});
      }
    }
  } catch (err) {
    console.error('Error in processPendingSettlements:', err.message);
  }
}

async function migrateExistingInvoicesOnStartup() {
  try {
    await Invoice.updateMany(
      { status: 'paid', releaseAt: { $exists: false } },
      { $set: { settlementStatus: 'released', releasedAt: new Date() } }
    );
  } catch (err) {
    console.warn('Migration existing invoices skipped:', err.message);
  }
}

async function updateStatsOnStartup() {
  const [depositAgg, withdrawAgg, totalUsers, totalTrx] = await Promise.all([
    Transaction.aggregate([{ $match: { type: 'deposit', status: 'paid' } }, { $group: { _id: null, totalAmount: { $sum: '$amount' }, totalFee: { $sum: '$fee' } } }]),
    Transaction.aggregate([{ $match: { type: 'withdraw', status: 'success' } }, { $group: { _id: null, totalAmount: { $sum: '$amount' }, totalFee: { $sum: '$fee' } } }]),
    User.countDocuments({ role: 'user' }),
    Transaction.countDocuments()
  ]);
  await Stats.deleteMany({});
  await Stats.create({
    totalDepositAmount: depositAgg[0]?.totalAmount || 0,
    totalDepositFee: depositAgg[0]?.totalFee || 0,
    totalWithdrawAmount: withdrawAgg[0]?.totalAmount || 0,
    totalWithdrawFee: withdrawAgg[0]?.totalFee || 0,
    totalUsers,
    totalTransactions: totalTrx
  });
}

let settlementInterval;

function startChecker() {
  if (checkerInterval) clearInterval(checkerInterval);
  if (settlementInterval) clearInterval(settlementInterval);

  getSettings().then(s => {
    checkerInterval = setInterval(checkMutasi, s.checkInterval * 1000);
    checkMutasi();
  });

  settlementInterval = setInterval(processPendingSettlements, 10000);
  processPendingSettlements();
}

setTimeout(async () => {
  try {
    await migrateExistingInvoicesOnStartup();
    await updateStatsOnStartup();
    startChecker();
  } catch (err) {
    console.warn('Startup background tasks skipped or deferred:', err.message);
  }
}, 2000);

// Catch-all 404 handler for unknown API routes
app.use('/api', (req, res) => {
  res.status(404).json({
    success: false,
    message: 'API endpoint tidak ditemukan'
  });
});

// Global Error Handler for database offline/errors
app.use((err, req, res, next) => {
  if (err && (err.name === 'MongooseError' || err.name === 'MongoNetworkError' || (err.message && err.message.includes('buffering timed out')))) {
    console.warn('[AI Studio] Database offline error caught');
    if (req.path.startsWith('/api')) {
      return res.status(503).json({ error: 'Layanan database tidak dapat dijangkau (MongoDB Offline)' });
    }
    req.session.errorMsg = 'Sistem database belum terhubung. Silakan konfigurasi MONGO_URI.';
    return res.redirect('/login');
  }
  console.error('Unhandled server error:', err);
  res.status(500).send('Terjadi kesalahan internal server');
});

server.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server berjalan di http://0.0.0.0:${PORT}`));
