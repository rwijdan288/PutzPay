/**
 * PutzPay Centralized SMTP Service
 * Unified Nodemailer transporter and delivery handling for:
 * 1. Mode 1: STARTTLS (Port 587, secure: false, requireTLS: true)
 * 2. Mode 2: SSL/TLS Direct (Port 465, secure: true, requireTLS: false)
 * 
 * Features:
 * - Strict consistency between Port and Security Mode
 * - Safe error mapping with structured JSON codes
 * - Cryptographically safe logging (no credentials, tokens, or plaintext secrets)
 * - Centralized use across Test Email, Registration OTP, Resend OTP, Password Reset, and Notifications
 */

const nodemailer = require('nodemailer');
const dns = require('dns');
const net = require('net');

/**
 * Mask email for safe privacy logging
 */
function maskEmail(email) {
  if (!email || typeof email !== 'string') return '***@***';
  const parts = email.split('@');
  if (parts.length !== 2) return '***';
  const name = parts[0];
  const domain = parts[1];
  const maskedName = name.length <= 2 
    ? name[0] + '***' 
    : name.substring(0, 2) + '***' + name.substring(name.length - 1);
  return `${maskedName}@${domain}`;
}

/**
 * Resolve hostname to IPv4 to prevent container IPv6 routing drop (ENETUNREACH / ETIMEDOUT)
 */
async function resolveToIpv4(hostname) {
  if (!hostname || typeof hostname !== 'string' || net.isIP(hostname)) {
    return hostname;
  }
  try {
    const addresses = await dns.promises.resolve4(hostname);
    if (addresses && addresses.length > 0) {
      return addresses[0];
    }
  } catch (e) {
    try {
      const res = await dns.promises.lookup(hostname, { family: 4 });
      if (res && res.address) return res.address;
    } catch (err) {}
  }
  return hostname;
}

/**
 * Map raw SMTP errors into clear, structured codes and Indonesian messages
 */
function mapSmtpError(err) {
  if (!err) {
    return {
      code: 'SMTP_UNKNOWN_ERROR',
      message: 'Terjadi kesalahan yang tidak diketahui saat menghubungi server SMTP.'
    };
  }

  const errCode = (err.code || '').toUpperCase();
  const errMsg = (err.message || '').toLowerCase();
  const respCode = Number(err.responseCode) || 0;

  // Authentication error (EAUTH / 535 / BadCredentials)
  if (errCode === 'EAUTH' || respCode === 535 || errMsg.includes('badcredentials') || errMsg.includes('auth') || errMsg.includes('invalid login') || errMsg.includes('username and password not accepted')) {
    return {
      code: 'SMTP_AUTH_FAILED',
      message: 'Autentikasi SMTP gagal (535 Bad Credentials). Pastikan Email Pengirim dan Google App Password (16 karakter) sudah benar di Pengaturan Sistem.'
    };
  }

  // Host not found / DNS lookup error
  if (errCode === 'ENOTFOUND' || errMsg.includes('getaddrinfo enotfound') || errMsg.includes('dns lookup')) {
    return {
      code: 'SMTP_HOST_NOT_FOUND',
      message: 'SMTP Host tidak ditemukan. Periksa kembali alamat server SMTP pada pengaturan.'
    };
  }

  // Connection timeout
  if (errCode === 'ETIMEDOUT' || errCode === 'ESOCKETTIMEDOUT' || errMsg.includes('timeout') || errMsg.includes('timed out')) {
    return {
      code: 'SMTP_CONNECTION_TIMEOUT',
      message: 'Koneksi ke server SMTP mengalami timeout. Periksa Host, Port (465 atau 587), atau firewall VPS/hosting yang mungkin membatasi koneksi outbound.'
    };
  }

  // Connection refused
  if (errCode === 'ECONNREFUSED' || errMsg.includes('connection refused')) {
    return {
      code: 'SMTP_CONNECTION_REFUSED',
      message: 'Koneksi ke server SMTP ditolak (ECONNREFUSED). Periksa nomor port dan pastikan port terbuka.'
    };
  }

  // Network unreachable
  if (errCode === 'ENETUNREACH' || errCode === 'EHOSTUNREACH' || errMsg.includes('network is unreachable')) {
    return {
      code: 'SMTP_NETWORK_UNREACHABLE',
      message: 'Jaringan server tidak dapat menjangkau server SMTP (ENETUNREACH).'
    };
  }

  // STARTTLS / TLS Handshake error
  if (errMsg.includes('starttls') || errMsg.includes('tls') || errMsg.includes('handshake') || errMsg.includes('certificate')) {
    return {
      code: 'SMTP_STARTTLS_FAILED',
      message: 'Koneksi keamanan TLS/STARTTLS gagal. Periksa konsistensi konfigurasi port dan protokol keamanan.'
    };
  }

  return {
    code: errCode || 'SMTP_SEND_FAILED',
    message: err.message || 'Gagal mengirim email melalui server SMTP.'
  };
}

/**
 * Build consistent Nodemailer configuration based on Port & Security mode
 * Strict rule:
 * - Port 587 => STARTTLS (secure: false, requireTLS: true)
 * - Port 465 => SSL/TLS Direct (secure: true, requireTLS: false)
 */
async function createTransporter(config, overridePort = null) {
  const rawHost = (config.host || 'smtp.gmail.com').trim();
  const effectivePort = Number(overridePort || config.port || 465);

  const isPort465 = (effectivePort === 465);
  const isPort587 = (effectivePort === 587);

  // If port is 465, secure MUST be true, requireTLS false
  // If port is 587, secure MUST be false, requireTLS true
  let secure = isPort465;
  let requireTLS = isPort587;

  // If non-standard port, defer to config.secure
  if (!isPort465 && !isPort587) {
    secure = Boolean(config.secure);
    requireTLS = !secure;
  }

  const resolvedHost = await resolveToIpv4(rawHost);

  const transportOptions = {
    host: resolvedHost,
    port: effectivePort,
    secure: secure,
    requireTLS: requireTLS,
    servername: rawHost, // SNI validation for TLS
    family: 4, // Enforce IPv4
    auth: {
      user: (config.user || '').trim(),
      pass: (config.pass || '').replace(/\s+/g, '').trim()
    },
    tls: {
      rejectUnauthorized: false,
      minVersion: 'TLSv1.2',
      servername: rawHost
    },
    connectionTimeout: 15000,
    greetingTimeout: 15000,
    socketTimeout: 20000
  };

  return {
    transporter: nodemailer.createTransport(transportOptions),
    meta: {
      rawHost,
      resolvedHost,
      port: effectivePort,
      secure,
      requireTLS
    }
  };
}

/**
 * Verify SMTP connection using transporter.verify()
 */
async function verifyConnection(config, overridePort = null) {
  if (!config.user || !config.pass) {
    return {
      success: false,
      code: 'SMTP_CONFIG_MISSING',
      message: 'Email Pengirim atau Google App Password belum diisi di Pengaturan Sistem.'
    };
  }

  try {
    const { transporter, meta } = await createTransporter(config, overridePort);
    console.log(`[SMTP] Verifying connection to ${meta.rawHost}:${meta.port} (secure: ${meta.secure}, requireTLS: ${meta.requireTLS})`);
    await transporter.verify();
    console.log(`[SMTP] Connection verified successfully on ${meta.rawHost}:${meta.port}`);
    return {
      success: true,
      code: 'SMTP_VERIFIED',
      message: 'SMTP berhasil terhubung.'
    };
  } catch (err) {
    const mapped = mapSmtpError(err);
    console.error(`[SMTP] Verify failed on port ${overridePort || config.port}:`, mapped.code, mapped.message);
    return {
      success: false,
      code: mapped.code,
      message: mapped.message
    };
  }
}

/**
 * Centralized sendEmail function with automatic fallback and strict error mapping
 */
async function sendEmail(mailOptions, config) {
  if (!config || !config.user || !config.pass) {
    console.warn('[SMTP] Delivery aborted: SMTP credentials not configured.');
    return {
      success: false,
      code: 'SMTP_NOT_CONFIGURED',
      error: 'Fitur email belum dikonfigurasi oleh Administrator.'
    };
  }

  const primaryPort = Number(config.port || 465);
  const rawHost = (config.host || 'smtp.gmail.com').trim();
  const recipientMasked = maskEmail(mailOptions.to);

  console.log(`[SMTP] Delivery attempt to ${recipientMasked} | Host: ${rawHost} | Port: ${primaryPort}`);

  try {
    const { transporter, meta } = await createTransporter(config, primaryPort);
    const info = await transporter.sendMail(mailOptions);
    console.log(`[SMTP] Email successfully delivered to ${recipientMasked} (ID: ${info.messageId})`);
    return {
      success: true,
      messageId: info.messageId
    };
  } catch (primaryErr) {
    const primaryMapped = mapSmtpError(primaryErr);
    console.warn(`[SMTP] Primary send failed on port ${primaryPort}: [${primaryMapped.code}] ${primaryMapped.message}`);

    // If timeout or connection refused on standard Gmail port (465 <-> 587), try graceful alternative port
    const isConnIssue = ['SMTP_CONNECTION_TIMEOUT', 'SMTP_CONNECTION_REFUSED', 'SMTP_NETWORK_UNREACHABLE', 'SMTP_STARTTLS_FAILED'].includes(primaryMapped.code);
    const isGmailHost = (rawHost === 'smtp.gmail.com');

    if (isConnIssue && isGmailHost && (primaryPort === 465 || primaryPort === 587)) {
      const fallbackPort = (primaryPort === 465) ? 587 : 465;
      console.log(`[SMTP] Retrying delivery via fallback port ${fallbackPort}...`);
      try {
        const { transporter: fallbackTransporter, meta: fallbackMeta } = await createTransporter(config, fallbackPort);
        const info = await fallbackTransporter.sendMail(mailOptions);
        console.log(`[SMTP] Email successfully delivered via fallback port ${fallbackPort} to ${recipientMasked}`);
        return {
          success: true,
          messageId: info.messageId,
          fallbackUsed: true
        };
      } catch (fallbackErr) {
        const fallbackMapped = mapSmtpError(fallbackErr);
        console.error(`[SMTP] Fallback delivery failed on port ${fallbackPort}: [${fallbackMapped.code}] ${fallbackMapped.message}`);
        return {
          success: false,
          code: fallbackMapped.code,
          error: fallbackMapped.message
        };
      }
    }

    return {
      success: false,
      code: primaryMapped.code,
      error: primaryMapped.message
    };
  }
}

/**
 * Send real test email for Admin SMTP Test Feature
 */
async function sendTestEmail(targetRecipient, config, appName = 'PutzPay') {
  if (!config.user || !config.pass) {
    return {
      success: false,
      code: 'SMTP_CONFIG_MISSING',
      message: 'Email Sender atau Google App Password belum diisi di Pengaturan Sistem.'
    };
  }

  // 1. Verify connection first
  const verifyRes = await verifyConnection(config);
  if (!verifyRes.success) {
    return {
      success: false,
      code: verifyRes.code,
      message: verifyRes.message
    };
  }

  // 2. Prepare test email
  const recipient = (targetRecipient && targetRecipient.includes('@')) 
    ? targetRecipient.trim() 
    : config.user;

  const mailOptions = {
    to: recipient,
    from: `"${appName}" <${config.user}>`,
    subject: `[TEST] Uji Koneksi SMTP Email - ${appName}`,
    text: `PutzPay SMTP Test\n\nSMTP ${appName} berhasil digunakan.\n\nJika Anda menerima email ini, konfigurasi SMTP pada ${appName} berhasil terhubung dan dapat mengirim email secara normal.\n\nWaktu pengujian: ${new Date().toLocaleString('id-ID')}\n\n© ${appName}`,
    html: `
      <div style="font-family: Arial, sans-serif; padding: 24px; background-color: #f8fafc;">
        <div style="max-width: 480px; margin: 0 auto; background: #ffffff; padding: 24px; border: 3px solid #000000; border-radius: 16px; box-shadow: 4px 4px 0px #000000;">
          <div style="background-color: #fef08a; padding: 12px; border: 2px solid #000000; border-radius: 8px; margin-bottom: 16px; text-align: center;">
            <h2 style="margin: 0; color: #000000; font-size: 18px; font-weight: 900; text-transform: uppercase;">
              ✓ SMTP Berhasil Terhubung
            </h2>
          </div>
          <p style="color: #334155; font-size: 14px; line-height: 1.6; margin-bottom: 16px;">
            Halo!<br><br>
            SMTP <strong>${appName}</strong> berhasil digunakan. Jika Anda menerima email ini, konfigurasi SMTP pada ${appName} berhasil terhubung dan dapat mengirim email dengan aman.
          </p>
          <div style="background: #f1f5f9; padding: 12px; border: 2px solid #000000; border-radius: 8px; font-size: 12px; color: #334155; font-family: monospace; margin: 16px 0;">
            <strong>Penerima:</strong> ${maskEmail(recipient)}<br>
            <strong>Waktu Uji:</strong> ${new Date().toLocaleString('id-ID')}<br>
            <strong>Status:</strong> Aktif & Terverifikasi
          </div>
          <p style="color: #64748b; font-size: 11px; margin: 0; text-align: center;">
            Email pengujian otomatis dikirim dari sistem ${appName}.
          </p>
        </div>
      </div>
    `
  };

  // 3. Send email and await real delivery
  const sendRes = await sendEmail(mailOptions, config);
  if (sendRes.success) {
    return {
      success: true,
      code: 'SMTP_SUCCESS',
      message: `✓ SMTP berhasil terhubung. Email percobaan berhasil dikirim ke ${maskEmail(recipient)}.`
    };
  } else {
    return {
      success: false,
      code: sendRes.code || 'SMTP_SEND_FAILED',
      message: sendRes.error || 'Gagal mengirim email uji coba.'
    };
  }
}

/**
 * Send 6-digit Verification OTP Email
 */
async function sendVerificationOtpEmail(user, otp, config, appName = 'PutzPay') {
  if (!config || !config.user || !config.pass) {
    console.warn('[OTP EMAIL] SMTP credentials not configured.');
    return {
      success: false,
      code: 'SMTP_CONFIG_MISSING',
      error: 'Fitur email belum dikonfigurasi oleh Administrator.'
    };
  }

  const mailOptions = {
    to: user.email,
    from: `"${appName}" <${config.user}>`,
    subject: `Kode OTP Verifikasi ${appName}`,
    text: `Halo ${user.username},\n\nGunakan kode OTP berikut untuk memverifikasi akun ${appName} Anda:\n\n${otp}\n\nKode OTP berlaku selama 5 menit.\n\nJika Anda tidak melakukan pendaftaran, abaikan email ini.\n\n${appName}`,
    html: `
      <div style="font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f3f4f8; padding: 40px 20px; margin: 0;">
        <div style="max-width: 480px; margin: 0 auto; background-color: #ffffff; border-radius: 24px; border: 3px solid #000000; overflow: hidden; box-shadow: 6px 6px 0px #000000;">
          <div style="background-color: #fde047; padding: 24px; text-align: center; border-bottom: 3px solid #000000;">
            <h1 style="margin: 0; color: #000000; font-size: 24px; font-weight: 900; letter-spacing: -0.5px; text-transform: uppercase;">
              ${appName}
            </h1>
            <p style="margin: 4px 0 0 0; color: #000000; font-size: 11px; font-weight: 700; text-transform: uppercase; font-family: monospace;">
              Verifikasi Email Akun
            </p>
          </div>
          <div style="padding: 32px 28px;">
            <p style="color: #000000; font-size: 14px; font-weight: 600; line-height: 1.6; margin: 0 0 16px 0;">
              Halo <strong>${user.username}</strong>,
            </p>
            <p style="color: #4b5563; font-size: 13px; line-height: 1.6; margin: 0 0 20px 0;">
              Gunakan kode OTP berikut untuk memverifikasi akun <strong>${appName}</strong> Anda:
            </p>
            
            <div style="text-align: center; margin: 24px 0; background-color: #fef08a; border: 3px solid #000000; border-radius: 16px; padding: 18px 10px; box-shadow: 4px 4px 0px #000000;">
              <span style="font-family: 'JetBrains Mono', 'Courier New', monospace; font-size: 34px; font-weight: 900; letter-spacing: 8px; color: #000000;">
                ${otp}
              </span>
            </div>

            <div style="background-color: #fee2e2; border: 2px solid #000000; border-radius: 12px; padding: 10px; text-align: center; margin-bottom: 20px;">
              <p style="color: #991b1b; font-size: 12px; font-weight: 700; margin: 0;">
                ⏱️ Kode OTP berlaku selama 5 menit.
              </p>
            </div>

            <p style="color: #6b7280; font-size: 12px; line-height: 1.5; margin: 0; text-align: center;">
              Jika Anda tidak melakukan pendaftaran, abaikan email ini secara aman.
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

  console.log(`[OTP] Sending verification email to ${maskEmail(user.email)}`);
  const result = await sendEmail(mailOptions, config);
  if (result.success) {
    console.log(`[OTP] Verification email sent successfully to ${maskEmail(user.email)}`);
  } else {
    console.error(`[OTP] Failed to send verification email to ${maskEmail(user.email)}:`, result.error);
  }
  return result;
}

module.exports = {
  maskEmail,
  resolveToIpv4,
  mapSmtpError,
  createTransporter,
  verifyConnection,
  sendEmail,
  sendTestEmail,
  sendVerificationOtpEmail
};
