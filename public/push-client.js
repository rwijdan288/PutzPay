// PutzPay Native Web Push Client Utility

async function parseApiResponse(response) {
  const contentType = response.headers.get("content-type") || "";
  const text = await response.text();

  if (!contentType.includes("application/json")) {
    console.error("Non-JSON API response:", {
      status: response.status,
      url: response.url,
      contentType,
      body: text.slice(0, 500)
    });

    throw new Error(
      `Server mengembalikan response bukan JSON (${response.status})`
    );
  }

  try {
    return JSON.parse(text);
  } catch (error) {
    console.error("Invalid JSON response:", text.slice(0, 500));
    throw new Error("Response server tidak valid.");
  }
}

function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding)
    .replace(/-/g, '+')
    .replace(/_/g, '/');

  const rawData = window.atob(base64);
  const outputArray = new Uint8Array(rawData.length);

  for (let i = 0; i < rawData.length; ++i) {
    outputArray[i] = rawData.charCodeAt(i);
  }
  return outputArray;
}

const PutzPush = {
  initialized: false,
  swRegistration: null,
  isSubscribed: false,

  // FLOW A: CHECK STATUS (Page Load) - Never hangs, lightweight, no auto-register
  async init() {
    console.log('[PUTZPAY PUSH] =========================');
    console.log('[PUTZPAY PUSH] CHECK FUNCTION STARTED');
    console.log('[PUTZPAY PUSH] =========================');

    try {
      // 1. Check secure context
      if (window.isSecureContext === false && window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
        console.warn('[PUTZPAY PUSH] Web Push requires HTTPS context.');
        this.updateUIState('error', '⚠️ WEB PUSH MEMBUTUHKAN HTTPS');
        this.initialized = true;
        return false;
      }

      // 2. Check Notification API
      if (!('Notification' in window)) {
        console.warn('[PUTZPAY PUSH] Notification API not supported.');
        this.updateUIState('unsupported', '❌ NOTIFICATION API TIDAK DIDUKUNG');
        this.initialized = true;
        return false;
      }

      // 3. Check Service Worker API
      if (!('serviceWorker' in navigator)) {
        console.warn('[PUTZPAY PUSH] Service Worker not supported.');
        this.updateUIState('unsupported', '❌ SERVICE WORKER TIDAK DIDUKUNG');
        this.initialized = true;
        return false;
      }

      // 4. Check PushManager API
      if (!('PushManager' in window)) {
        console.warn('[PUTZPAY PUSH] Push API not supported.');
        this.updateUIState('unsupported', '❌ PUSH API TIDAK DIDUKUNG');
        this.initialized = true;
        return false;
      }

      console.log('[PUTZPAY PUSH] Permission state:', Notification.permission);

      if (Notification.permission === 'denied') {
        this.updateUIState('denied', '🚫 NOTIFIKASI DIBLOKIR');
        this.initialized = true;
        return false;
      }

      console.log('[PUTZPAY PUSH] Getting Service Worker registration...');
      
      // Timeout guard for getRegistration (3 seconds max)
      const regPromise = navigator.serviceWorker.getRegistration();
      const timeoutPromise = new Promise(resolve => setTimeout(() => resolve(null), 3000));
      const registration = await Promise.race([regPromise, timeoutPromise]);

      console.log('[PUTZPAY PUSH] Service Worker Registration found:', !!registration);

      if (!registration) {
        this.updateUIState('unsubscribed', '⚪ NOTIFIKASI BELUM AKTIF');
        this.initialized = true;
        return false;
      }

      this.swRegistration = registration;

      console.log('[PUTZPAY PUSH] Getting Push Subscription...');
      
      // Timeout guard for getSubscription (3 seconds max)
      const subPromise = registration.pushManager.getSubscription();
      const subTimeoutPromise = new Promise(resolve => setTimeout(() => resolve(null), 3000));
      const subscription = await Promise.race([subPromise, subTimeoutPromise]);

      console.log('[PUTZPAY PUSH] Push Subscription found:', !!subscription);

      if (subscription) {
        this.isSubscribed = true;
        this.updateUIState('subscribed', '🟢 NOTIFIKASI AKTIF');
        this.loadSettings().catch(e => console.warn('[PUTZPAY PUSH] loadSettings error:', e));
      } else {
        this.isSubscribed = false;
        this.updateUIState('unsubscribed', '⚪ NOTIFIKASI BELUM AKTIF');
      }

      this.initialized = true;
      return true;
    } catch (error) {
      console.error('[PUTZPAY PUSH] CHECK ERROR:', error);
      if (Notification.permission === 'denied') {
        this.updateUIState('denied', '🚫 NOTIFIKASI DIBLOKIR');
      } else {
        this.updateUIState('error', '⚠️ GAGAL MEMERIKSA');
      }
      this.initialized = true;
      return false;
    } finally {
      console.log('[PUTZPAY PUSH] CHECK FUNCTION FINISHED');
    }
  },

  // FLOW B: SUBSCRIBE / ACTIVATE (User Clicks "Aktifkan Notifikasi")
  async subscribe() {
    console.log('[PUTZPAY PUSH] =========================');
    console.log('[PUTZPAY PUSH] SUBSCRIBE STARTED (User clicked button)');
    console.log('[PUTZPAY PUSH] =========================');

    const enableBtn = document.getElementById('pushNotifEnableBtn');
    if (enableBtn) {
      enableBtn.disabled = true;
      const enableSpan = enableBtn.querySelector('span');
      if (enableSpan) enableSpan.textContent = '⏳ MENGAKTIFKAN...';
    }

    try {
      if (window.isSecureContext === false && window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
        throw new Error('Notifikasi memerlukan koneksi HTTPS');
      }

      if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) {
        throw new Error('Browser/perangkat ini tidak mendukung Push Notification');
      }

      console.log('[PUTZPAY PUSH] Step 1: Requesting Notification permission...');
      const permission = await Notification.requestPermission();
      console.log('[PUTZPAY PUSH] Permission result:', permission);

      if (permission === 'denied') {
        this.updateUIState('denied', '🚫 NOTIFIKASI DIBLOKIR');
        throw new Error('Izin notifikasi diblokir oleh browser. Silakan ubah izin situs di pengaturan browser kamu.');
      }

      if (permission !== 'granted') {
        this.updateUIState('unsubscribed', '⚪ NOTIFIKASI BELUM AKTIF');
        throw new Error('Izin notifikasi belum diberikan');
      }

      console.log('[PUTZPAY PUSH] Step 2: Registering Service Worker /service-worker.js...');
      const swPromise = navigator.serviceWorker.register('/service-worker.js', { scope: '/' });
      const timeoutPromise = new Promise((_, reject) => setTimeout(() => reject(new Error('Pendaftaran Service Worker timeout (10s)')), 10000));
      this.swRegistration = await Promise.race([swPromise, timeoutPromise]);
      console.log('[PUTZPAY PUSH] Service Worker registered:', this.swRegistration);

      console.log('[PUTZPAY PUSH] Step 3: Fetching VAPID public key...');
      const response = await fetch('/api/notifications/vapid-key');
      const data = await parseApiResponse(response);
      if (!data.success || !data.publicKey) {
        throw new Error(data.message || 'VAPID public key tidak tersedia');
      }
      console.log('[PUTZPAY PUSH] VAPID public key received');

      console.log('[PUTZPAY PUSH] Step 4: Creating Push Subscription...');
      const applicationServerKey = urlBase64ToUint8Array(data.publicKey);

      let subscription = await this.swRegistration.pushManager.getSubscription();
      if (!subscription) {
        subscription = await this.swRegistration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey
        });
      }
      console.log('[PUTZPAY PUSH] Push Subscription created successfully');

      console.log('[PUTZPAY PUSH] Step 5: Sending subscription to server...');
      const subResponse = await fetch('/api/notifications/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          subscription: subscription.toJSON ? subscription.toJSON() : subscription,
          userAgent: navigator.userAgent
        })
      });

      const subResult = await parseApiResponse(subResponse);
      if (subResult.success) {
        this.isSubscribed = true;
        this.updateUIState('subscribed', '🟢 NOTIFIKASI AKTIF');
        console.log('[PUTZPAY PUSH] Web Push activated successfully!');
        if (typeof showToast === 'function') {
          showToast('🟢 Notifikasi berhasil diaktifkan!', 'success');
        } else {
          alert('🟢 Notifikasi berhasil diaktifkan pada perangkat ini!');
        }
      } else {
        throw new Error(subResult.message || 'Gagal menyimpan subscription di server');
      }
    } catch (err) {
      console.error('[PUTZPAY PUSH] SUBSCRIBE ERROR:', err);
      if (Notification.permission === 'denied') {
        this.updateUIState('denied', '🚫 NOTIFIKASI DIBLOKIR');
      } else {
        this.updateUIState('error', '⚠️ NOTIFIKASI GAGAL DIAKTIFKAN');
      }
      const userFriendlyMsg = '⚠️ NOTIFIKASI GAGAL DIAKTIFKAN\n\n' + (err.message && !err.message.includes('JSON') ? err.message : 'Server PutzPay tidak memberikan response API yang valid. Silakan coba lagi.');
      if (typeof showToast === 'function') {
        showToast(userFriendlyMsg, 'error');
      } else {
        alert(userFriendlyMsg);
      }
    } finally {
      if (enableBtn) {
        enableBtn.disabled = false;
        const enableSpan = enableBtn.querySelector('span');
        if (enableSpan) enableSpan.textContent = 'Aktifkan Notifikasi';
      }
      console.log('[PUTZPAY PUSH] SUBSCRIBE FINISHED');
    }
  },

  async unsubscribe() {
    console.log('[PUTZPAY PUSH] UNSUBSCRIBE STARTED');
    try {
      if (!this.swRegistration) {
        const regPromise = navigator.serviceWorker.getRegistration();
        const timeoutPromise = new Promise(resolve => setTimeout(() => resolve(null), 3000));
        this.swRegistration = await Promise.race([regPromise, timeoutPromise]);
      }
      if (this.swRegistration) {
        const subscription = await this.swRegistration.pushManager.getSubscription();
        if (subscription) {
          const endpoint = subscription.endpoint;
          await subscription.unsubscribe();
          const unSubRes = await fetch('/api/notifications/unsubscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ endpoint })
          });
          await parseApiResponse(unSubRes);
        }
      }
      this.isSubscribed = false;
      this.updateUIState('unsubscribed', '⚪ NOTIFIKASI BELUM AKTIF');
      console.log('[PUTZPAY PUSH] Notifications disabled.');
      if (typeof showToast === 'function') {
        showToast('Notifikasi telah dinonaktifkan', 'info');
      } else {
        alert('Notifikasi telah dinonaktifkan.');
      }
    } catch (err) {
      console.error('[PUTZPAY PUSH] UNSUBSCRIBE ERROR:', err);
      if (typeof showToast === 'function') {
        showToast('Gagal mematikan notifikasi: ' + err.message, 'error');
      } else {
        alert('Gagal mematikan notifikasi: ' + err.message);
      }
    }
  },

  async sendTestNotification() {
    console.log('[PUTZPAY PUSH] TEST NOTIFICATION REQUESTED');
    try {
      if (!this.isSubscribed) {
        if (typeof showToast === 'function') {
          showToast('Aktifkan notifikasi terlebih dahulu pada perangkat ini', 'error');
        } else {
          alert('Aktifkan notifikasi terlebih dahulu pada perangkat ini.');
        }
        return;
      }
      const response = await fetch('/api/notifications/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      const data = await parseApiResponse(response);
      if (data.success) {
        console.log('[PUTZPAY PUSH] Test notification request sent successfully.');
        if (typeof showToast === 'function') {
          showToast('📱 Test notifikasi telah dikirim ke perangkat kamu!', 'success');
        } else {
          alert('📱 Test notifikasi dikirim! Periksa panel notifikasi perangkat kamu.');
        }
      } else {
        throw new Error(data.message || 'Unknown error');
      }
    } catch (err) {
      console.error('[PUTZPAY PUSH] Test notification error:', err);
      if (typeof showToast === 'function') {
        showToast('❌ Gagal mengirim test notifikasi: ' + err.message, 'error');
      } else {
        alert('Gagal mengirim test notifikasi: ' + err.message);
      }
    }
  },

  async loadSettings() {
    try {
      const res = await fetch('/api/notifications/settings');
      const data = await parseApiResponse(res);
      if (data.success && data.settings) {
        const s = data.settings;
        const keys = ['paymentSuccess', 'paymentPending', 'paymentFailed', 'paymentExpired', 'withdrawSuccess', 'withdrawFailed', 'balanceUpdated', 'securityAlert'];
        keys.forEach(k => {
          const el = document.getElementById(`notif_pref_${k}`);
          if (el) el.checked = !!s[k];
        });
      }
    } catch (e) {
      console.warn('[PUTZPAY PUSH] Failed to load notification settings:', e);
    }
  },

  async saveSettings() {
    try {
      const keys = ['paymentSuccess', 'paymentPending', 'paymentFailed', 'paymentExpired', 'withdrawSuccess', 'withdrawFailed', 'balanceUpdated', 'securityAlert'];
      const payload = {};
      keys.forEach(k => {
        const el = document.getElementById(`notif_pref_${k}`);
        if (el) payload[k] = el.checked;
      });

      const res = await fetch('/api/notifications/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await parseApiResponse(res);
      if (data.success) {
        if (typeof showToast === 'function') {
          showToast('✅ Pengaturan notifikasi berhasil disimpan', 'success');
        } else {
          alert('Pengaturan notifikasi berhasil disimpan.');
        }
      }
    } catch (e) {
      console.error('[PUTZPAY PUSH] Failed to save settings:', e);
      if (typeof showToast === 'function') {
        showToast('Gagal menyimpan preferensi: ' + e.message, 'error');
      } else {
        alert('Gagal menyimpan preferensi: ' + e.message);
      }
    }
  },

  updateUIState(status, message) {
    const statusTextEl = document.getElementById('pushNotifStatusText');
    const badgeEl = document.getElementById('profilePushBadge');
    const enableBtn = document.getElementById('pushNotifEnableBtn');
    const disableBtn = document.getElementById('pushNotifDisableBtn');
    const testBtn = document.getElementById('pushNotifTestBtn');

    if (statusTextEl) statusTextEl.textContent = message;

    if (badgeEl) {
      if (status === 'subscribed') {
        badgeEl.className = 'text-[11px] font-mono font-bold bg-emerald-200 border-2 border-black px-2 py-0.5 rounded-lg text-black';
      } else if (status === 'denied') {
        badgeEl.className = 'text-[11px] font-mono font-bold bg-rose-200 border-2 border-black px-2 py-0.5 rounded-lg text-black';
      } else if (status === 'unsupported' || status === 'error') {
        badgeEl.className = 'text-[11px] font-mono font-bold bg-amber-200 border-2 border-black px-2 py-0.5 rounded-lg text-black';
      } else {
        badgeEl.className = 'text-[11px] font-mono font-bold bg-yellow-200 border-2 border-black px-2 py-0.5 rounded-lg text-black';
      }
    }

    if (status === 'subscribed') {
      if (enableBtn) enableBtn.classList.add('hidden');
      if (disableBtn) disableBtn.classList.remove('hidden');
      if (testBtn) testBtn.classList.remove('hidden');
    } else {
      if (enableBtn) enableBtn.classList.remove('hidden');
      if (disableBtn) disableBtn.classList.add('hidden');
      if (testBtn) testBtn.classList.add('hidden');
    }
  }
};

function autoInitPutzPush() {
  PutzPush.init();
}

if (document.readyState === 'complete' || document.readyState === 'interactive') {
  setTimeout(autoInitPutzPush, 10);
} else {
  document.addEventListener('DOMContentLoaded', autoInitPutzPush);
}
