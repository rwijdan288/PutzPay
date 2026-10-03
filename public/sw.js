// PutzPay Native Web Push Service Worker
self.addEventListener('push', function(event) {
  if (!event.data) return;

  try {
    const data = event.data.json();
    const title = data.title || '💰 PutzPay';
    const options = {
      body: data.body || 'Notifikasi dari PutzPay',
      icon: data.icon || '/public/profile/profile-6a16058f5ab6c5c4d875cf86-1780154370158.png',
      badge: data.badge || '/public/profile/profile-6a16058f5ab6c5c4d875cf86-1780154370158.png',
      tag: data.tag || 'putzpay-notification',
      renotify: true,
      data: data.data || {}
    };

    event.waitUntil(
      self.registration.showNotification(title, options)
    );
  } catch (err) {
    console.error('Service Worker push error:', err);
    const options = {
      body: event.data.text(),
      icon: '/public/profile/profile-6a16058f5ab6c5c4d875cf86-1780154370158.png'
    };
    event.waitUntil(
      self.registration.showNotification('💰 PutzPay', options)
    );
  }
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();

  const targetUrl = (event.notification.data && event.notification.data.url) ? event.notification.data.url : '/dashboard';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {
      for (let i = 0; i < clientList.length; i++) {
        const client = clientList[i];
        if (client.url && 'focus' in client) {
          if (client.url.includes(targetUrl) || targetUrl === '/dashboard') {
            return client.focus();
          }
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});
