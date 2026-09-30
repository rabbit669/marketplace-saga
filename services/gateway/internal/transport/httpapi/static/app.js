// Storefront for the marketplace API.
//
// Plain JavaScript on purpose: no framework, no bundler, no lockfile. The point
// of this page is to make the checkout saga visible, and a build step would make
// the demo the most complicated part of the repository.
//
// The token is held in a variable, never in localStorage. A token in
// localStorage is readable by any script that ends up on the page, and this page
// exists to be copied from and adapted — it should not model the mistake.

const API = '/v1';

let token = null;
let customer = null;
let products = [];
const cart = new Map(); // product id -> quantity

// ---------------------------------------------------------------- helpers

const $ = (id) => document.getElementById(id);

// money renders minor units. The API never sends a decimal, because a JSON
// number is a double to most clients and a double cannot hold 0.1 exactly.
function money(m) {
  if (!m || typeof m.amount_minor !== 'number') return '—';
  const whole = Math.trunc(m.amount_minor / 100);
  const cents = Math.abs(m.amount_minor % 100).toString().padStart(2, '0');
  return `${whole}.${cents} ${m.currency}`;
}

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s ?? '';
  return div.innerHTML;
}

// hueFor gives each product a stable colour.
//
// There are no product photographs, and a page of identical grey rectangles
// looks unfinished. A tint derived from the SKU is deterministic, so a product
// keeps its colour between reloads and nobody wonders whether it means
// something.
function hueFor(sku) {
  let h = 0;
  for (let i = 0; i < sku.length; i++) {
    h = (h * 31 + sku.charCodeAt(i)) % 360;
  }
  // Compressed into a range that stays legible against a dark background and
  // avoids the muddy yellows and greens in the middle.
  return [255, 275, 200, 320, 15][h % 5];
}

async function api(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (token) headers.Authorization = `Bearer ${token}`;

  const response = await fetch(API + path, { ...options, headers });
  const text = await response.text();
  const body = text ? JSON.parse(text) : null;

  if (!response.ok) {
    const err = new Error(body?.error?.message || `HTTP ${response.status}`);
    err.code = body?.error?.code;
    err.status = response.status;
    err.body = body;
    throw err;
  }
  return body;
}

// --------------------------------------------------------------- session

// The page signs itself in as a demo customer. A login form would be more
// faithful to a real shop and would put a step between the reader and the thing
// they came to see.
async function signIn() {
  const email = 'demo@example.com';
  const password = 'correct-horse-battery';

  try {
    const pair = await api('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
    token = pair.access_token;
    customer = pair.user;
  } catch {
    await api('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ email, password, full_name: 'Demo Customer' }),
    });
    const pair = await api('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
    token = pair.access_token;
    customer = pair.user;
  }

  $('who').textContent = `Вы вошли как ${customer.email}`;
}

// -------------------------------------------------------------- products

async function loadProducts() {
  const grid = $('products');
  try {
    const data = await api('/products');
    products = data.products || [];

    if (products.length === 0) {
      grid.innerHTML = '<p class="empty">Каталог пуст.</p>';
      return;
    }

    grid.innerHTML = '';
    for (const p of products) {
      grid.appendChild(productCard(p));
    }
  } catch (err) {
    grid.innerHTML = `<div class="error-box">Не удалось загрузить товары: ${escapeHtml(err.message)}</div>`;
  }
}

function productCard(p) {
  const card = document.createElement('article');
  card.className = 'card';

  const attrs = Object.entries(p.attributes || {})
    .map(([k, v]) => `${k}: ${v}`)
    .join(' · ');

  card.innerHTML = `
    <div class="thumb" style="--hue: ${hueFor(p.sku)}" aria-hidden="true">
      ${escapeHtml(p.name.trim().charAt(0).toUpperCase())}
    </div>
    <div class="card-body">
      <div class="sku">${escapeHtml(p.sku)}</div>
      <h4>${escapeHtml(p.name)}</h4>
      ${attrs ? `<div class="attrs">${escapeHtml(attrs)}</div>` : ''}
      <div class="card-foot">
        <span class="price">${money(p.price)}</span>
      </div>
    </div>
  `;

  const button = document.createElement('button');
  button.className = 'btn';
  button.textContent = 'В корзину';
  button.onclick = () => addToCart(p.id);
  card.querySelector('.card-foot').appendChild(button);

  return card;
}

// ------------------------------------------------------------------ cart

function addToCart(id) {
  cart.set(id, (cart.get(id) || 0) + 1);
  renderCart();

  // Bring the cart into view. Adding an item to something off-screen is the
  // most common way a demo like this seems to do nothing.
  $('cart-section').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderCart() {
  const section = $('cart-section');
  const rows = $('cart-rows');

  if (cart.size === 0) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  rows.innerHTML = '';

  let totalMinor = 0;
  let currency = 'USD';

  for (const [id, qty] of cart) {
    const p = products.find((x) => x.id === id);
    if (!p) continue;

    const lineTotal = p.price.amount_minor * qty;
    totalMinor += lineTotal;
    currency = p.price.currency;

    const li = document.createElement('li');
    li.innerHTML = `
      <div class="line-name">
        ${escapeHtml(p.name)}
        <span class="sku">${escapeHtml(p.sku)}</span>
      </div>
      <input class="qty" type="number" min="1" max="1000" value="${qty}"
             aria-label="Количество">
      <span class="money">${money({ amount_minor: lineTotal, currency })}</span>
      <button class="remove" aria-label="Убрать">✕</button>
    `;

    li.querySelector('input').onchange = (e) => {
      const v = Math.max(1, Math.min(1000, parseInt(e.target.value, 10) || 1));
      cart.set(id, v);
      renderCart();
    };
    li.querySelector('.remove').onclick = () => {
      cart.delete(id);
      renderCart();
    };

    rows.appendChild(li);
  }

  $('cart-total').textContent = money({ amount_minor: totalMinor, currency });
}

// -------------------------------------------------------------- checkout

async function checkout() {
  const button = $('pay-btn');
  button.disabled = true;
  button.textContent = 'Оформляю…';

  const paymentToken = document.querySelector('input[name="card"]:checked').value;
  const idempotencyKey = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

  const body = {
    lines: [...cart].map(([product_id, quantity]) => ({ product_id, quantity })),
    shipping_address: {
      country: $('ship-country').value,
      city: $('ship-city').value,
      street: $('ship-street').value,
      postal_code: $('ship-postal').value,
      recipient: $('ship-recipient').value,
    },
    payment_method_token: paymentToken,
  };

  try {
    const order = await api('/orders', {
      method: 'POST',
      headers: { 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(body),
    });
    await showResult(order, null);
  } catch (err) {
    // A rolled-back checkout arrives as 422 with the order in the body. That is
    // the interesting case, not an error to swallow.
    await showResult(err.body, err);
  } finally {
    button.disabled = false;
    button.textContent = 'Оплатить';
  }
}

async function showResult(order, err) {
  $('shop-section').hidden = true;
  $('cart-section').hidden = true;
  $('result-section').hidden = false;

  if (!order || !order.id) {
    $('result-title').textContent = 'Заказ не создан';
    $('result-summary').textContent = err ? err.message : 'Пустой ответ.';
    $('result-failure').hidden = true;
    $('saga-steps').innerHTML = '';
    return;
  }

  const rolledBack = order.status === 'cancelled';
  $('result-title').textContent = rolledBack ? 'Заказ отменён' : 'Заказ оформлен';

  $('result-summary').innerHTML = rolledBack
    ? `Списания не было и товар не зарезервирован. Заказ <b>${escapeHtml(order.id)}</b> на <b>${money(order.total)}</b>.`
    : `Заказ <b>${escapeHtml(order.id)}</b> на <b>${money(order.total)}</b> подтверждён.`;

  const failure = $('result-failure');
  if (order.failure) {
    failure.hidden = false;
    $('failure-code').textContent = order.failure.code;
    $('failure-message').textContent = order.failure.message;
    $('failure-step').textContent = order.failure.failed_step
      ? `Сломался шаг: ${order.failure.failed_step}` +
        (order.failure.retryable ? ' — ошибка временная, можно попробовать снова' : '')
      : '';
  } else {
    failure.hidden = true;
  }

  await renderSaga(order.id);
  $('result-section').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ------------------------------------------------------------- saga view

const STEP_NOTES = {
  reserve_stock: 'Отложить товар на складе',
  authorize_payment: 'Заморозить деньги на карте',
  capture_payment: 'Списать деньги',
  commit_stock: 'Списать товар со склада',
  confirm_order: 'Подтвердить заказ',
};

const STEP_COMPENSATION = {
  reserve_stock: 'резерв снят',
  authorize_payment: 'заморозка отменена',
};

// The pivot: the step after which nothing can be undone. Labelling it is the
// whole argument of the design, and it is invisible without a marker.
const PIVOT_STEP = 'capture_payment';

const STATUS_LABEL = {
  pending: '',
  completed: 'выполнен',
  failed: 'ошибка',
  compensated: 'отменён',
  compensation_failed: 'отменить не удалось',
};

const STATUS_MARK = {
  completed: '✓',
  compensated: '↺',
  failed: '✕',
  compensation_failed: '!',
  pending: '·',
};

async function renderSaga(orderId) {
  const list = $('saga-steps');
  list.innerHTML = '<li class="loading">Читаю состояние саги…</li>';

  let saga;
  try {
    saga = await api(`/orders/${orderId}/saga`);
  } catch (err) {
    list.innerHTML = `<li class="error-box">Не удалось прочитать сагу: ${escapeHtml(err.message)}</li>`;
    return;
  }

  list.innerHTML = '';

  for (const step of saga.steps || []) {
    const li = document.createElement('li');
    li.className = `step s-${step.status}`;

    const pivot = step.name === PIVOT_STEP
      ? '<span class="pivot">точка невозврата</span>'
      : '';

    let note = STEP_NOTES[step.name] || '';
    if (step.status === 'compensated' && STEP_COMPENSATION[step.name]) {
      note = STEP_COMPENSATION[step.name];
    }
    if (step.error && step.status !== 'pending') {
      // The last segment of the wrapped error is the readable one; the rest is
      // internal identifiers.
      note += `<br>${escapeHtml(step.error.split(': ').pop())}`;
    }

    const meta = [];
    if (step.attempts > 1) meta.push(`попыток: ${step.attempts}`);
    if (STATUS_LABEL[step.status]) meta.push(STATUS_LABEL[step.status]);

    li.innerHTML = `
      <div class="dot" aria-hidden="true">${STATUS_MARK[step.status] || '·'}</div>
      <div>
        <div class="step-name">${escapeHtml(step.name)}${pivot}</div>
        <div class="step-note">${note}</div>
      </div>
      <div class="step-meta">${meta.join('<br>')}</div>
    `;

    list.appendChild(li);
  }

  // A deep link into Jaeger, filtered by service. The natural next step for
  // someone who wants to see where the time went.
  $('jaeger-link').href = 'http://localhost:16686/search?service=order&limit=20';
}

// ------------------------------------------------------------------ boot

$('pay-btn').onclick = checkout;
$('again-btn').onclick = () => {
  cart.clear();
  renderCart();
  $('result-section').hidden = true;
  $('shop-section').hidden = false;
  window.scrollTo({ top: 0, behavior: 'smooth' });
};

(async function boot() {
  try {
    await signIn();
  } catch (err) {
    $('who').innerHTML = `<span class="error-box">Не удалось войти: ${escapeHtml(err.message)}</span>`;
    return;
  }
  await loadProducts();
})();
