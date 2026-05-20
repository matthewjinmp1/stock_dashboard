const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function makeElement() {
  return {
    textContent: '',
    innerHTML: '',
    value: '',
    dataset: {},
    style: { setProperty() {} },
    classList: { toggle() {}, add() {}, remove() {} },
    addEventListener() {},
    querySelector() { return makeElement(); },
    querySelectorAll() { return []; },
    closest() { return null; },
    insertAdjacentHTML() {},
  };
}

const body = makeElement();

const elements = new Map();
const documentStub = {
  body,
  addEventListener(event, callback) {
    if (event === 'DOMContentLoaded') callback();
  },
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, makeElement());
    return elements.get(id);
  },
  querySelector() {
    return makeElement();
  },
};

const storage = new Map();
const localStorageStub = {
  getItem(key) {
    return storage.has(key) ? storage.get(key) : null;
  },
  setItem(key, value) {
    storage.set(key, String(value));
  },
  removeItem(key) {
    storage.delete(key);
  },
};

const context = {
  console,
  document: documentStub,
  localStorage: localStorageStub,
  window: { scrollY: 0, scrollTo() {} },
  performance: { now: () => 0 },
  fetch: async () => ({ ok: true, json: async () => ({}) }),
};
context.global = context;
context.window.window = context.window;

const script = fs.readFileSync(path.join(__dirname, '..', 'public', 'script.js'), 'utf8');
vm.runInNewContext(script, context, { filename: 'public/script.js' });

const api = context.window.__stockAnalysisTestApi;
assert(api, 'test API should be exposed');

const data = {
  ticker: 'NEG',
  ev: '--',
  derivedEnterpriseValue: '19.7B',
  marketCap: '20B',
  revenue: '--',
  adj_income: '-100M',
  margin: '-10%',
  cy_growth: '5%',
  ny_growth: '5%',
  cy_revenue: '--',
  ny_revenue: '--',
  medianTaxRate: '20%',
  incomeStatement: {
    annual: {
      periods: ['TTM', '2025-12-31'],
      rows: [
        { label: 'Total Revenue', values: ['1B', '950M'] },
      ],
    },
  },
  metrics: {
    derivedEnterpriseValue: { raw: 19_700_000_000, display: '19.7B', kind: 'money' },
    marketCap: { raw: 20_000_000_000, display: '20B', kind: 'money' },
    revenue: { raw: null, display: '--', kind: 'money' },
    adj_income: { raw: -100_000_000, display: '-100M', kind: 'money' },
    margin: { raw: -0.10, display: '-10%', kind: 'percent' },
    cy_growth: { raw: 0.05, display: '5%', kind: 'percent' },
    ny_growth: { raw: 0.05, display: '5%', kind: 'percent' },
    cy_revenue: { raw: null, display: '--', kind: 'money' },
    ny_revenue: { raw: null, display: '--', kind: 'money' },
    medianTaxRate: { raw: 0.20, display: '20%', kind: 'percent' },
    ev_adj_ebit: { raw: null, display: '--', kind: 'ratio' },
    ev_cy_ebit: { raw: null, display: '--', kind: 'ratio' },
    ev_ny_ebit: { raw: null, display: '--', kind: 'ratio' },
  },
};

api.state.assumptions.NEG = { margin: 0.28 };
const adjusted = api.applyAssumptions(data);

assert.notStrictEqual(api.metricDisplay(adjusted, 'adj_income'), '--', 'edited margin should create adjusted income');
assert.notStrictEqual(api.metricDisplay(adjusted, 'ev_adj_ebit'), '--', 'edited positive margin should create EV/Adj Inc');
assert.notStrictEqual(api.metricDisplay(adjusted, 'ev_cy_ebit'), '--', 'edited positive margin should create EV/CY Adj Inc');
assert.notStrictEqual(api.metricDisplay(adjusted, 'ev_ny_ebit'), '--', 'edited positive margin should create EV/NY Adj Inc');

const calc = api.calcDefinitions(adjusted).ev_adj;
assert.notStrictEqual(calc.divisor, '--', 'calc page denominator should register');
assert.notStrictEqual(calc.result, '--', 'calc page result should register');

const blankStructuredData = {
  ...data,
  ticker: 'LEGACY',
  metrics: {
    ...data.metrics,
    revenue: { raw: null, display: '--', kind: 'money' },
    adj_income: { raw: null, display: '--', kind: 'money' },
    margin: { raw: null, display: '--', kind: 'percent' },
  },
};

api.state.assumptions.LEGACY = { margin: 0.28 };
const adjustedFromLegacy = api.applyAssumptions(blankStructuredData);

assert.notStrictEqual(api.metricDisplay(adjustedFromLegacy, 'adj_income'), '--', 'legacy fallback should create adjusted income');
assert.notStrictEqual(api.metricDisplay(adjustedFromLegacy, 'ev_adj_ebit'), '--', 'legacy fallback should create EV/Adj Inc');
assert.notStrictEqual(api.metricDisplay(adjustedFromLegacy, 'ev_cy_ebit'), '--', 'legacy fallback should create EV/CY Adj Inc');
assert.notStrictEqual(api.metricDisplay(adjustedFromLegacy, 'ev_ny_ebit'), '--', 'legacy fallback should create EV/NY Adj Inc');

console.log('frontend assumption tests passed');
