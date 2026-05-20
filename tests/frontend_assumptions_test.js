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

function assertAlmostEqual(actual, expected, epsilon, message) {
  assert(Math.abs(actual - expected) <= epsilon, `${message}: expected ${expected}, got ${actual}`);
}

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
  dataDate: '2025-12-31',
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
assertAlmostEqual(api.metricEntry(adjusted, 'adj_income').raw, 280_000_000, 1, 'edited margin should recalculate adjusted income from revenue');
assertAlmostEqual(api.metricEntry(adjusted, 'ev_adj_ebit').raw, 87.9464285714, 0.0001, 'EV/Adj Inc should use after-tax adjusted income');

const calc = api.calcDefinitions(adjusted).ev_adj;
assert.notStrictEqual(calc.divisor, '--', 'calc page denominator should register');
assert.notStrictEqual(calc.result, '--', 'calc page result should register');

const cyCalc = api.calcDefinitions(adjusted).ev_cy;
assert(cyCalc.rows.some(([label]) => label === '10% Discount Rate'), 'forward valuation calc should show discount rate');
assert(cyCalc.rows.some(([label, value]) => label === 'Discounted After-Tax CY Adj Op Inc' && value !== '--'), 'forward valuation calc should show discounted denominator');

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

const noValidTaxRateData = {
  ...data,
  ticker: 'TEAM',
  medianTaxRate: '--',
  metrics: {
    ...data.metrics,
    medianTaxRate: { raw: null, display: '--', kind: 'percent' },
  },
};

api.state.assumptions.TEAM = { margin: 0.28 };
const adjustedWithoutTaxRate = api.applyAssumptions(noValidTaxRateData);

assert.notStrictEqual(api.metricDisplay(adjustedWithoutTaxRate, 'ev_adj_ebit'), '--', 'positive margin should work when no valid tax rate exists');
assert.notStrictEqual(api.calcDefinitions(adjustedWithoutTaxRate).ev_adj.divisor, '--', 'calc denominator should work when no valid tax rate exists');

const editedTaxData = { ...data, ticker: 'TAX' };
api.state.assumptions.TAX = { margin: 0.28, medianTaxRate: 0.30 };
const adjustedTax = api.applyAssumptions(editedTaxData);

assert.strictEqual(api.metricDisplay(adjustedTax, 'medianTaxRate'), '30%', 'edited tax rate should display');
assertAlmostEqual(api.metricEntry(adjustedTax, 'ev_adj_ebit').raw, 100.5102040816, 0.0001, 'edited tax rate should flow into valuation multiple');

const editedGrowthData = { ...data, ticker: 'GROWTH' };
api.state.assumptions.GROWTH = { margin: 0.28, cy_growth: 0.20, ny_growth: 0.10 };
const adjustedGrowth = api.applyAssumptions(editedGrowthData);

assert.strictEqual(api.metricDisplay(adjustedGrowth, 'cy_growth'), '20%', 'edited CY growth should display');
assert.strictEqual(api.metricDisplay(adjustedGrowth, 'ny_growth'), '10%', 'edited NY growth should display');
assertAlmostEqual(api.metricEntry(adjustedGrowth, 'cy_revenue').raw, 1_140_000_000, 1, 'edited CY growth should use last annual revenue');
assertAlmostEqual(api.metricEntry(adjustedGrowth, 'ny_revenue').raw, 1_254_000_000, 1, 'edited NY growth should compound from CY revenue');
assert.notStrictEqual(api.metricDisplay(adjustedGrowth, 'ev_cy_ebit'), '--', 'edited growth should keep CY valuation active');
assert.notStrictEqual(api.metricDisplay(adjustedGrowth, 'ev_ny_ebit'), '--', 'edited growth should keep NY valuation active');

const bpsMetricHtml = api.metricValueHtml('0.72 bps');
assert(bpsMetricHtml.includes('value-display-with-unit'), 'basis point metric should render as attached unit markup');
assert(bpsMetricHtml.includes('<span>0.72</span><span class="value-unit">bps</span>'), 'basis point unit should stay attached to the number');

console.log('frontend assumption tests passed');
