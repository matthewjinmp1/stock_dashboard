const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function makeElement() {
  const classes = new Set();
  const attributes = {};
  return {
    textContent: '',
    innerHTML: '',
    value: '',
    dataset: {},
    style: { setProperty() {} },
    classList: {
      toggle(name, enabled) {
        if (enabled === undefined ? !classes.has(name) : enabled) classes.add(name);
        else classes.delete(name);
      },
      add(name) { classes.add(name); },
      remove(name) { classes.delete(name); },
      contains(name) { return classes.has(name); },
    },
    addEventListener() {},
    setAttribute(name, value) { attributes[name] = String(value); },
    getAttribute(name) { return attributes[name]; },
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

let now = 0;
const intervalCallbacks = [];
const context = {
  console,
  document: documentStub,
  localStorage: localStorageStub,
  window: { scrollY: 0, scrollTo() {} },
  performance: { now: () => now },
  fetch: async () => ({ ok: true, json: async () => ({}) }),
  requestAnimationFrame(callback) { callback(); },
  setInterval(callback) {
    intervalCallbacks.push(callback);
    return intervalCallbacks.length;
  },
  clearInterval() {},
};
context.global = context;
context.window.window = context.window;

const script = fs.readFileSync(path.join(__dirname, '..', 'public', 'script.js'), 'utf8');
vm.runInNewContext(script, context, { filename: 'public/script.js' });

const api = context.window.__stockAnalysisTestApi;
assert(api, 'test API should be exposed');

assert(api.accountLabelMatchesSearch('Operating Income', 'op'), 'search should match the prefix of the first word');
assert(api.accountLabelMatchesSearch('Operating Income', 'inc'), 'search should match the prefix of any word');
assert(api.accountLabelMatchesSearch('Net Non Operating Interest Income Expense', 'int inc'), 'multi-word search should match word prefixes across the label');
assert(!api.accountLabelMatchesSearch('Operating Income', 'come'), 'search should not match arbitrary substrings inside a word');

const quarterlyPeriods = ['2024-03-31', '2024-06-30', '2024-09-30', '2024-12-31', '2025-03-31'];
const quarterlyValues = ['100', '110', '120', '130', '150'];
api.state.periodicity = 'quarterly';
api.state.quarterlyGrowthMode = 'yoy';
assert.strictEqual(api.growthRowLabel(), 'YoY Growth', 'quarterly YoY mode should label growth rows');
assert.deepStrictEqual(api.growthValues(quarterlyValues, quarterlyPeriods), ['--', '--', '--', '--', '50.0%'], 'quarterly YoY growth should compare with the same quarter last year');
api.state.quarterlyGrowthMode = 'qoq';
assert.strictEqual(api.growthRowLabel(), 'QoQ Growth', 'quarterly QoQ mode should label growth rows');
assert.deepStrictEqual(api.growthValues(quarterlyValues, quarterlyPeriods), ['--', '10.0%', '9.1%', '8.3%', '15.4%'], 'quarterly QoQ growth should compare with the prior quarter');
api.state.periodicity = 'annual';

api.state.scanRequestId = 17;
api.startFetchTimer(0, 17);
const fetchInfoNode = elements.get('result-fetch-info');
assert.strictEqual(fetchInfoNode.textContent, 'Fetching: 0.00s • Fetches: --', 'fetch timer should render immediately while loading');
assert.strictEqual(fetchInfoNode.getAttribute('aria-busy'), 'true', 'fetch timer should be marked busy while loading');
now = 1234;
intervalCallbacks.at(-1)();
assert.strictEqual(fetchInfoNode.textContent, 'Fetching: 1.23s • Fetches: --', 'fetch timer should update while the request is still pending');
api.stopFetchTimer();
assert.strictEqual(fetchInfoNode.getAttribute('aria-busy'), 'false', 'fetch timer should clear busy state after loading');

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

const deLikeValuationData = {
  ...data,
  ticker: 'DE',
  ev: '197B',
  derivedEnterpriseValue: '197B',
  marketCap: '143B',
  valuationPrefix: 'EV',
  valuationNumeratorLabel: 'Derived Enterprise Value',
  metrics: {
    ...data.metrics,
    ev: { raw: 197_487_282_176, display: '197B', kind: 'money', currency: 'USD' },
    derivedEnterpriseValue: { raw: 197_487_282_176, display: '197B', kind: 'money', currency: 'USD' },
    marketCap: { raw: 142_927_282_176, display: '143B', kind: 'money', currency: 'USD' },
    adj_income: { raw: 8_070_000_000, display: '8.07B', kind: 'money', currency: 'USD' },
    cy_adj_inc: { raw: 7_281_402_194, display: '7.28B', kind: 'money', currency: 'USD' },
    ny_adj_inc: { raw: 7_900_000_000, display: '7.9B', kind: 'money', currency: 'USD' },
    ev_adj_ebit: { raw: 31.42, display: '31.4', kind: 'ratio' },
    ev_cy_ebit: { raw: 36.3, display: '36.3', kind: 'ratio' },
    ev_ny_ebit: { raw: 32.1, display: '32.1', kind: 'ratio' },
  },
};
const deCalc = api.calcDefinitions(deLikeValuationData).ev_cy;
assert.strictEqual(deCalc.numeratorLabel, 'Derived Enterprise Value', 'calc page should label the valuation numerator actually used');
assert.strictEqual(deCalc.numerator, '197B', 'calc page headline numerator should equal metrics.ev');
assert(deCalc.rows.some(([label, value]) => label === 'Derived Enterprise Value' && value === '197B'), 'calc breakdown should show the EV used in the formula');

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
assertAlmostEqual(api.metricEntry(adjustedGrowth, 'ev_cy_ebit').raw, 84.8550531653, 0.0001, 'CY valuation should use after-tax discounted forward income');
assertAlmostEqual(api.metricEntry(adjustedGrowth, 'ev_ny_ebit').raw, 84.8495177193, 0.0001, 'NY valuation should use after-tax discounted forward income');
assert.notStrictEqual(api.metricDisplay(adjustedGrowth, 'ev_cy_ebit'), '--', 'edited growth should keep CY valuation active');
assert.notStrictEqual(api.metricDisplay(adjustedGrowth, 'ev_ny_ebit'), '--', 'edited growth should keep NY valuation active');

const zeroInvestmentData = {
  ...data,
  ticker: 'ZERO_CAPEX',
  investmentCapex: '0',
  metrics: {
    ...data.metrics,
    investmentCapex: { raw: 0, display: '0', kind: 'money' },
  },
};

api.state.assumptions.ZERO_CAPEX = { margin: 0.28 };
const adjustedZeroInvestment = api.applyAssumptions(zeroInvestmentData);
assert.strictEqual(api.metricDisplay(adjustedZeroInvestment, 'capexAdjIncome'), '0%', 'zero investment capex should display as 0% investment rate');

const transactionCostData = {
  ...data,
  ticker: 'TXN_COST',
  metrics: {
    ...data.metrics,
    bidPrice: { raw: 100, display: '100', kind: 'money' },
    askPrice: { raw: 100.02, display: '100.02', kind: 'money' },
    bidAskSpread: { raw: 0.02, display: '0.02', kind: 'money' },
    transactionCost: { raw: 0.0001, display: '1 bps', kind: 'basisPoints' },
  },
};
const transactionCostCalc = api.calcDefinitions(transactionCostData).transaction_cost;
assert.strictEqual(transactionCostCalc.result, '1 bps', 'transaction cost calc should display basis points');
assert(transactionCostCalc.rows.some(([label, value]) => label === 'Half Spread' && value !== '--'), 'transaction cost calc should show half spread');
assert(transactionCostCalc.rows.some(([label, value]) => label === 'Midpoint' && value !== '--'), 'transaction cost calc should show midpoint');

const estimatedMarginData = {
  ...data,
  ticker: 'EST_MARGIN',
  metrics: {
    ...data.metrics,
    currentYearEstimatedNetMargin: { raw: 0.25, display: '25%', kind: 'percent' },
    nextYearEstimatedNetMargin: { raw: 0.2916666667, display: '29.2%', kind: 'percent' },
  },
};
assert.strictEqual(api.metricDisplay(estimatedMarginData, 'currentYearEstimatedNetMargin'), '25%', 'CY estimated net margin should display from structured metrics');
assert.strictEqual(api.metricDisplay(estimatedMarginData, 'nextYearEstimatedNetMargin'), '29.2%', 'NY estimated net margin should display from structured metrics');

const bpsMetricHtml = api.metricValueHtml('0.72 bps');
assert(bpsMetricHtml.includes('value-display-with-unit'), 'basis point metric should render as attached unit markup');
assert(bpsMetricHtml.includes('<span>0.72</span><span class="value-unit">bps</span>'), 'basis point unit should stay attached to the number');

console.log('frontend assumption tests passed');
