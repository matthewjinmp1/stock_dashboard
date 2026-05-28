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
assert.deepStrictEqual(api.growthValues(['-7.62B', '-7.95B', '-8.36B', '-8.78B', '-11B'], []), ['--', '4.3%', '5.2%', '5.0%', '25.3%'], 'annual growth should compare magnitude for rows that stay negative');
assert.deepStrictEqual(api.growthValues(['-100M', '50M'], []), ['--', '--'], 'annual growth should not show misleading percentages when signs flip');
api.state.latest = {
  incomeStatement: {
    quarterly: { periods: ['2026-06-30', '2026-03-31'] },
  },
};
assert.deepStrictEqual(
  api.growthValues(['100', '120', '132'], ['2024-12-31', '2025-12-31', 'TTM'], 'income'),
  ['--', '20.0%', '21.2%'],
  'annual TTM growth should annualize over the fraction of a year between last fiscal year end and the TTM end date',
);
api.state.latest = null;

api.state.loadedTicker = 'AAPL';
api.state.periodicity = 'quarterly';
api.state.statementTab = 'income';
api.state.quarterlyGrowthMode = 'qoq';
api.state.statementSearch = 'rev';
api.resetStatementDefaultsForTicker({ ticker: 'MSFT' });
assert.strictEqual(api.state.loadedTicker, 'MSFT', 'new tickers should become the loaded statement ticker');
assert.strictEqual(api.state.periodicity, 'annual', 'new tickers should default statements to annual');
assert.strictEqual(api.state.statementTab, 'starred', 'new tickers should default statements to starred');
assert.strictEqual(api.state.quarterlyGrowthMode, 'yoy', 'new tickers should reset quarterly growth mode');
assert.strictEqual(api.state.statementSearch, '', 'new tickers should clear statement search');

api.state.periodicity = 'quarterly';
api.state.statementTab = 'cash';
api.state.quarterlyGrowthMode = 'qoq';
api.state.statementSearch = 'cash';
api.resetStatementDefaultsForTicker({ ticker: 'MSFT' });
assert.strictEqual(api.state.periodicity, 'quarterly', 'same-ticker refreshes should keep statement periodicity');
assert.strictEqual(api.state.statementTab, 'cash', 'same-ticker refreshes should keep statement tab');
assert.strictEqual(api.state.quarterlyGrowthMode, 'qoq', 'same-ticker refreshes should keep quarterly growth mode');
assert.strictEqual(api.state.statementSearch, 'cash', 'same-ticker refreshes should keep active statement search');

const ratiosData = {
  incomeStatement: {
    annual: {
      periods: ['TTM', '2025-12-31', '2024-12-31'],
      rows: [
        { label: 'Net Income', values: ['12B', '10B', '8B'] },
        { label: 'Operating Income', values: ['20B', '18B', '16B'] },
        { label: 'Adjusted Net Income', values: ['18B', '15B', '14B'] },
        { label: 'Tax Rate', values: ['20%', '25%', '25%'] },
      ],
    },
    quarterly: {
      periods: ['2025-03-31', '2025-06-30'],
      rows: [
        { label: 'Net Income Common Stockholders', values: ['1B', '1.5B'] },
        { label: 'Operating Income', values: ['2B', '2.4B'] },
        { label: 'Adjusted Net Income', values: ['1.8B', '2B'] },
        { label: 'Tax Rate', values: ['20%', '25%'] },
      ],
    },
  },
  balanceStatement: {
    annual: {
      periods: ['MRQ', '2025-12-31', '2024-12-31'],
      rows: [
        { label: 'Total Assets', values: ['120B', '100B', '80B'] },
        { label: 'Total Debt', values: ['30B', '25B', '20B'] },
        { label: 'Stockholders Equity', values: ['70B', '65B', '60B'] },
        { label: 'Cash, Equivalents & Short Term Investments', values: ['20B', '15B', '10B'] },
      ],
    },
    quarterly: {
      periods: ['2025-03-31', '2025-06-30'],
      rows: [
        { label: 'Total Assets', values: ['50B', '60B'] },
        { label: 'Total Debt', values: ['10B', '12B'] },
        { label: 'Stockholders Equity', values: ['40B', '44B'] },
        { label: 'Cash, Equivalents & Short Term Investments', values: ['5B', '6B'] },
      ],
    },
  },
};
api.state.periodicity = 'annual';
const annualRatios = api.buildRatiosStatement(ratiosData);
assert.deepStrictEqual(Array.from(annualRatios.periods), ['TTM', '2025-12-31', '2024-12-31'], 'annual ratios should use income statement periods');
assert.strictEqual(annualRatios.rows[0].label, 'ROA', 'annual ratios should include ROA');
assert.deepStrictEqual(Array.from(annualRatios.rows[0].values), ['10%', '10%', '10%'], 'annual ratios should calculate ROA from net income over total assets, with TTM paired to MRQ assets');
assert.strictEqual(annualRatios.rows[1].label, 'ROIC', 'annual ratios should include ROIC');
assert.deepStrictEqual(Array.from(annualRatios.rows[1].values), ['20%', '18%', '17.1%'], 'annual ROIC should use after-tax operating income over debt plus equity minus cash');
assert.strictEqual(annualRatios.rows[2].label, 'Adj ROIC', 'annual ratios should include adjusted ROIC');
assert.deepStrictEqual(Array.from(annualRatios.rows[2].values), ['22.5%', '20%', '20%'], 'annual Adj ROIC should use adjusted net income over debt plus equity minus cash');
api.state.periodicity = 'quarterly';
const quarterlyRatios = api.buildRatiosStatement(ratiosData);
assert.deepStrictEqual(Array.from(quarterlyRatios.periods), ['2025-03-31', '2025-06-30'], 'quarterly ratios should use income statement periods');
assert.strictEqual(quarterlyRatios.rows[0].label, 'ROA', 'quarterly ratios should include ROA');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[0].values), ['2%', '2.5%'], 'quarterly ratios should calculate ROA from same-period quarterly net income and assets');
assert.strictEqual(quarterlyRatios.rows[1].label, 'ROIC', 'quarterly ratios should include ROIC');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[1].values), ['3.6%', '3.6%'], 'quarterly ROIC should use same-period after-tax operating income and invested capital');
assert.strictEqual(quarterlyRatios.rows[2].label, 'Adj ROIC', 'quarterly ratios should include adjusted ROIC');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[2].values), ['4%', '4%'], 'quarterly Adj ROIC should use same-period adjusted net income and invested capital');
api.state.periodicity = 'annual';

const balanceForMargin = {
  periods: ['2025-12-31', '2024-12-31'],
  rows: [
    { label: 'Total Assets', values: ['100B', '80B'] },
    { label: 'Cash And Cash Equivalents', values: ['25B', '16B'] },
  ],
};
assert.deepStrictEqual(
  api.marginValues(balanceForMargin.rows[1], balanceForMargin.periods, balanceForMargin, 'balance'),
  ['25.0%', '20.0%'],
  'balance sheet margin rows should use total assets as the denominator',
);

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
    grossPpe: { raw: 1_000_000_000, display: '1B', kind: 'money' },
    netWorkingCapital: { raw: 200_000_000, display: '200M', kind: 'money' },
    netFixedAssets: { raw: 800_000_000, display: '800M', kind: 'money' },
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
assertAlmostEqual(api.metricEntry(adjusted, 'afterTaxAdjIncome').raw, 224_000_000, 1, 'edited margin should compute after-tax adjusted income');
assertAlmostEqual(api.metricEntry(adjusted, 'adjEbitGrossPpe').raw, 0.224, 0.0001, 'ROGPPE should use after-tax adjusted income');
assertAlmostEqual(api.metricEntry(adjusted, 'roc').raw, 0.224, 0.0001, 'ROC should use after-tax adjusted income');
assert.strictEqual(api.metricDisplay(adjusted, 'netDebtAdjIncome'), '0', 'net cash or no debt should show zero leverage');

const adjustedWithDebt = api.applyAssumptions({
  ...data,
  metrics: {
    ...data.metrics,
    netCash: { raw: -2_240_000_000, display: '-2.24B', kind: 'money' },
  },
});
assertAlmostEqual(api.metricEntry(adjustedWithDebt, 'netDebtAdjIncome').raw, 10, 0.0001, 'net debt to adjusted income should use positive net debt over adjusted net income');
assert.strictEqual(api.metricDisplay(adjustedWithDebt, 'netDebtAdjIncome'), '10', 'net debt to adjusted income should display as a ratio');

delete api.state.assumptions.NEG;
const negativeAdjustedIncomeWithDebt = api.applyAssumptions({
  ...data,
  metrics: {
    ...data.metrics,
    netCash: { raw: -100_000_000, display: '-100M', kind: 'money' },
  },
});
assert.strictEqual(api.metricDisplay(negativeAdjustedIncomeWithDebt, 'netDebtAdjIncome'), '∞', 'positive net debt with negative adjusted net income should show infinity');
api.state.assumptions.NEG = { margin: 0.28 };

const invalidTaxData = {
  ...data,
  metrics: {
    ...data.metrics,
    medianTaxRate: { raw: 0.55, display: '55%', kind: 'percent' },
  },
};
const sanitizedTax = api.applyAssumptions(invalidTaxData);
assert.strictEqual(api.metricDisplay(sanitizedTax, 'medianTaxRate'), '20%', 'out-of-range tax rates should display as the default 20% rate');
assertAlmostEqual(api.metricEntry(sanitizedTax, 'ev_adj_ebit').raw, 87.9464285714, 0.0001, 'out-of-range tax rates should use the default 20% rate in calculations');

const calc = api.calcDefinitions(adjusted).ev_adj;
assert.notStrictEqual(calc.divisor, '--', 'calc page denominator should register');
assert.notStrictEqual(calc.result, '--', 'calc page result should register');
const rogppeCalc = api.calcDefinitions(adjusted).adj_ebit_gross_ppe;
assert.strictEqual(rogppeCalc.numerator, '224M', 'ROGPPE calc should show after-tax adjusted income');
assert(rogppeCalc.rows.some(([label, value]) => label === 'Adjusted Net Income' && value === '224M'), 'ROGPPE calc should include adjusted net income bridge');
const rocCalc = api.calcDefinitions(adjusted).roc;
assert.strictEqual(rocCalc.numerator, '224M', 'ROC calc should show after-tax adjusted income');

const netDebtData = {
  ...data,
  metrics: {
    ...data.metrics,
    netCash: { raw: -2_300_000_000, display: '-2.3B', kind: 'money' },
  },
  balanceStatement: {
    annual: {
      periods: ['MRQ'],
      rows: [
        { label: 'Cash, Equivalents & Short Term Investments', values: ['1B'] },
      ],
    },
  },
};
const netDebtPresentation = api.netCashPresentation(netDebtData);
assert.strictEqual(netDebtPresentation.label, 'Net Debt', 'negative net cash should relabel as net debt');
assert.strictEqual(netDebtPresentation.display, '2.3B', 'negative net cash should display as a positive net debt amount');
assert.strictEqual(netDebtPresentation.formula, 'Total Debt - Cash & Short Term Investments', 'negative net cash should use the net debt formula');
const netDebtCalc = api.calcDefinitions(netDebtData).net_cash;
assert.strictEqual(netDebtCalc.title, 'Net Debt', 'net cash calc should relabel negative values as net debt');
assert.strictEqual(netDebtCalc.result, '2.3B', 'net cash calc should show net debt as a positive amount');
assert(netDebtCalc.rows.some(([label, value]) => label === 'Net Debt' && value === '2.3B'), 'net debt calc should include the positive net debt result');
const leverageCalc = api.calcDefinitions(adjustedWithDebt).net_debt_adj_income;
assert.strictEqual(leverageCalc.numerator, '2.2B', 'leverage calc should use positive net debt');
assert.strictEqual(leverageCalc.divisor, '224M', 'leverage calc should use adjusted net income');
assert.strictEqual(leverageCalc.result, '10', 'leverage calc should show net debt over adjusted net income');

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
assert(cyCalc.rows.some(([label, value]) => label === 'Discounted CY Adjusted Net Income' && value !== '--'), 'forward valuation calc should show discounted denominator');

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
api.state.latest = editedTaxData;
api.resetAssumption('medianTaxRate');
assert.strictEqual(api.state.assumptions.TAX.margin, 0.28, 'reset should keep other edited assumptions');
assert.strictEqual(api.state.assumptions.TAX.medianTaxRate, undefined, 'reset should clear the selected edited assumption');

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

const dataromaHtml = api.renderDataromaCards({
  ticker: 'META',
  dataroma: {
    sourceUrl: 'https://www.dataroma.com/m/stock.php?sym=META',
    ownershipCount: '29',
    ownershipRank: '5',
    portfolioPercent: '1.693%',
    holdPrice: '$572.18',
    insiderBuys: { transactions: '0', total: '$0' },
    insiderSells: { transactions: '142', total: '$128,110,152' },
  },
});
assert(dataromaHtml.includes('Super Investor Stats'), 'Dataroma card should render super investor stats');
assert(dataromaHtml.includes('Ownership Count'), 'Dataroma card should render ownership count label');
assert(dataromaHtml.includes('1.693%'), 'Dataroma card should render portfolio percentage');
assert(dataromaHtml.includes('$128,110,152'), 'Dataroma card should render insider sell value');
assert.strictEqual(api.renderDataromaCards({ ticker: 'META' }), '', 'Dataroma card should be omitted when data is absent');

const estimatedMarginData = {
  ...data,
  ticker: 'EST_MARGIN',
  incomeStatement: {
    annual: {
      periods: ['TTM', '2025-12-31'],
      rows: [
        { label: 'Total Revenue', values: ['105B', '100B'] },
      ],
    },
  },
  metrics: {
    ...data.metrics,
    cy_revenue: { raw: 120_000_000_000, display: '120B', kind: 'money' },
    ny_revenue: { raw: 144_000_000_000, display: '144B', kind: 'money' },
    revenue: { raw: 100_000_000_000, display: '100B', kind: 'money' },
    yearAgoEps: { raw: 8, display: '8', kind: 'number' },
    currentYearEps: { raw: 10, display: '10', kind: 'number' },
    nextYearEps: { raw: 14, display: '14', kind: 'number' },
    dilutedShares: { raw: 3_000_000_000, display: '3B', kind: 'number' },
    lastYearEstimatedNetMargin: { raw: 0.24, display: '24%', kind: 'percent' },
    currentYearEstimatedNetMargin: { raw: 0.25, display: '25%', kind: 'percent' },
    nextYearEstimatedNetMargin: { raw: 0.2916666667, display: '29.2%', kind: 'percent' },
  },
};
assert.strictEqual(api.metricDisplay(estimatedMarginData, 'lastYearEstimatedNetMargin'), '24%', 'LY estimated net margin should display from structured metrics');
assert.strictEqual(api.metricDisplay(estimatedMarginData, 'currentYearEstimatedNetMargin'), '25%', 'CY estimated net margin should display from structured metrics');
assert.strictEqual(api.metricDisplay(estimatedMarginData, 'nextYearEstimatedNetMargin'), '29.2%', 'NY estimated net margin should display from structured metrics');
const lyNetMarginCalc = api.calcDefinitions(estimatedMarginData).ly_est_net_margin;
assert.strictEqual(lyNetMarginCalc.numerator, '24B', 'LY estimated net margin calc should show implied net income');
assert.strictEqual(lyNetMarginCalc.divisor, '100B', 'LY estimated net margin calc should show last year revenue from statement base');
assert.strictEqual(lyNetMarginCalc.result, '24%', 'LY estimated net margin calc should show margin result');
const cyNetMarginCalc = api.calcDefinitions(estimatedMarginData).cy_est_net_margin;
assert.strictEqual(cyNetMarginCalc.numerator, '30B', 'CY estimated net margin calc should show implied net income');
assert.strictEqual(cyNetMarginCalc.divisor, '120B', 'CY estimated net margin calc should show CY revenue');
assert.strictEqual(cyNetMarginCalc.result, '25%', 'CY estimated net margin calc should show margin result');
assert(cyNetMarginCalc.rows.some(([label, value]) => label === 'Diluted Shares' && value === '3B'), 'CY estimated net margin calc should show diluted shares');
const nyNetMarginCalc = api.calcDefinitions(estimatedMarginData).ny_est_net_margin;
assert.strictEqual(nyNetMarginCalc.numerator, '42B', 'NY estimated net margin calc should show implied net income');
assert.strictEqual(nyNetMarginCalc.divisor, '144B', 'NY estimated net margin calc should show NY revenue');
assert.strictEqual(nyNetMarginCalc.result, '29.2%', 'NY estimated net margin calc should show margin result');

const bpsMetricHtml = api.metricValueHtml('0.72 bps');
assert(bpsMetricHtml.includes('value-display-with-unit'), 'basis point metric should render as attached unit markup');
assert(bpsMetricHtml.includes('<span>0.72</span><span class="value-unit">bps</span>'), 'basis point unit should stay attached to the number');

console.log('frontend assumption tests passed');
