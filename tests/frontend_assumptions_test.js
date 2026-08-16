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
  querySelectorAll() {
    return [];
  },
};

const storage = new Map();
const storageFailures = new Set();
const localStorageStub = {
  getItem(key) {
    return storage.has(key) ? storage.get(key) : null;
  },
  setItem(key, value) {
    if (storageFailures.has(key)) {
      const err = new Error(`Quota exceeded for ${key}`);
      err.name = 'QuotaExceededError';
      throw err;
    }
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
  URLSearchParams,
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
assert.strictEqual(api.tickerFromUrlSearch('?ticker=ci'), 'CI', 'dashboard links should normalize ticker query parameters');
assert.strictEqual(api.tickerFromUrlSearch('?other=value'), '', 'dashboard links should ignore unrelated query parameters');
assert.strictEqual(api.displayCurrency({ financialCurrency: 'USD', usdFxRate: 1 }), 'USD • 1.0000', 'currency summary should stay compact');

const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
const stylesCss = fs.readFileSync(path.join(__dirname, '..', 'public', 'styles.css'), 'utf8');
assert(indexHtml.includes('data-workspace-tab="metrics"'), 'app shell should expose a Metrics workspace tab');
assert(indexHtml.includes('data-workspace-tab="financials"'), 'app shell should expose a Financials workspace tab');
assert(indexHtml.includes('data-workspace-tab="info"'), 'app shell should expose an Info workspace tab');
assert.strictEqual((indexHtml.match(/data-metric-tab=/g) || []).length, 8, 'metrics workspace should expose eight category tabs');
assert(
  indexHtml.indexOf('class="workspace-tabs"') > indexHtml.indexOf('class="glass-card result-card"'),
  'workspace tabs should appear below the stock summary card',
);
assert(!indexHtml.includes('<h1>Stock Analysis'), 'legacy dashboard hero should not remain visible');
assert(indexHtml.includes('class="tabs legacy-navigation hidden"'), 'legacy list navigation should remain hidden while its functionality is retained');
assert(/\.statement-tabs\s*\{[^}]*flex-wrap:\s*nowrap;/s.test(stylesCss), 'statement tabs should stay on one row');
assert(
  /\.company-overview\s*\{[^}]*border:\s*1px solid var\(--card-border\);[^}]*background:\s*var\(--surface-1\);/s.test(stylesCss),
  'company description should use the same bordered surface treatment as metric cards',
);
assert(
  /\.margin-history-card\s*\{[^}]*max-width:\s*76rem;/s.test(stylesCss),
  'margin history cards should stay compact on wide screens',
);

const metricsWorkspaceTab = elements.get('workspace-tab-metrics');
const financialsWorkspaceTab = elements.get('workspace-tab-financials');
const infoWorkspaceTab = elements.get('workspace-tab-info');
metricsWorkspaceTab.dataset.workspaceTab = 'metrics';
financialsWorkspaceTab.dataset.workspaceTab = 'financials';
infoWorkspaceTab.dataset.workspaceTab = 'info';
api.showDashboardTab('financials');
assert(elements.get('metrics-workspace').classList.contains('hidden'), 'Financials tab should hide metrics');
assert(!elements.get('financials-workspace').classList.contains('hidden'), 'Financials tab should reveal statements');
assert.strictEqual(financialsWorkspaceTab.getAttribute('aria-selected'), 'true', 'Financials tab should expose its selected state');
api.showDashboardTab('metrics');
assert(!elements.get('metrics-workspace').classList.contains('hidden'), 'Metrics tab should reveal metrics');
assert(elements.get('financials-workspace').classList.contains('hidden'), 'Metrics tab should hide statements');
api.showDashboardTab('info');
assert(elements.get('metrics-workspace').classList.contains('hidden'), 'Info tab should hide metrics');
assert(elements.get('financials-workspace').classList.contains('hidden'), 'Info tab should hide statements');
assert(!elements.get('info-workspace').classList.contains('hidden'), 'Info tab should reveal company information');
assert.strictEqual(infoWorkspaceTab.getAttribute('aria-selected'), 'true', 'Info tab should expose its selected state');

api.showMetricTab('growth');
assert.strictEqual(api.state.metricTab, 'growth', 'metric category tabs should update the active category');
assert.strictEqual(storage.get('stock_metric_tab'), 'growth', 'metric category selection should persist between scans');
api.showMetricTab('not-a-category');
assert.strictEqual(api.state.metricTab, 'valuation', 'unknown metric categories should fall back to valuation');

assert(api.accountLabelMatchesSearch('Operating Income', 'op'), 'search should match the prefix of the first word');
assert(api.accountLabelMatchesSearch('Operating Income', 'inc'), 'search should match the prefix of any word');
assert(api.accountLabelMatchesSearch('Net Non Operating Interest Income Expense', 'int inc'), 'multi-word search should match word prefixes across the label');
assert(api.accountLabelMatchesSearch('Cash, Equivalents & Short Term Investments', 'eq sh inv'), 'search should treat punctuation as word boundaries');
assert(api.accountLabelMatchesSearch('Total Revenue', 'cash, rev'), 'comma-separated search should match any group');
assert(api.accountLabelMatchesSearch('Cash Dividends Paid', 'rev, cash div'), 'comma-separated search should preserve multi-word matching inside each group');
assert(!api.accountLabelMatchesSearch('Operating Income', 'come'), 'search should not match arbitrary substrings inside a word');
assert(!api.accountLabelMatchesSearch('Operating Income', 'rev, cash'), 'comma-separated search should reject labels that match none of the groups');

assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.historicalRevenueGrowth({
    incomeStatement: {
      annual: {
        periods: ['TTM', '2025-12-31', '2023-12-31', '2022-12-31', '2024-12-31'],
        rows: [{ label: 'Total Revenue', values: ['150B', '144B', '110B', '100B', '120B'] }],
      },
    },
  }))),
  [
    { label: '2023', value: '10%' },
    { label: '2024', value: '9.1%' },
    { label: '2025', value: '20%' },
  ],
  'revenue growth should use the latest three fiscal-year changes and ignore TTM',
);

const marginHistoryData = {
  incomeStatement: {
    annual: {
      periods: ['TTM', '2025-12-31', '2023-12-31', '2022-12-31', '2024-12-31'],
      rows: [
        { label: 'Total Revenue', values: ['150B', '144B', '110B', '100B', '120B'] },
        { label: 'Gross Profit', values: ['90B', '72B', '44B', '30B', '60B'] },
        { label: 'Adjusted Operating Income', values: ['45B', '43.2B', '27.5B', '20B', '36B'] },
        { label: 'Diluted Average Shares', values: ['92M', '94M', '100M', '105M', '97M'] },
      ],
    },
    quarterly: {
      periods: ['2026-06-30', '2026-03-31', '2025-12-31'],
    },
  },
};
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.historicalMarginSeries(marginHistoryData, ['Gross Profit']))),
  [
    { label: '2022', value: '30%' },
    { label: '2023', value: '40%' },
    { label: '2024', value: '50%' },
    { label: '2025', value: '50%' },
    { label: 'TTM', value: '60%' },
  ],
  'gross margin history should show four fiscal years followed by TTM',
);
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.historicalMarginSeries(marginHistoryData, ['Adjusted Operating Income']))),
  [
    { label: '2022', value: '20%' },
    { label: '2023', value: '25%' },
    { label: '2024', value: '30%' },
    { label: '2025', value: '30%' },
    { label: 'TTM', value: '30%' },
  ],
  'adjusted operating margin history should use adjusted operating income over revenue',
);
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.historicalIncomeGrowthSeries(marginHistoryData, ['Gross Profit']))),
  [
    { label: '2023', value: '46.7%' },
    { label: '2024', value: '36.3%' },
    { label: '2025', value: '20%' },
    { label: 'TTM', value: '56.9%' },
  ],
  'gross profit history should show three annual growth rates and date-annualized TTM growth',
);
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.historicalIncomeGrowthSeries(marginHistoryData, ['Adjusted Operating Income']))),
  [
    { label: '2023', value: '37.5%' },
    { label: '2024', value: '30.8%' },
    { label: '2025', value: '20%' },
    { label: 'TTM', value: '8.6%' },
  ],
  'adjusted operating income history should show three annual growth rates and date-annualized TTM growth',
);
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.historicalIncomeGrowthSeries(marginHistoryData, ['Diluted Average Shares']))),
  [
    { label: '2023', value: '-4.8%' },
    { label: '2024', value: '-3%' },
    { label: '2025', value: '-3.1%' },
    { label: 'TTM', value: '-4.2%' },
  ],
  'diluted share history should show three annual changes and date-annualized TTM change',
);

api.state.statementTab = 'income';
api.state.periodicity = 'annual';
api.state.statementSearch = '';
const copyData = {
  incomeStatement: {
    annual: {
      periods: ['2024-12-31', '2025-12-31', 'TTM'],
      rows: [
        { label: 'Total Revenue', values: ['1.2B', '1,500,000', '--'] },
        { label: 'Gross Margin', values: ['22%', '23.5%', '24%'] },
      ],
    },
  },
};
assert.strictEqual(
  api.buildStatementCopyText(copyData),
  'Line Item\t2024-12-31\t2025-12-31\tTTM\nTotal Revenue\t1.2\t0.0015\t\nGross Margin\t22%\t23.5%\t24%',
  'statement copy should use tab-separated billion-scaled numbers without B/M suffixes',
);
api.state.statementSearch = 'rev';
assert.strictEqual(
  api.buildStatementCopyText(copyData),
  'Line Item\t2024-12-31\t2025-12-31\tTTM\nTotal Revenue\t1.2\t0.0015\t',
  'statement copy should respect visible search-filtered rows',
);
api.state.statementSearch = '';

const compactTicker = api.compactTickerData({
  ticker: 'ROK',
  companyName: 'Rockwell Automation',
  metrics: { margin: { raw: 0.2, display: '20%', kind: 'percent' } },
  incomeStatement: { annual: { rows: [{ label: 'Total Revenue', values: ['9B'] }] } },
  balanceStatement: { annual: { rows: [{ label: 'Total Assets', values: ['12B'] }] } },
  cashFlowStatement: { annual: { rows: [{ label: 'Operating Cash Flow', values: ['1B'] }] } },
});
assert.strictEqual(compactTicker.ticker, 'ROK', 'compact ticker cache should keep ticker identity');
assert.strictEqual(compactTicker.metrics.margin.display, '20%', 'compact ticker cache should keep metric summaries');
assert.strictEqual(compactTicker.incomeStatement, undefined, 'compact ticker cache should drop statement payloads');
api.state.dataByTicker.ROK = compactTicker;
storageFailures.add('stock_data_by_ticker');
assert.doesNotThrow(() => api.saveTickerData('ROK'), 'ticker cache quota failures should not fail the scan');
storageFailures.delete('stock_data_by_ticker');

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
assert.deepStrictEqual(
  api.growthValues(['100', '--', '121'], ['2022-12-31', '2023-12-31', '2024-12-31']),
  ['--', '--', '10.0%'],
  'annual growth should bridge blank periods and annualize over the date gap',
);
assert.deepStrictEqual(
  api.growthValues(
    ['466M', '482M', '496M', '--', '556M'],
    ['2021-12-31', '2022-12-31', '2023-12-31', '2024-12-31', '2025-12-31'],
  ),
  ['--', '3.4%', '2.9%', '--', '5.9%'],
  'annual growth should bridge multi-year money gaps by annualizing between the nearest real values',
);
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
api.state.latest = {
  incomeStatement: {
    quarterly: { periods: ['2025-12-31', '2025-09-30'] },
  },
};
assert.deepStrictEqual(
  api.growthValues(['100', '120', '120'], ['2024-12-31', '2025-12-31', 'TTM'], 'income'),
  ['--', '20.0%', '--'],
  'annual TTM growth should be blank when TTM ends at the latest annual period',
);
api.state.latest = {
  incomeStatement: {
    quarterly: { periods: ['2026-03-31', '2025-12-31'] },
  },
};
assert.deepStrictEqual(
  api.growthValues(['100', '120', '120'], ['2024-12-31', '2025-12-31', 'TTM'], 'income'),
  ['--', '20.0%', '--'],
  'annual TTM growth should be blank when the TTM value is the same as the latest annual value',
);
api.state.latest = {
  incomeStatement: {
    quarterly: { periods: ['2026-03-31', '2025-12-31'] },
  },
};
assert.deepStrictEqual(
  api.growthValues(['466M', '482M', '496M', '--', '556M'], ['2022-12-31', '2023-12-31', '2024-12-31', '2025-12-31', 'TTM'], 'income'),
  ['--', '3.4%', '2.9%', '--', '9.6%'],
  'annual TTM growth should bridge blank annual periods and annualize from the nearest prior value',
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
assert.strictEqual(api.state.statementTab, 'starred-income', 'new tickers should default statements to starred income');
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

assert.strictEqual(api.starredStatementKey('starred-income'), 'income', 'starred IS should map to the income statement');
assert.strictEqual(api.starredStatementKey('starred-balance'), 'balance', 'starred BS should map to the balance sheet');
assert.strictEqual(api.starredStatementKey('starred-cash'), 'cash', 'starred CF should map to the cash flow statement');
assert.strictEqual(api.starredStatementKey('starred-ratios'), 'ratios', 'starred ratios should map to ratios');
assert.strictEqual(api.starredStatementKey('income'), '', 'regular statement tabs should not be treated as starred tabs');

api.state.periodicity = 'annual';
api.state.statementTab = 'starred-balance';
api.state.statementSearch = '';
api.state.starredAccounts = {
  'income:Total Revenue': true,
  'balance:Total Assets': true,
};
assert.strictEqual(
  api.buildStatementCopyText({
    incomeStatement: { annual: { periods: ['2025-12-31'], rows: [{ label: 'Total Revenue', values: ['10B'] }] } },
    balanceStatement: { annual: { periods: ['2025-12-31'], rows: [{ label: 'Total Assets', values: ['20B'] }] } },
  }),
  'Line Item\t2025-12-31\nTotal Assets\t20',
  'focused starred tabs should copy only rows from their statement type',
);

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
        { label: 'Gross PP&E', values: ['200B', '150B', '100B'] },
      ],
    },
    quarterly: {
      periods: ['2025-03-31', '2025-06-30'],
      rows: [
        { label: 'Total Assets', values: ['50B', '60B'] },
        { label: 'Total Debt', values: ['10B', '12B'] },
        { label: 'Stockholders Equity', values: ['40B', '44B'] },
        { label: 'Cash, Equivalents & Short Term Investments', values: ['5B', '6B'] },
        { label: 'Gross PP&E', values: ['20B', '25B'] },
      ],
    },
  },
};
api.state.periodicity = 'annual';
const annualRatios = api.buildRatiosStatement(ratiosData);
assert.deepStrictEqual(Array.from(annualRatios.periods), ['TTM', '2025-12-31', '2024-12-31'], 'annual ratios should use income statement periods');
assert.strictEqual(annualRatios.rows[0].label, 'ROA', 'annual ratios should include ROA');
assert.deepStrictEqual(Array.from(annualRatios.rows[0].values), ['10%', '10%', '10%'], 'annual ratios should calculate ROA from net income over total assets, with TTM paired to MRQ assets');
assert.strictEqual(annualRatios.rows[1].label, 'ROE', 'annual ratios should include ROE');
assert.deepStrictEqual(Array.from(annualRatios.rows[1].values), ['17.1%', '15.4%', '13.3%'], 'annual ROE should calculate net income over equity, with TTM paired to MRQ equity');
assert.strictEqual(annualRatios.rows[2].label, 'ROIC', 'annual ratios should include ROIC');
assert.deepStrictEqual(Array.from(annualRatios.rows[2].values), ['20%', '18%', '17.1%'], 'annual ROIC should use after-tax operating income over debt plus equity minus cash');
assert.strictEqual(annualRatios.rows[3].label, 'Adj ROIC', 'annual ratios should include adjusted ROIC');
assert.deepStrictEqual(Array.from(annualRatios.rows[3].values), ['22.5%', '20%', '20%'], 'annual Adj ROIC should use adjusted net income over debt plus equity minus cash');
assert.strictEqual(annualRatios.rows[4].label, 'ROGPPE', 'annual ratios should include ROGPPE');
assert.deepStrictEqual(Array.from(annualRatios.rows[4].values), ['9%', '10%', '14%'], 'annual ROGPPE should use adjusted net income over gross PP&E, with TTM paired to MRQ gross PP&E');
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(api.currentReturnRatioValues(ratiosData))),
  { roa: '10%', roe: '17.1%', roic: '20%' },
  'returns metrics should use the current annual TTM ratio values regardless of statement controls',
);
const netPpeFallbackRatios = api.buildRatiosStatement({
  incomeStatement: {
    annual: {
      periods: ['TTM'],
      rows: [{ label: 'Adjusted Net Income', values: ['10B'] }],
    },
  },
  balanceStatement: {
    annual: {
      periods: ['MRQ'],
      rows: [{ label: 'Net PP&E', values: ['50B'] }],
    },
  },
});
assert.strictEqual(netPpeFallbackRatios.rows[4].values[0], '20%', 'ROGPPE should fall back to net PP&E when gross PP&E is unavailable');
const noInvestedCapitalRatios = api.buildRatiosStatement({
  incomeStatement: {
    annual: {
      periods: ['2025-12-31'],
      rows: [
        { label: 'Operating Income', values: ['10B'] },
        { label: 'Adjusted Net Income', values: ['8B'] },
        { label: 'Tax Rate', values: ['20%'] },
      ],
    },
  },
  balanceStatement: {
    annual: {
      periods: ['2025-12-31'],
      rows: [
        { label: 'Total Debt', values: ['10B'] },
        { label: 'Stockholders Equity', values: ['5B'] },
        { label: 'Cash, Equivalents & Short Term Investments', values: ['15B'] },
      ],
    },
  },
});
assert.strictEqual(noInvestedCapitalRatios.rows[2].values[0], '--', 'ROIC should be blank when invested capital is zero');
assert.strictEqual(noInvestedCapitalRatios.rows[3].values[0], '--', 'Adj ROIC should be blank when invested capital is zero');
api.state.periodicity = 'quarterly';
const quarterlyRatios = api.buildRatiosStatement(ratiosData);
assert.deepStrictEqual(Array.from(quarterlyRatios.periods), ['2025-03-31', '2025-06-30'], 'quarterly ratios should use income statement periods');
assert.strictEqual(quarterlyRatios.rows[0].label, 'ROA', 'quarterly ratios should include ROA');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[0].values), ['2%', '2.5%'], 'quarterly ratios should calculate ROA from same-period quarterly net income and assets');
assert.strictEqual(quarterlyRatios.rows[1].label, 'ROE', 'quarterly ratios should include ROE');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[1].values), ['2.5%', '3.4%'], 'quarterly ROE should calculate net income over same-period equity');
assert.strictEqual(quarterlyRatios.rows[2].label, 'ROIC', 'quarterly ratios should include ROIC');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[2].values), ['3.6%', '3.6%'], 'quarterly ROIC should use same-period after-tax operating income and invested capital');
assert.strictEqual(quarterlyRatios.rows[3].label, 'Adj ROIC', 'quarterly ratios should include adjusted ROIC');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[3].values), ['4%', '4%'], 'quarterly Adj ROIC should use same-period adjusted net income and invested capital');
assert.strictEqual(quarterlyRatios.rows[4].label, 'ROGPPE', 'quarterly ratios should include ROGPPE');
assert.deepStrictEqual(Array.from(quarterlyRatios.rows[4].values), ['9%', '8%'], 'quarterly ROGPPE should use same-period adjusted net income and gross PP&E');
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

api.state.latest = {
  incomeStatement: {
    annual: {
      periods: ['TTM', '2025-12-31'],
      rows: [
        { label: 'Total Revenue', values: ['200B', '100B'] },
        { label: 'EBITDA', values: ['40B', '25B'] },
      ],
    },
  },
};
const filteredIncomeForMargin = {
  periods: ['TTM', '2025-12-31'],
  rows: [{ label: 'EBITDA', values: ['40B', '25B'] }],
};
assert.deepStrictEqual(
  api.marginValues(filteredIncomeForMargin.rows[0], filteredIncomeForMargin.periods, filteredIncomeForMargin, 'income'),
  ['20.0%', '25.0%'],
  'filtered income statement margin rows should use revenue from the full income statement',
);
const cashForMargin = {
  periods: ['TTM', '2025-12-31'],
  rows: [
    { label: 'Operating Cash Flow', values: ['50B', '40B'] },
    { label: 'Capital Expenditures', values: ['-20B', '-10B'] },
  ],
};
assert.deepStrictEqual(
  api.marginValues(cashForMargin.rows[1], cashForMargin.periods, cashForMargin, 'cash'),
  ['-10.0%', '-10.0%'],
  'cash flow margin rows should use income statement revenue as the denominator instead of operating cash flow',
);
api.state.latest = null;

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

const salesMultipleData = {
  ...data,
  ticker: 'SALES',
  ev: '30B',
  marketCap: '20B',
  incomeStatement: {
    annual: {
      periods: ['TTM', '2025-12-31'],
      rows: [
        { label: 'Total Revenue', values: ['10B', '9B'] },
        { label: 'Gross Profit', values: ['4B', '3.5B'] },
      ],
    },
  },
  metrics: {
    ...data.metrics,
    ev: { raw: 30_000_000_000, display: '30B', kind: 'money' },
    marketCap: { raw: 20_000_000_000, display: '20B', kind: 'money' },
    revenue: { raw: 10_000_000_000, display: '10B', kind: 'money' },
  },
};
const salesMultiples = api.applyAssumptions(salesMultipleData);
assertAlmostEqual(api.metricEntry(salesMultiples, 'priceSales').raw, 2, 0.0001, 'price to sales should use market cap over TTM revenue');
assertAlmostEqual(api.metricEntry(salesMultiples, 'priceGrossProfit').raw, 5, 0.0001, 'price to gross profit should use market cap over TTM gross profit');
const priceSalesCalc = api.calcDefinitions(salesMultiples).price_sales;
assert.strictEqual(priceSalesCalc.numerator, '20B', 'price to sales calc should show market cap as its numerator');
assert.strictEqual(priceSalesCalc.divisor, '10B', 'price to sales calc should show TTM revenue as its denominator');
const priceGrossProfitCalc = api.calcDefinitions(salesMultiples).price_gross_profit;
assert.strictEqual(priceGrossProfitCalc.numerator, '20B', 'price to gross profit calc should show market cap as its numerator');
assert.strictEqual(priceGrossProfitCalc.divisor, '4B', 'price to gross profit calc should show TTM gross profit as its denominator');

const growthRevenueCalc = api.calcDefinitions(adjusted).growth_revenue;
assert.strictEqual(growthRevenueCalc.title, 'Revenue Growth', 'growth section should link to a revenue bridge calc');
assert(growthRevenueCalc.rows.some(([label, value]) => label === 'Last Year Revenue' && value === '950M'), 'growth calc should include last year revenue');
assert(growthRevenueCalc.rows.some(([label, value]) => label === 'CY Revenue' && value !== '--'), 'growth calc should include CY revenue');
assert(growthRevenueCalc.rows.some(([label, value]) => label === 'NY Revenue' && value !== '--'), 'growth calc should include NY revenue');

const estimateBaseData = {
  ...data,
  ticker: 'ESTBASE',
  metrics: {
    ...data.metrics,
    cyRevenueBase: { raw: 6_156_000_000, display: '6.16B', kind: 'money' },
    cy_revenue: { raw: 6_958_767_500, display: '6.96B', kind: 'money' },
    cy_growth: { raw: 0.1304, display: '13%', kind: 'percent' },
  },
};
const estimateBaseCalc = api.calcDefinitions(estimateBaseData).growth_revenue;
assert(estimateBaseCalc.rows.some(([label, value]) => label === 'Last Year Revenue' && value === '6.2B'), 'growth calc should prefer Yahoo estimate year-ago sales when present');

api.state.assumptions.ESTBASE = { cy_growth: 0.20 };
const adjustedEstimateBase = api.applyAssumptions(estimateBaseData);
assertAlmostEqual(api.metricEntry(adjustedEstimateBase, 'cy_revenue').raw, 7_387_200_000, 1, 'edited CY growth should use Yahoo estimate year-ago sales when present');

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
assert(JSON.parse(storage.get('stock_assumptions')).TAX.margin === 0.28, 'reset should persist the remaining edited assumptions');
api.resetAssumption('margin');
assert.strictEqual(api.state.assumptions.TAX, undefined, 'resetting the final edited assumption should remove the ticker entry');
assert.strictEqual(JSON.parse(storage.get('stock_assumptions')).TAX, undefined, 'resetting the final edited assumption should persist removal');

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
api.saveAssumptions();
assert.strictEqual(JSON.parse(storage.get('stock_assumptions')).GROWTH.cy_growth, 0.20, 'edited assumptions should persist across scans until reset');

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
