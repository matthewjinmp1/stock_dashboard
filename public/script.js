document.addEventListener('DOMContentLoaded', () => {
    const $ = (id) => document.getElementById(id);
    const FORWARD_DISCOUNT_RATE = 0.10;
    const localStarredAccounts = JSON.parse(localStorage.getItem('stock_starred_accounts') || '{}');
    const state = {
        activeView: 'scanner',
        previousScroll: 0,
        latest: null,
        dataByTicker: JSON.parse(localStorage.getItem('stock_data_by_ticker') || '{}'),
        watchlist: JSON.parse(localStorage.getItem('stock_watchlist') || '[]'),
        starred: JSON.parse(localStorage.getItem('stock_starred_tickers') || '[]'),
        most: JSON.parse(localStorage.getItem('stock_search_counts') || '{}'),
        assumptions: {},
        statementTab: localStorage.getItem('stock_statement_tab') || 'income',
        periodicity: localStorage.getItem('stock_periodicity') || 'annual',
        quarterlyGrowthMode: localStorage.getItem('stock_quarterly_growth_mode') || 'yoy',
        statementSearch: '',
        starredAccounts: localStarredAccounts,
        statementToggles: JSON.parse(localStorage.getItem('stock_statement_toggles') || '{}'),
        groups: [],
        sort: {},
        scanRequestId: 0,
        loadedTicker: null,
    };
    let activeFetchTimer = null;
    localStorage.removeItem('stock_assumptions');
    localStorage.removeItem('stock_statement_search');

    const views = {
        scanner: $('view-scanner'),
        watchlist: $('view-watchlist'),
        groups: $('view-groups'),
        starred: $('view-starred'),
        'most-searched': $('view-most-searched'),
        fetchDetails: $('view-fetch-details'),
        calc: $('view-calc'),
    };

    const tabIds = ['scanner', 'watchlist', 'groups', 'starred', 'most-searched'];
    tabIds.forEach((name) => {
        const tab = $(`tab-${name}`);
        if (!tab) return;
        tab.addEventListener('click', () => showView(name));
    });
    const fetchInfoButton = $('result-fetch-info');
    if (fetchInfoButton) {
        fetchInfoButton.addEventListener('click', () => openFetchDetails());
    }

    function save(key, value) {
        localStorage.setItem(key, JSON.stringify(value));
    }

    function saveTickerData() {
        save('stock_data_by_ticker', state.dataByTicker);
    }

    function hasEnabledStarredAccount(accounts) {
        return Object.values(accounts || {}).some(Boolean);
    }

    async function loadStarredAccounts() {
        try {
            const response = await fetch('/api/preferences/starred-accounts');
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.error || 'Failed to load starred accounts');
            const serverAccounts = payload.starredAccounts || {};
            if (hasEnabledStarredAccount(serverAccounts)) {
                state.starredAccounts = serverAccounts;
                localStorage.setItem('stock_starred_accounts', JSON.stringify(serverAccounts));
            } else if (hasEnabledStarredAccount(localStarredAccounts)) {
                await saveStarredAccounts();
            }
            renderStatements(state.latest);
        } catch (err) {
            console.warn('Starred account preferences unavailable; using browser backup.', err);
        }
    }

    async function saveStarredAccounts() {
        localStorage.setItem('stock_starred_accounts', JSON.stringify(state.starredAccounts));
        try {
            const response = await fetch('/api/preferences/starred-accounts', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ starredAccounts: state.starredAccounts }),
            });
            if (!response.ok) throw new Error('Failed to save starred accounts');
        } catch (err) {
            console.warn('Starred account preferences were saved only in this browser.', err);
        }
    }

    function showView(name) {
        state.activeView = name;
        Object.entries(views).forEach(([viewName, node]) => {
            if (!node) return;
            node.classList.toggle('hidden', viewName !== name);
        });
        tabIds.forEach((tabName) => {
            const tab = $(`tab-${tabName}`);
            if (tab) tab.classList.toggle('active', tabName === name);
        });
        if (name === 'watchlist') renderTickerTable('watchlist');
        if (name === 'groups') renderTickerTable('groups');
        if (name === 'starred') renderStarredTickers();
        if (name === 'most-searched') renderMostSearched();
    }

    function formatSigned(value) {
        if (value && typeof value === 'object' && 'display' in value) return formatSigned(value.display);
        if (typeof value !== 'string') return value || '--';
        return value.startsWith('+') ? value.slice(1) : value;
    }

    function metricEntry(data, key) {
        return data?.metrics?.[key] ?? data?.[key];
    }

    function metricDisplay(data, key) {
        return formatSigned(metricEntry(data, key) || '--');
    }

    function setMetric(data, key, raw, display, kind = 'number') {
        data[key] = display;
        data.metrics = data.metrics || {};
        data.metrics[key] = { raw, display, kind };
    }

    function formatStatementValue(value) {
        const display = formatSigned(value);
        if (display === '--') return display;
        const raw = String(display).replace(/,/g, '').trim();
        if (!/^-?\d+(\.\d+)?$/.test(raw)) return display;
        const number = Number(raw);
        if (!Number.isFinite(number) || Math.abs(number) < 1e6) return display;
        return formatMoneyFront(number);
    }

    function displayDate(data) {
        const source = data.pulledAt || data.dataDate || '';
        const match = String(source).match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/);
        if (!match) return data.dataDate || '--';

        const [, year, month, day, hour, minute] = match;
        const monthName = [
            'January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December',
        ][Number(month) - 1];
        const dayNumber = Number(day);
        const suffix = dayNumber % 10 === 1 && dayNumber % 100 !== 11 ? 'st'
            : dayNumber % 10 === 2 && dayNumber % 100 !== 12 ? 'nd'
            : dayNumber % 10 === 3 && dayNumber % 100 !== 13 ? 'rd'
            : 'th';

        if (!hour || !minute) return `${monthName} ${dayNumber}${suffix}, ${year}`;

        const hourNumber = Number(hour);
        const period = hourNumber >= 12 ? 'pm' : 'am';
        const displayHour = hourNumber % 12 || 12;
        return `${monthName} ${dayNumber}${suffix}, ${displayHour}:${minute}${period}, ${year}`;
    }

    function displayFetchInfo(data) {
        const fetches = data.fetchCount === undefined ? '--' : data.fetchCount;
        return `Fetch time: ${data.fetchTime || '--'} • Fetches: ${fetches}`;
    }

    function setFetchInfoText(text, loading = false) {
        const node = $('result-fetch-info');
        if (!node) return;
        node.textContent = text;
        node.classList.toggle('loading', loading);
        node.setAttribute('aria-busy', loading ? 'true' : 'false');
    }

    function startFetchTimer(startedAt, requestId) {
        stopFetchTimer();
        const update = () => {
            if (requestId !== state.scanRequestId) return;
            const elapsed = (performance.now() - startedAt) / 1000;
            setFetchInfoText(`Fetching: ${elapsed.toFixed(2)}s • Fetches: --`, true);
        };
        update();
        if (typeof requestAnimationFrame === 'function') requestAnimationFrame(update);
        activeFetchTimer = setInterval(update, 100);
    }

    function stopFetchTimer() {
        if (!activeFetchTimer) return;
        clearInterval(activeFetchTimer);
        activeFetchTimer = null;
        const node = $('result-fetch-info');
        if (node) {
            node.classList.remove('loading');
            node.setAttribute('aria-busy', 'false');
        }
    }

    function formatSeconds(value) {
        const number = Number(value);
        if (!Number.isFinite(number)) return '--';
        return `${number.toFixed(2)}s`;
    }

    function fetchTimingRows(data) {
        const timing = data?.fetchTiming || {};
        const rows = [];
        if (Number.isFinite(Number(timing.clientSeconds))) {
            rows.push({
                label: 'Browser round trip',
                seconds: Number(timing.clientSeconds),
                status: 'ok',
            });
        }
        if (Number.isFinite(Number(timing.totalSeconds))) {
            const sourceLabels = {
                cache: 'Cache response',
                'stale-cache': 'Stale cache response',
                test: 'Test fixture',
                fresh: 'Backend yfinance work',
                error: 'Backend yfinance work',
            };
            rows.push({
                label: sourceLabels[timing.source] || 'Backend yfinance work',
                seconds: Number(timing.totalSeconds),
                status: timing.source === 'error' ? 'error' : 'ok',
            });
        }
        (timing.stages || []).forEach((stage) => {
            rows.push({
                label: stage.label || stage.key || 'Fetch stage',
                seconds: Number(stage.seconds),
                status: stage.status || 'ok',
            });
        });
        return rows.filter((row) => Number.isFinite(row.seconds));
    }

    function renderFetchDetails(data, targetId = 'fetch-detail-content') {
        const panel = $(targetId);
        if (!panel) return;
        const rows = fetchTimingRows(data);
        if (!rows.length) {
            panel.innerHTML = '<div class="fetch-detail-empty">No fetch timing details for this response.</div>';
            return;
        }
        const maxSeconds = Math.max(...rows.map((row) => row.seconds), 0.01);
        panel.innerHTML = `<div class="fetch-detail-header">
            <span>Fetch Breakdown</span>
            <span>${data.fetchTime || formatSeconds(data.fetchTiming?.clientSeconds)}</span>
        </div>
        <div class="fetch-detail-rows">
            ${rows.map((row) => {
                const width = Math.max(4, Math.min(100, (row.seconds / maxSeconds) * 100));
                const status = row.status && row.status !== 'ok' ? `<small>${escapeAttr(row.status)}</small>` : '';
                return `<div class="fetch-detail-row">
                    <div class="fetch-detail-label">${escapeAttr(row.label)}${status}</div>
                    <div class="fetch-detail-bar"><span style="width:${width}%"></span></div>
                    <div class="fetch-detail-time">${formatSeconds(row.seconds)}</div>
                </div>`;
            }).join('')}
        </div>`;
    }

    function openFetchDetails() {
        if (!state.latest) return;
        state.previousScroll = window.scrollY;
        $('fetch-ticker-badge').textContent = state.latest.ticker || '--';
        renderFetchDetails(state.latest);
        showView('fetchDetails');
        document.querySelector('.tabs').classList.add('hidden');
        window.scrollTo(0, 0);
    }

    function displayCurrency(data) {
        const currency = data.financialCurrency || '--';
        const rate = Number(data.usdFxRate);
        const formattedRate = Number.isFinite(rate) ? rate.toFixed(4) : '--';
        return `Native currency: ${currency} • USD rate: ${formattedRate}`;
    }

    function escapeAttr(value) {
        return String(value ?? '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function metricValueHtml(value) {
        const displayValue = formatSigned(value || '--');
        const bpsMatch = String(displayValue).match(/^(.+?)\s+bps$/);
        if (bpsMatch) {
            return `<div class="value-display value-display-with-unit"><span>${escapeAttr(bpsMatch[1])}</span><span class="value-unit">bps</span></div>`;
        }
        return `<div class="value-display">${escapeAttr(displayValue)}</div>`;
    }

    function metricInputWidth(value) {
        const length = String(value ?? '').length || 2;
        return `${Math.max(3.5, Math.min(length + 1.25, 10))}ch`;
    }

    function metric(label, value, calcType = '', editType = '') {
        const link = calcType ? ' metric-title-link' : '';
        const displayValue = formatSigned(value || '--');
        const ticker = (state.latest?.ticker || '').toUpperCase();
        const isEdited = Boolean(editType && ticker && state.assumptions[ticker]?.[editType] !== undefined);
        const editableValue = editType
            ? `<span class="metric-edit-row"><span class="metric-edit-wrap" data-metric-value="${escapeAttr(displayValue)}"><input class="value-display metric-edit-input" type="text" value="${escapeAttr(displayValue)}" style="--metric-input-width: ${metricInputWidth(displayValue)}" data-edit-assumption="${editType}" data-original-value="${escapeAttr(displayValue)}" aria-label="Edit ${escapeAttr(label)}"></span>${isEdited ? `<button class="metric-reset-btn" type="button" data-reset-assumption="${editType}" aria-label="Reset ${escapeAttr(label)} to original value">Reset</button>` : ''}</span>`
            : metricValueHtml(displayValue);
        return `<div class="stat-box">
            <span class="stat-label${link}" data-calc="${calcType}">${label}</span>
            ${editableValue}
        </div>`;
    }

    function metricGroup(title, items) {
        return `<section class="metric-group metric-count-${items.length}">
            <h3>${title}</h3>
            <div class="metric-group-grid">${items.join('')}</div>
        </section>`;
    }

    function renderStats(data) {
        data = applyAssumptions(data);
        const val = (key) => metricDisplay(data, key);
        const netCash = netCashPresentation(data);
        const stats = $('result-stats');
        if (!stats) return;
        stats.classList.remove('stats-grid');
        stats.innerHTML = [
            metricGroup('Margins', [
                metric('Adj Op Inc Margin', val('margin'), 'adj_margin', 'margin'),
                metric('Gross Margin', val('grossMargin')),
            ]),
            metricGroup('Growth', [
                metric('3Y Growth', val('gp_3y_growth') || '--', 'gp_3y_growth'),
                metric('CY Growth', val('cy_growth'), '', 'cy_growth'),
                metric('NY Growth', val('ny_growth'), '', 'ny_growth'),
            ]),
            metricGroup('Valuation', [
                metric(`${data.valuationPrefix || 'EV'}/Adj Inc`, val('ev_adj_ebit'), 'ev_adj'),
                metric(`${data.valuationPrefix || 'EV'}/CY Adj Inc`, val('ev_cy_ebit'), 'ev_cy'),
                metric(`${data.valuationPrefix || 'EV'}/NY Adj Inc`, val('ev_ny_ebit'), 'ev_ny'),
            ]),
            metricGroup('Returns', [
                metric('ROGPPE', val('adjEbitGrossPpe'), 'adj_ebit_gross_ppe'),
                metric('ROC', val('roc'), 'roc'),
            ]),
            metricGroup('Spending', [
                metric('Investment Rate', val('capexAdjIncome'), 'capex_adj_income'),
                metric('R&D / Adj Op Inc', val('rndAdjIncome')),
            ]),
            metricGroup('Taxes', [
                metric('Median Tax Rate', val('medianTaxRate'), '', 'medianTaxRate'),
            ]),
            metricGroup('Short Interest', [
                metric('Short % Shares Out', val('shortFloat')),
            ]),
            metricGroup('Market', [
                metric('Market Cap', val('marketCap')),
                metric(netCash.label, netCash.display, 'net_cash'),
                metric('Our EV', val('derivedEnterpriseValue')),
            ]),
            metricGroup('Price & Yield', [
                metric('Current Price', val('currentPrice')),
                metric('Dividend Yield', val('dividendYield')),
                metric('Beta', val('beta')),
                metric('Est Txn Cost', val('transactionCost'), 'transaction_cost'),
            ]),
            metricGroup('EPS Growth', [
                metric('CY EPS Growth', val('currentYearEpsGrowth')),
                metric('NY EPS Growth', val('nextYearEpsGrowth')),
            ]),
            metricGroup('Est Net Margin', [
                metric('LY Net Margin', val('lastYearEstimatedNetMargin'), 'ly_est_net_margin'),
                metric('CY Net Margin', val('currentYearEstimatedNetMargin'), 'cy_est_net_margin'),
                metric('NY Net Margin', val('nextYearEstimatedNetMargin'), 'ny_est_net_margin'),
            ]),
            metricGroup('P/E', [
                metric('P/LY EPS', val('priceCurrentEps')),
                metric('P/CY EPS', val('priceCyEps')),
                metric('P/NY EPS', val('priceNyEps')),
            ]),
            renderDataromaCards(data),
            renderAnalystCards(data),
        ].join('');

        stats.querySelectorAll('[data-calc]').forEach((node) => {
            node.addEventListener('click', () => openCalc(node.dataset.calc));
        });
        stats.querySelectorAll('[data-case]').forEach((node) => {
            node.addEventListener('click', () => openCalc(`target_${node.dataset.case}`));
        });
        stats.querySelectorAll('[data-edit-assumption]').forEach((node) => {
            const syncWidth = () => {
                const display = node.value || node.dataset.editingOriginalValue || node.dataset.originalValue || '--';
                node.style.setProperty('--metric-input-width', metricInputWidth(display));
                if (node.parentElement) node.parentElement.dataset.metricValue = display;
            };
            node.addEventListener('focus', () => {
                node.dataset.editingOriginalValue = node.value;
                node.value = '';
                syncWidth();
            });
            node.addEventListener('input', syncWidth);
            node.addEventListener('blur', () => commitAssumptionInput(node));
            node.addEventListener('keydown', (event) => {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    node.blur();
                }
                if (event.key === 'Escape') {
                    node.value = node.dataset.editingOriginalValue || node.dataset.originalValue || '';
                    node.blur();
                }
            });
            node.title = 'Edit directly. Press Enter or click away to apply.';
        });
        stats.querySelectorAll('[data-reset-assumption]').forEach((node) => {
            node.addEventListener('click', () => resetAssumption(node.dataset.resetAssumption));
        });
    }

    function applyAssumptions(input) {
        const data = { ...input };
        data.metrics = { ...(input.metrics || {}) };
        const ticker = (data.ticker || '').toUpperCase();
        const assumptions = state.assumptions[ticker] || {};
        const originalMargin = parsePercentValue(metricEntry(data, 'margin')) || parsePercentValue(data.margin);
        const margin = assumptions.margin ?? originalMargin;
        const cyGrowth = assumptions.cy_growth ?? (parsePercentValue(metricEntry(data, 'cy_growth')) || parsePercentValue(data.cy_growth));
        const nyGrowth = assumptions.ny_growth ?? (parsePercentValue(metricEntry(data, 'ny_growth')) || parsePercentValue(data.ny_growth));
        const sourceTaxRate = parsePercentValue(metricEntry(data, 'medianTaxRate')) || parsePercentValue(data.medianTaxRate);
        const taxRate = assumptions.medianTaxRate ?? saneTaxRateOrDefault(sourceTaxRate);
        const afterTaxFactor = 1 - taxRate;
        const originalAdjRaw = parseMoney(metricEntry(data, 'adj_income')) || parseMoney(data.adj_income);
        const impliedRevenueRaw = originalAdjRaw && originalMargin
            ? Math.abs(originalAdjRaw / originalMargin)
            : 0;
        const revenueRaw = parseMoney(metricEntry(data, 'revenue'))
            || parseMoney(data.revenue)
            || statementRevenueRaw(data)
            || impliedRevenueRaw;
        const cyRevenueBaseRaw = lastYearRevenueRaw(data) || revenueRaw;
        const valuationRaw = parseMoney(metricEntry(data, 'ev'))
            || parseMoney(data.ev)
            || parseMoney(metricEntry(data, 'derivedEnterpriseValue'))
            || parseMoney(data.derivedEnterpriseValue)
            || parseMoney(metricEntry(data, 'marketCap'))
            || parseMoney(data.marketCap);
        const grossPpeRaw = parseMoney(metricEntry(data, 'grossPpe'));
        const investmentCapexRaw = parseMoney(metricEntry(data, 'investmentCapex'));
        const rocDenomRaw = parseMoney(metricEntry(data, 'netWorkingCapital')) + parseMoney(metricEntry(data, 'netFixedAssets'));
        const baseAdjRaw = originalAdjRaw || (revenueRaw * margin);
        const pretaxAdjRaw = assumptions.margin !== undefined ? revenueRaw * margin : baseAdjRaw;
        const afterTaxAdjRaw = pretaxAdjRaw && afterTaxFactor > 0 ? pretaxAdjRaw * afterTaxFactor : 0;

        if (assumptions.margin !== undefined) {
            setMetric(data, 'margin', margin, formatPercentDecimal(margin), 'percent');
            setMetric(data, 'adj_income', pretaxAdjRaw, formatMoneyFront(pretaxAdjRaw), 'money');
            setMetric(data, 'afterTaxAdjIncome', afterTaxAdjRaw, afterTaxAdjRaw ? formatMoneyFront(afterTaxAdjRaw) : '--', 'money');
            setMetric(data, 'adjEbitGrossPpe', grossPpeRaw && afterTaxAdjRaw ? afterTaxAdjRaw / grossPpeRaw : null, grossPpeRaw && afterTaxAdjRaw ? formatPercentDecimal(afterTaxAdjRaw / grossPpeRaw) : '--', 'percent');
            setMetric(data, 'capexAdjIncome', pretaxAdjRaw ? investmentCapexRaw / pretaxAdjRaw : null, pretaxAdjRaw ? formatPercentDecimal(investmentCapexRaw / pretaxAdjRaw) : '--', 'percent');
            setMetric(data, 'roc', rocDenomRaw && afterTaxAdjRaw ? afterTaxAdjRaw / rocDenomRaw : null, rocDenomRaw && afterTaxAdjRaw ? formatPercentDecimal(afterTaxAdjRaw / rocDenomRaw) : '--', 'percent');
        }

        if (assumptions.cy_growth !== undefined) setMetric(data, 'cy_growth', cyGrowth, formatPercentDecimal(cyGrowth), 'percent');
        if (assumptions.ny_growth !== undefined) setMetric(data, 'ny_growth', nyGrowth, formatPercentDecimal(nyGrowth), 'percent');
        if (assumptions.medianTaxRate !== undefined || (sourceTaxRate && sourceTaxRate !== taxRate)) setMetric(data, 'medianTaxRate', taxRate, formatPercentDecimal(taxRate), 'percent');

        const existingCyRevenueRaw = parseMoney(metricEntry(data, 'cy_revenue'));
        const existingNyRevenueRaw = parseMoney(metricEntry(data, 'ny_revenue'));
        const cyRevenueRaw = assumptions.cy_growth !== undefined && cyRevenueBaseRaw
            ? cyRevenueBaseRaw * (1 + cyGrowth)
            : existingCyRevenueRaw || (cyRevenueBaseRaw ? cyRevenueBaseRaw * (1 + cyGrowth) : 0);
        const nyRevenueRaw = assumptions.ny_growth !== undefined && cyRevenueRaw
            ? cyRevenueRaw * (1 + nyGrowth)
            : existingNyRevenueRaw || (cyRevenueRaw ? cyRevenueRaw * (1 + nyGrowth) : 0);
        const cyAdjRaw = cyRevenueRaw * margin;
        const nyAdjRaw = nyRevenueRaw * margin;
        const cyAfterTaxAdjRaw = cyAdjRaw && afterTaxFactor > 0 ? cyAdjRaw * afterTaxFactor : 0;
        const nyAfterTaxAdjRaw = nyAdjRaw && afterTaxFactor > 0 ? nyAdjRaw * afterTaxFactor : 0;
        const cyDiscount = forwardDiscountInfo(data, 'cy');
        const nyDiscount = forwardDiscountInfo(data, 'ny');
        const cyDiscountedMultiple = discountedForwardMultiple(valuationRaw, cyAfterTaxAdjRaw, cyDiscount.factor);
        const nyDiscountedMultiple = discountedForwardMultiple(valuationRaw, nyAfterTaxAdjRaw, nyDiscount.factor);
        setMetric(data, 'ev_adj_ebit', valuationRaw && afterTaxAdjRaw ? valuationRaw / afterTaxAdjRaw : null, valuationRaw && afterTaxAdjRaw ? formatRatio(valuationRaw / afterTaxAdjRaw) : '--', 'ratio');
        if (cyDiscountedMultiple !== null) {
            setMetric(data, 'ev_cy_ebit', cyDiscountedMultiple, formatRatio(cyDiscountedMultiple), 'ratio');
        }
        if (nyDiscountedMultiple !== null) {
            setMetric(data, 'ev_ny_ebit', nyDiscountedMultiple, formatRatio(nyDiscountedMultiple), 'ratio');
        }
        if (assumptions.margin !== undefined || assumptions.cy_growth !== undefined) {
            setMetric(data, 'cy_revenue', cyRevenueRaw, formatMoneyFront(cyRevenueRaw), 'money');
            setMetric(data, 'cy_adj_inc', cyAdjRaw, formatMoneyFront(cyAdjRaw), 'money');
            setMetric(data, 'ev_cy_ebit', cyDiscountedMultiple, cyDiscountedMultiple !== null ? formatRatio(cyDiscountedMultiple) : '--', 'ratio');
        }
        if (assumptions.margin !== undefined || assumptions.cy_growth !== undefined || assumptions.ny_growth !== undefined) {
            setMetric(data, 'ny_revenue', nyRevenueRaw, formatMoneyFront(nyRevenueRaw), 'money');
            setMetric(data, 'ny_adj_inc', nyAdjRaw, formatMoneyFront(nyAdjRaw), 'money');
            setMetric(data, 'ev_ny_ebit', nyDiscountedMultiple, nyDiscountedMultiple !== null ? formatRatio(nyDiscountedMultiple) : '--', 'ratio');
        }
        if (!afterTaxAdjRaw) {
            setMetric(data, 'ev_adj_ebit', null, '--', 'ratio');
            setMetric(data, 'ev_cy_ebit', null, '--', 'ratio');
            setMetric(data, 'ev_ny_ebit', null, '--', 'ratio');
        }
        return data;
    }

    function discountedForwardMultiple(valuationRaw, forwardIncomeRaw, discountFactor) {
        if (!valuationRaw || !forwardIncomeRaw || !discountFactor) return null;
        return (valuationRaw / forwardIncomeRaw) * discountFactor;
    }

    function commitAssumptionInput(input) {
        const key = input.dataset.editAssumption;
        const ticker = (state.latest?.ticker || '').toUpperCase();
        if (!ticker) return;
        const entered = input.value.trim();
        const original = input.dataset.editingOriginalValue || input.dataset.originalValue || '';
        state.assumptions[ticker] = state.assumptions[ticker] || {};
        if (entered === '') {
            input.value = original;
            return;
        } else {
            const parsed = Number(entered.replace('%', ''));
            if (!Number.isFinite(parsed)) {
                input.value = original;
                return;
            }
            state.assumptions[ticker][key] = parsed / 100;
        }
        if (!Object.keys(state.assumptions[ticker]).length) delete state.assumptions[ticker];
        renderStats(state.latest);
    }

    function resetAssumption(key) {
        const ticker = (state.latest?.ticker || '').toUpperCase();
        if (!ticker || !key || !state.assumptions[ticker]) return;
        delete state.assumptions[ticker][key];
        if (!Object.keys(state.assumptions[ticker]).length) delete state.assumptions[ticker];
        renderStats(state.latest);
    }

    function dataromaValue(value) {
        return value === undefined || value === null || value === '' ? '--' : escapeAttr(value);
    }

    function renderDataromaCards(data) {
        const dataroma = data.dataroma || {};
        if (!dataroma || !Object.keys(dataroma).length) return '';
        const buys = dataroma.insiderBuys || {};
        const sells = dataroma.insiderSells || {};
        return `<section class="analyst-grid dataroma-grid">
            <div class="metric-group dataroma-card">
                <div class="section-title-row">
                    <h3>Super Investor Stats</h3>
                </div>
                <div class="dataroma-list">
                    <div><span>Ownership Count</span><strong>${dataromaValue(dataroma.ownershipCount)}</strong></div>
                    <div><span>Ownership Rank</span><strong>${dataromaValue(dataroma.ownershipRank)}</strong></div>
                    <div><span>% of Portfolios</span><strong>${dataromaValue(dataroma.portfolioPercent)}</strong></div>
                    <div><span>Hold Price</span><strong>${dataromaValue(dataroma.holdPrice)}</strong></div>
                </div>
            </div>
            <div class="metric-group dataroma-card">
                <div class="section-title-row">
                    <h3>Insider Activity</h3>
                </div>
                <div class="dataroma-insiders">
                    <div class="dataroma-insider-head"><span></span><span>Transactions</span><span>Total</span></div>
                    <div class="dataroma-insider-row buys"><span>Buys</span><strong>${dataromaValue(buys.transactions)}</strong><strong>${dataromaValue(buys.total)}</strong></div>
                    <div class="dataroma-insider-row sells"><span>Sells</span><strong>${dataromaValue(sells.transactions)}</strong><strong>${dataromaValue(sells.total)}</strong></div>
                </div>
            </div>
        </section>`;
    }

    function renderAnalystCards(data) {
        const val = (key) => metricDisplay(data, key);
        const rec = data.analystRecommendations || {};
        const counts = [
            ['Strong Buy', rec.strongBuy || 0, 'strong-buy', 5],
            ['Buy', rec.buy || 0, 'buy', 4],
            ['Hold', rec.hold || 0, 'hold', 3],
            ['Sell', rec.sell || 0, 'sell', 2],
            ['Strong Sell', rec.strongSell || 0, 'strong-sell', 1],
        ];
        const total = counts.reduce((sum, item) => sum + Number(item[1] || 0), 0);
        const countWeightedRating = total
            ? counts.reduce((sum, [, count, , stars]) => sum + Number(count || 0) * stars, 0) / total
            : null;
        const rating = countWeightedRating !== null
            ? countWeightedRating.toFixed(1)
            : data.recommendationMean && data.recommendationMean !== '--'
                ? Math.max(0, 6 - Number(data.recommendationMean)).toFixed(1)
                : '--';
        return `<section class="analyst-grid">
            <div class="metric-group analyst-card">
                <h3>Analyst Price Target</h3>
                <div class="target-cases">
                    ${caseButton('Bear', val('targetLowPrice'), val('currentPrice'))}
                    ${caseButton('Base', val('targetMeanPrice'), val('currentPrice'))}
                    ${caseButton('Bull', val('targetHighPrice'), val('currentPrice'))}
                </div>
            </div>
            <div class="metric-group analyst-card">
                <h3>Analyst Recommendations</h3>
                <div class="rec-summary">${rating}/5 stars</div>
                <div class="rec-grid">${counts.map(([label, count, tone]) => {
            const pct = total ? `${Math.round((count / total) * 100)}%` : '0%';
            return `<div class="rec-pill rec-${tone}"><strong>${count}</strong><span>${label}</span><small>${pct}</small></div>`;
        }).join('')}</div>
            </div>
        </section>`;
    }

    function caseButton(label, target, current) {
        const targetRaw = Number(target);
        const currentRaw = Number(current);
        const move = targetRaw && currentRaw ? `${((targetRaw / currentRaw - 1) * 100).toFixed(1)}%` : '--';
        return `<button class="case-btn case-${label.toLowerCase()}" type="button" data-case="${label.toLowerCase()}"><span>${label}</span><strong>${move}</strong></button>`;
    }

    async function fetchTicker(ticker, refresh = false) {
        delete state.assumptions[ticker];
        const started = performance.now();
        const url = `/api/short-interest/${ticker}${refresh ? '?refresh=1' : ''}`;
        const response = await fetch(url);
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Failed to fetch data');
        const clientSeconds = (performance.now() - started) / 1000;
        data.fetchTime = `${clientSeconds.toFixed(2)}s`;
        data.fetchTiming = {
            ...(data.fetchTiming || {}),
            clientSeconds,
        };
        state.dataByTicker[ticker] = data;
        saveTickerData();
        return data;
    }

    function resetStatementDefaultsForTicker(data, fallbackTicker) {
        const nextTicker = String(data?.ticker || fallbackTicker || '').toUpperCase();
        if (!nextTicker || nextTicker === state.loadedTicker) return;
        state.loadedTicker = nextTicker;
        state.periodicity = 'annual';
        state.statementTab = 'starred';
        state.quarterlyGrowthMode = 'yoy';
        state.statementSearch = '';
        localStorage.setItem('stock_periodicity', state.periodicity);
        localStorage.setItem('stock_statement_tab', state.statementTab);
        localStorage.setItem('stock_quarterly_growth_mode', state.quarterlyGrowthMode);
    }

    function renderTickerResult(data, fallbackTicker) {
        const ticker = data.ticker || fallbackTicker;
        resetStatementDefaultsForTicker(data, fallbackTicker);
        state.latest = data;
        $('result-stats').classList.remove('hidden');
        $('result-ticker').textContent = ticker;
        const title = $('result-ticker').parentElement;
        if (title && !title.querySelector('.company-name')) {
            $('result-ticker').insertAdjacentHTML('afterend', '<div class="company-name"></div>');
        }
        const company = title ? title.querySelector('.company-name') : null;
        if (company) company.textContent = data.companyName || '--';
        $('result-data-date').textContent = displayDate(data);
        setFetchInfoText(displayFetchInfo(data), false);
        $('result-fetch-info').title = 'Click to see fetch timing details';
        $('result-currency-info').textContent = displayCurrency(data);
        updateResultStarButton(ticker);
        renderStats(data);
        renderStatements(data);
    }

    async function scanTicker(ticker, refresh = false) {
        if (!ticker) return;
        ticker = ticker.toUpperCase();
        const requestId = state.scanRequestId + 1;
        state.scanRequestId = requestId;
        const fetchStartedAt = performance.now();
        showView('scanner');
        $('result-container').classList.remove('hidden');
        $('result-stats').classList.add('hidden');
        $('statement-panel').classList.add('hidden');
        state.latest = null;
        $('result-ticker').textContent = ticker;
        const title = $('result-ticker').parentElement;
        const company = title ? title.querySelector('.company-name') : null;
        if (company) company.textContent = '';
        $('result-data-date').textContent = '--';
        setFetchInfoText('Fetching: 0.00s • Fetches: --', true);
        startFetchTimer(fetchStartedAt, requestId);
        $('result-currency-info').textContent = 'Native currency: -- • USD rate: --';
        $('glass-card').classList.remove('refreshing');
        $('loading-spinner').classList.remove('hidden');
        $('error-message').classList.add('hidden');
        $('glass-card').style.display = 'block';
        incrementSearch(ticker);

        try {
            const data = await fetchTicker(ticker, refresh);
            if (requestId !== state.scanRequestId) return;
            stopFetchTimer();
            state.latest = data;
            $('loading-spinner').classList.add('hidden');
            $('glass-card').classList.remove('refreshing');
            renderTickerResult(data, ticker);
        } catch (err) {
            if (requestId !== state.scanRequestId) return;
            stopFetchTimer();
            const elapsed = (performance.now() - fetchStartedAt) / 1000;
            setFetchInfoText(`Fetch failed after ${elapsed.toFixed(2)}s • Fetches: --`, false);
            $('loading-spinner').classList.add('hidden');
            $('glass-card').classList.remove('refreshing');
            $('error-message').textContent = err.message;
            $('error-message').classList.remove('hidden');
        }
    }

    function incrementSearch(ticker) {
        state.most[ticker] = (state.most[ticker] || 0) + 1;
        save('stock_search_counts', state.most);
    }

    $('search-form').addEventListener('submit', async (event) => {
        event.preventDefault();
        const ticker = $('ticker-input').value.trim().toUpperCase();
        $('ticker-input').value = '';
        await scanTicker(ticker);
    });

    $('refresh-data-btn').addEventListener('click', () => {
        const ticker = ($('result-ticker').textContent || '').trim().toUpperCase();
        if (ticker && ticker !== '--') scanTicker(ticker, true);
    });

    function updateResultStarButton(ticker) {
        const btn = $('result-star-btn');
        if (!btn) return;
        const isStarred = state.starred.includes(ticker);
        btn.textContent = isStarred ? 'Starred' : 'Star';
        btn.classList.toggle('active', isStarred);
    }

    $('result-star-btn').addEventListener('click', () => {
        const ticker = ($('result-ticker').textContent || '').trim().toUpperCase();
        if (!ticker || ticker === '--') return;
        toggleStarredTicker(ticker);
        updateResultStarButton(ticker);
    });

    function toggleStarredTicker(ticker) {
        if (state.starred.includes(ticker)) {
            state.starred = state.starred.filter((item) => item !== ticker);
        } else {
            state.starred.push(ticker);
        }
        save('stock_starred_tickers', state.starred);
    }

    function sortIcon(key, kind) {
        const sort = state.sort[kind] || {};
        const active = sort.key === key;
        const direction = active ? sort.direction : '';
        const label = active
            ? `Sorted ${direction === 'asc' ? 'ascending' : 'descending'}`
            : 'Sortable column';
        return `<span class="sort-icon ${active ? 'active' : ''}" data-direction="${direction}" aria-hidden="true"></span><span class="sr-only">${label}</span>`;
    }

    function sortableHeader(key, label, kind) {
        const sort = state.sort[kind] || {};
        const active = sort.key === key;
        const direction = active ? sort.direction : '';
        const ariaSort = active ? (direction === 'asc' ? 'ascending' : 'descending') : 'none';
        return `<th data-sort="${key}" aria-sort="${ariaSort}"><span class="sort-label">${label}</span>${sortIcon(key, kind)}</th>`;
    }

    function tableHeaders(kind = 'watchlist') {
        return `<tr>
            ${sortableHeader('ticker', 'Ticker', kind)}${sortableHeader('margin', 'Adj Margin', kind)}
            ${sortableHeader('grossMargin', 'Gross Margin', kind)}${sortableHeader('cy_growth', 'CY Growth', kind)}
            ${sortableHeader('ny_growth', 'NY Growth', kind)}${sortableHeader('shortFloat', 'Short % SO', kind)}
            ${sortableHeader('ev_adj_ebit', 'EV/Adj Inc', kind)}${sortableHeader('ev_cy_ebit', 'EV/CY Adj Inc', kind)}
            ${sortableHeader('ev_ny_ebit', 'EV/NY Adj Inc', kind)}<th>Actions</th>
        </tr>`;
    }

    function renderTickerTable(kind) {
        const list = kind === 'watchlist' ? state.watchlist : state.groups;
        const body = kind === 'watchlist' ? $('watchlist-body') : $('groups-body');
        const head = kind === 'watchlist' ? document.querySelector('#watchlist-table thead') : $('groups-head');
        if (head) head.innerHTML = tableHeaders(kind);
        if (!list.length) {
            body.innerHTML = `<tr><td colspan="10">No tickers yet.</td></tr>`;
            return;
        }
        body.innerHTML = sortedTickers(list, kind).map((ticker) => tableRow(ticker, state.dataByTicker[ticker], kind)).join('');
    }

    function sortedTickers(list, kind) {
        const sort = state.sort[kind];
        if (!sort || !sort.key || sort.key === 'ticker') {
            const sorted = [...list].sort();
            return sort && sort.direction === 'desc' ? sorted.reverse() : sorted;
        }
        return [...list].sort((a, b) => {
            const avData = state.dataByTicker[a] ? applyAssumptions(state.dataByTicker[a]) : null;
            const bvData = state.dataByTicker[b] ? applyAssumptions(state.dataByTicker[b]) : null;
            const av = sortableValue(metricEntry(avData, sort.key));
            const bv = sortableValue(metricEntry(bvData, sort.key));
            return sort.direction === 'asc' ? av - bv : bv - av;
        });
    }

    function sortableValue(value) {
        if (value === null || value === undefined || value === '' || value === '--') return Number.NEGATIVE_INFINITY;
        if (value && typeof value === 'object' && 'raw' in value) {
            const raw = Number(value.raw);
            return Number.isFinite(raw) ? raw : Number.NEGATIVE_INFINITY;
        }
        let text = String(value).replace(/,/g, '').replace('%', '');
        let mult = 1;
        if (text.endsWith('T')) { mult = 1e12; text = text.slice(0, -1); }
        if (text.endsWith('B')) { mult = 1e9; text = text.slice(0, -1); }
        if (text.endsWith('M')) { mult = 1e6; text = text.slice(0, -1); }
        const parsed = Number(text);
        return Number.isFinite(parsed) ? parsed * mult : Number.NEGATIVE_INFINITY;
    }

    function tableRow(ticker, data, kind) {
        if (!data) return `<tr id="${kind}-row-${ticker}"><td>${ticker}</td><td colspan="8">No cached data. Use Refresh.</td><td>${actionButtons(ticker, kind)}</td></tr>`;
        data = applyAssumptions(data);
        return `<tr id="${kind}-row-${ticker}">
            <td>${ticker}</td><td>${metricDisplay(data, 'margin')}</td><td>${metricDisplay(data, 'grossMargin')}</td>
            <td>${metricDisplay(data, 'cy_growth')}</td><td>${metricDisplay(data, 'ny_growth')}</td>
            <td>${metricDisplay(data, 'shortFloat')}</td><td>${metricDisplay(data, 'ev_adj_ebit')}</td>
            <td>${metricDisplay(data, 'ev_cy_ebit')}</td><td>${metricDisplay(data, 'ev_ny_ebit')}</td>
            <td>${actionButtons(ticker, kind)}</td>
        </tr>`;
    }

    function actionButtons(ticker, kind) {
        return `<button class="scan-btn" type="button" data-scan="${ticker}">Scan</button>
            <button class="scan-btn" type="button" data-refresh-row="${ticker}" data-kind="${kind}">Refresh</button>
            <button class="remove-btn" type="button" data-remove="${ticker}" data-kind="${kind}">Remove</button>`;
    }

    document.body.addEventListener('click', (event) => {
        const scan = event.target.closest('[data-scan]');
        if (scan) scanTicker(scan.dataset.scan);
        const rowRefresh = event.target.closest('[data-refresh-row]');
        if (rowRefresh) refreshTableTicker(rowRefresh.dataset.refreshRow, rowRefresh.dataset.kind);
        const sortHeader = event.target.closest('[data-sort]');
        if (sortHeader) toggleSort(sortHeader.dataset.sort, sortHeader.closest('table'));
        const remove = event.target.closest('[data-remove]');
        if (remove) {
            const kind = remove.dataset.kind;
            const ticker = remove.dataset.remove;
            if (kind === 'watchlist') {
                state.watchlist = state.watchlist.filter((item) => item !== ticker);
                save('stock_watchlist', state.watchlist);
                renderTickerTable('watchlist');
            } else if (kind === 'groups') {
                state.groups = state.groups.filter((item) => item !== ticker);
                renderTickerTable('groups');
            } else if (kind === 'starred') {
                toggleStarredTicker(ticker);
                renderStarredTickers();
            }
        }
        const star = event.target.closest('[data-star-account]');
        if (star) toggleStarredAccount(star.dataset.statement, star.dataset.starAccount);
        const toggle = event.target.closest('[data-toggle-ratio]');
        if (toggle) toggleStatementRatio(toggle.dataset.statement, toggle.dataset.toggleRatio, toggle.dataset.label);
        const periodicityBtn = event.target.closest('[data-periodicity]');
        if (periodicityBtn) {
            state.periodicity = periodicityBtn.dataset.periodicity;
            localStorage.setItem('stock_periodicity', state.periodicity);
            renderStatements(state.latest);
        }
        const growthModeBtn = event.target.closest('[data-quarterly-growth-mode]');
        if (growthModeBtn) {
            state.quarterlyGrowthMode = growthModeBtn.dataset.quarterlyGrowthMode;
            localStorage.setItem('stock_quarterly_growth_mode', state.quarterlyGrowthMode);
            renderStatements(state.latest);
        }
        const statement = event.target.closest('[data-statement-tab]');
        if (statement) {
            state.statementTab = statement.dataset.statementTab;
            localStorage.setItem('stock_statement_tab', state.statementTab);
            renderStatements(state.latest);
        }
    });

    document.body.addEventListener('input', (event) => {
        const statementSearch = event.target.closest('[data-statement-search]');
        if (!statementSearch) return;
        state.statementSearch = statementSearch.value;
        const results = $('statement-results');
        if (results) results.innerHTML = renderStatementResults(state.latest);
    });

    function bindListForm(formId, inputId, listName, storageKey) {
        const form = $(formId);
        if (!form) return;
        form.addEventListener('submit', (event) => {
            event.preventDefault();
            const ticker = $(inputId).value.trim().toUpperCase();
            $(inputId).value = '';
            if (!ticker || state[listName].includes(ticker)) return;
            state[listName].push(ticker);
            if (storageKey) save(storageKey, state[listName]);
            if (listName === 'starred') renderStarredTickers();
            else renderTickerTable(listName);
        });
    }

    async function refreshTableTicker(ticker, kind) {
        const row = $(`${kind}-row-${ticker}`);
        if (row) row.innerHTML = `<td>${ticker}</td><td colspan="8">Refreshing...</td><td>${actionButtons(ticker, kind)}</td>`;
        try {
            const data = await fetchTicker(ticker, true);
            const refreshed = $(`${kind}-row-${ticker}`);
            if (refreshed) refreshed.outerHTML = tableRow(ticker, data, kind);
        } catch {
            const failed = $(`${kind}-row-${ticker}`);
            if (failed) failed.innerHTML = `<td>${ticker}</td><td colspan="8">Refresh failed.</td><td>${actionButtons(ticker, kind)}</td>`;
        }
    }

    function toggleSort(key, table) {
        const kind = table && table.id === 'watchlist-table' ? 'watchlist' : 'groups';
        const current = state.sort[kind] || {};
        state.sort[kind] = {
            key,
            direction: current.key === key && current.direction === 'asc' ? 'desc' : 'asc',
        };
        renderTickerTable(kind);
    }
    bindListForm('watchlist-form', 'watchlist-ticker-input', 'watchlist', 'stock_watchlist');
    bindListForm('groups-form', 'groups-ticker-input', 'groups', '');
    bindListForm('starred-form', 'starred-ticker-input', 'starred', 'stock_starred_tickers');

    function renderStarredTickers() {
        const body = $('starred-body');
        body.innerHTML = state.starred.length
            ? state.starred.map((ticker) => `<tr><td>${ticker}</td><td>${actionButtons(ticker, 'starred')}</td></tr>`).join('')
            : '<tr><td colspan="2">No starred tickers yet.</td></tr>';
    }

    function renderMostSearched() {
        const rows = Object.entries(state.most).sort((a, b) => b[1] - a[1]);
        $('most-searched-body').innerHTML = rows.length
            ? rows.map(([ticker, count]) => `<tr><td>${ticker}</td><td>${count}</td><td><button class="scan-btn" data-scan="${ticker}">Scan</button></td></tr>`).join('')
            : '<tr><td colspan="3">No searches yet.</td></tr>';
    }

    function renderStatements(data) {
        const panel = $('statement-panel');
        if (!data) return;
        panel.classList.remove('hidden');
        const tabs = [
            ['income', 'Income Statement'],
            ['balance', 'Balance Sheet'],
            ['cash', 'Cash Flow'],
            ['ratios', 'Ratios'],
            ['all', 'All Accounts'],
            ['starred', 'Starred'],
        ];
        const activeTab = tabs.find(t => t[0] === state.statementTab) || tabs[0];
        panel.innerHTML = `<div class="statement-header">
            <div class="statement-heading-row">
                <div>
                    <h2>${state.statementTab === 'starred' ? 'Starred Statements' : activeTab[1]}</h2>
                    <p>${state.periodicity === 'annual' ? 'Annual' : 'Quarterly'} figures shown in USD-normalized values</p>
                </div>
                <div class="statement-period-actions">
                        <button class="mini-btn ${state.periodicity === 'annual' ? 'on blue' : ''}" data-periodicity="annual">Annual</button>
                        <button class="mini-btn ${state.periodicity === 'quarterly' ? 'on blue' : ''}" data-periodicity="quarterly">Quarterly</button>
                        <span class="quarterly-growth-controls ${state.periodicity === 'quarterly' ? '' : 'is-hidden'}" aria-hidden="${state.periodicity === 'quarterly' ? 'false' : 'true'}">
                            <button class="mini-btn ${state.quarterlyGrowthMode === 'yoy' ? 'on blue' : ''}" data-quarterly-growth-mode="yoy">YoY</button>
                            <button class="mini-btn ${state.quarterlyGrowthMode === 'qoq' ? 'on blue' : ''}" data-quarterly-growth-mode="qoq">QoQ</button>
                        </span>
                </div>
            </div>
            <div class="statement-toolbar">
                <label class="statement-search">
                    <span class="sr-only">Search statement line items</span>
                    <input type="search" value="${escapeAttr(state.statementSearch)}" placeholder="Search line items" data-statement-search autocomplete="off">
                </label>
                <div class="statement-tabs">${tabs.map(([key, label]) => `<button class="tab-btn ${state.statementTab === key ? 'active' : ''}" data-statement-tab="${key}">${label}</button>`).join('')}</div>
            </div>
        </div>
        <div id="statement-results">${renderStatementResults(data)}</div>`;
    }

    function renderStatementResults(data) {
        if (!data) return '';
        if (state.statementTab === 'starred') return renderStarredStatementTable(data);
        if (state.statementTab === 'all') return renderAllStatementTable(data);
        return renderStatementTable(statementForTab(data, state.statementTab), state.statementTab);
    }

    function statementForTab(data, tab) {
        let stmt = {};
        if (tab === 'balance') stmt = data.balanceStatement || {};
        else if (tab === 'cash') stmt = data.cashFlowStatement || {};
        else if (tab === 'ratios') return buildRatiosStatement(data);
        else stmt = data.incomeStatement || {};
        return stmt[state.periodicity || 'annual'] || {};
    }

    function buildRatiosStatement(data) {
        const periodicity = state.periodicity || 'annual';
        const income = ((data || {}).incomeStatement || {})[periodicity] || {};
        const balance = ((data || {}).balanceStatement || {})[periodicity] || {};
        const periods = income.periods || balance.periods || [];
        return {
            periods,
            rows: [
                {
                    label: 'ROA',
                    values: periods.map((period) => {
                        const netIncome = statementValueForPeriod(income, [
                            'Net Income',
                            'Net Income Common Stockholders',
                            'Net Income Applicable To Common Shares',
                            'Net Income Continuous Operations',
                        ], period);
                        const assetPeriod = String(period || '').toUpperCase() === 'TTM' ? firstAvailablePeriod(balance, ['MRQ', 'LATEST', 'TTM']) : period;
                        const totalAssets = statementValueForPeriod(balance, ['Total Assets'], assetPeriod);
                        const netIncomeRaw = parseMoney(netIncome);
                        const totalAssetsRaw = parseMoney(totalAssets);
                        return totalAssetsRaw ? formatPercentDecimal(netIncomeRaw / totalAssetsRaw) : '--';
                    }),
                },
                {
                    label: 'ROIC',
                    values: periods.map((period) => {
                        const investedCapitalPeriod = String(period || '').toUpperCase() === 'TTM' ? firstAvailablePeriod(balance, ['MRQ', 'LATEST', 'TTM']) : period;
                        const operatingIncome = statementValueForPeriod(income, [
                            'Operating Income',
                            'Operating Income Loss',
                        ], period);
                        const taxRate = statementValueForPeriod(income, ['Tax Rate', 'Tax Rate For Calcs'], period);
                        const totalDebt = statementValueForPeriod(balance, ['Total Debt'], investedCapitalPeriod);
                        const equity = statementValueForPeriod(balance, [
                            'Stockholders Equity',
                            'Common Stock Equity',
                            'Total Equity Gross Minority Interest',
                        ], investedCapitalPeriod);
                        const cash = statementValueForPeriod(balance, [
                            'Cash, Equivalents & Short Term Investments',
                            'Cash And Cash Equivalents',
                            'Cash & Cash Equivalents',
                            'Cash Cash Equivalents And Short Term Investments',
                        ], investedCapitalPeriod);
                        const afterTaxOperatingIncome = parseMoney(operatingIncome) * (1 - parsePercentValue(taxRate));
                        const investedCapital = parseMoney(totalDebt) + parseMoney(equity) - parseMoney(cash);
                        return investedCapital ? formatPercentDecimal(afterTaxOperatingIncome / investedCapital) : '--';
                    }),
                },
                {
                    label: 'Adj ROIC',
                    values: periods.map((period) => {
                        const investedCapitalPeriod = String(period || '').toUpperCase() === 'TTM' ? firstAvailablePeriod(balance, ['MRQ', 'LATEST', 'TTM']) : period;
                        const adjustedNetIncome = statementValueForPeriod(income, ['Adjusted Net Income'], period);
                        const totalDebt = statementValueForPeriod(balance, ['Total Debt'], investedCapitalPeriod);
                        const equity = statementValueForPeriod(balance, [
                            'Stockholders Equity',
                            'Common Stock Equity',
                            'Total Equity Gross Minority Interest',
                        ], investedCapitalPeriod);
                        const cash = statementValueForPeriod(balance, [
                            'Cash, Equivalents & Short Term Investments',
                            'Cash And Cash Equivalents',
                            'Cash & Cash Equivalents',
                            'Cash Cash Equivalents And Short Term Investments',
                        ], investedCapitalPeriod);
                        const investedCapital = parseMoney(totalDebt) + parseMoney(equity) - parseMoney(cash);
                        return investedCapital ? formatPercentDecimal(parseMoney(adjustedNetIncome) / investedCapital) : '--';
                    }),
                },
            ],
        };
    }

    function firstAvailablePeriod(statement, candidates) {
        const periods = statement?.periods || [];
        const upperPeriods = periods.map((period) => String(period || '').toUpperCase());
        for (const candidate of candidates) {
            const idx = upperPeriods.indexOf(String(candidate || '').toUpperCase());
            if (idx >= 0) return periods[idx];
        }
        return periods[0] || '';
    }

    function statementValueForPeriod(statement, labels, period) {
        const periods = statement?.periods || [];
        const idx = periods.findIndex((candidate) => String(candidate || '').toUpperCase() === String(period || '').toUpperCase());
        if (idx < 0) return '--';
        const labelSet = new Set(labels.map((label) => label.toLowerCase()));
        const row = (statement.rows || []).find((candidate) => labelSet.has(String(candidate.label || '').toLowerCase()));
        return row ? (row.values || [])[idx] || '--' : '--';
    }

    function starredKey(statement, label) {
        return `${statement}:${label}`;
    }

    function toggleStarredAccount(statement, label) {
        const key = starredKey(statement, label);
        state.starredAccounts[key] = !state.starredAccounts[key];
        saveStarredAccounts();
        renderStatements(state.latest);
    }

    function toggleStatementRatio(statement, type, label) {
        const key = `${statement}:${type}:${label}`;
        state.statementToggles[key] = !state.statementToggles[key];
        save('stock_statement_toggles', state.statementToggles);
        renderStatements(state.latest);
    }

    function renderStarredStatementTable(data) {
        const p = state.periodicity || 'annual';
        const blocks = [
            ['income', 'Income Statement', (data.incomeStatement || {})[p] || {}],
            ['balance', 'Balance Sheet', (data.balanceStatement || {})[p] || {}],
            ['cash', 'Cash Flow Statement', (data.cashFlowStatement || {})[p] || {}],
            ['ratios', 'Ratios', buildRatiosStatement(data)],
        ].map(([key, label, statement]) => {
            const rows = (statement.rows || []).filter((row) => state.starredAccounts[starredKey(key, row.label)]);
            if (!rows.length) return '';
            return `<h3 class="statement-section-title">${label}</h3>${renderStatementTable({ periods: statement.periods, rows }, key, false)}`;
        }).join('');
        return blocks || '<p class="empty-note">Star accounts from a statement to show them here.</p>';
    }

    function renderAllStatementTable(data) {
        const p = state.periodicity || 'annual';
        const term = statementSearchTerm();
        const blocks = [
            ['income', 'Income Statement', (data.incomeStatement || {})[p] || {}],
            ['balance', 'Balance Sheet', (data.balanceStatement || {})[p] || {}],
            ['cash', 'Cash Flow Statement', (data.cashFlowStatement || {})[p] || {}],
            ['ratios', 'Ratios', buildRatiosStatement(data)],
        ].map(([key, label, statement]) => {
            const rows = term
                ? (statement.rows || []).filter((row) => accountLabelMatchesSearch(row.label, term))
                : (statement.rows || []);
            if (!rows.length) return '';
            return `<h3 class="statement-section-title">${label}</h3>${renderStatementTable({ periods: statement.periods, rows }, key, false)}`;
        }).join('');
        return blocks || `<p class="empty-note">${term ? 'No matching line items.' : 'No statement data available.'}</p>`;
    }

    function renderStatementTable(statement, statementKey, hideHeader = false) {
        statement = statementForDisplay(statement);
        const periods = statement.periods || [];
        const allRows = statement.rows || [];
        const rows = filterStatementRows(allRows);
        if (!rows.length) {
            return allRows.length && statementSearchTerm()
                ? '<p class="empty-note">No matching line items.</p>'
                : '<p class="empty-note">No statement data available.</p>';
        }
        return `<div class="statement-table-wrapper">
            <table class="statement-table">
                ${hideHeader ? '' : `<thead><tr><th>Actions</th><th>Line Item</th>${periods.map(p => `<th>${p}</th>`).join('')}</tr></thead>`}
                <tbody>${rows.map(row => renderStatementRow(row, periods, statementKey, statement)).join('')}</tbody>
            </table>
        </div>`;
    }

    function statementSearchTerm() {
        return String(state.statementSearch || '').trim().toLowerCase();
    }

    function filterStatementRows(rows) {
        const term = statementSearchTerm();
        if (!term) return rows;
        return rows.filter((row) => accountLabelMatchesSearch(row.label, term));
    }

    function accountLabelMatchesSearch(label, term) {
        const normalizedLabel = String(label || '').toLowerCase().trim();
        const normalizedTerm = String(term || '').toLowerCase().trim();
        if (!normalizedTerm) return true;
        if (normalizedLabel.startsWith(normalizedTerm)) return true;
        const words = normalizedLabel.match(/[a-z0-9]+/g) || [];
        const tokens = normalizedTerm.match(/[a-z0-9]+/g) || [];
        if (!tokens.length) return true;
        return tokens.every((token) => words.some((word) => word.startsWith(token)));
    }

    function statementForDisplay(statement) {
        const periods = statement.periods || [];
        const rows = statement.rows || [];
        const sortable = periods.map((period, idx) => ({ period, idx }));
        sortable.sort((a, b) => {
            const aSpecial = isSummaryPeriod(a.period);
            const bSpecial = isSummaryPeriod(b.period);
            if (aSpecial && bSpecial) return 0;
            if (aSpecial) return 1;
            if (bSpecial) return -1;
            return Date.parse(a.period) - Date.parse(b.period);
        });
        return {
            periods: sortable.map(item => item.period),
            rows: rows.map(row => ({
                ...row,
                values: sortable.map(item => (row.values || [])[item.idx] || '--'),
            })),
        };
    }

    function isSummaryPeriod(period) {
        return ['TTM', 'LATEST', 'MRQ'].includes(String(period || '').toUpperCase());
    }

    function renderStatementRow(row, periods, statementKey, displayStatement) {
        const canMargin = statementKey === 'income' || statementKey === 'cash' || statementKey === 'balance';
        const starred = state.starredAccounts[starredKey(statementKey, row.label)];
        const growthOn = state.statementToggles[`${statementKey}:growth:${row.label}`];
        const marginOn = state.statementToggles[`${statementKey}:margin:${row.label}`];
        const shouldHighlight = starred && state.statementTab !== 'starred';
        let html = `<tr${shouldHighlight ? ' class="starred-row"' : ''}><td class="statement-action-cell"><div class="statement-actions">
            <button class="mini-btn ${starred ? 'on gold' : ''}" data-statement="${statementKey}" data-star-account="${row.label}">${starred ? 'Starred' : 'Star'}</button>
            <button class="mini-btn ${growthOn ? 'on blue' : ''}" data-statement="${statementKey}" data-toggle-ratio="growth" data-label="${row.label}">Growth</button>
            ${canMargin ? `<button class="mini-btn ${marginOn ? 'on green' : ''}" data-statement="${statementKey}" data-toggle-ratio="margin" data-label="${row.label}">Margin</button>` : ''}
        </div></td><td class="statement-label-cell">${row.label}</td>${(row.values || []).map(value => `<td>${formatStatementValue(value)}</td>`).join('')}</tr>`;
        if (growthOn) html += ratioRow(growthRowLabel(), growthValues(row.values || [], periods, statementKey));
        if (marginOn) html += ratioRow('Margin', marginValues(row, periods, displayStatement, statementKey));
        return html;
    }

    function ratioRow(label, values) {
        return `<tr class="ratio-row"><td></td><td>${label}</td>${values.map(v => `<td>${v}</td>`).join('')}</tr>`;
    }

    function parsePercentBase(value) {
        if (!value || value === '--') return 0;
        if (value && typeof value === 'object' && 'raw' in value) {
            const raw = Number(value.raw);
            return Number.isFinite(raw) ? raw : 0;
        }
        let n = parseFloat(String(value).replace(/,/g, ''));
        if (String(value).includes('T')) n *= 1e12;
        if (String(value).includes('B')) n *= 1e9;
        if (String(value).includes('M')) n *= 1e6;
        return n || 0;
    }

    function parseMoney(value) {
        return parsePercentBase(value);
    }

    function parsePercentValue(value) {
        if (!value || value === '--') return 0;
        if (value && typeof value === 'object' && 'raw' in value) {
            const raw = Number(value.raw);
            return Number.isFinite(raw) ? raw : 0;
        }
        return Number(String(value).replace('%', '').replace('+', '')) / 100 || 0;
    }

    function formatRatio(value) {
        if (!Number.isFinite(value) || value === 0) return '--';
        return value >= 10 ? value.toFixed(1).replace(/\.0$/, '') : value.toFixed(2).replace(/0$/, '').replace(/\.$/, '');
    }

    function formatPercentDecimal(value) {
        if (!Number.isFinite(value)) return '--';
        return `${(value * 100).toFixed(1).replace(/\.0$/, '')}%`;
    }

    function formatMoneyFront(value) {
        if (!Number.isFinite(value)) return '--';
        const abs = Math.abs(value);
        const sign = value < 0 ? '-' : '';
        if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2).replace(/\.?0+$/, '')}T`;
        if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(1).replace(/\.0$/, '')}B`;
        if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(1).replace(/\.0$/, '')}M`;
        return `${sign}${abs.toFixed(2).replace(/\.?0+$/, '')}`;
    }

    function growthValues(values, periods, statementKey = '') {
        const lookback = state.periodicity === 'quarterly' && state.quarterlyGrowthMode === 'qoq' ? 1 : state.periodicity === 'quarterly' ? 4 : 1;
        return values.map((value, idx) => {
            if (idx < lookback) return '--';
            
            // Try to find the actual YoY index by period label if quarterly
            let prevIdx = idx - lookback;
            if (state.periodicity === 'quarterly' && state.quarterlyGrowthMode !== 'qoq') {
                const currentPeriod = periods[idx];
                const currentDate = Date.parse(currentPeriod);
                if (!isNaN(currentDate)) {
                    const targetDate = new Date(currentDate);
                    targetDate.setFullYear(targetDate.getFullYear() - 1);
                    const targetTime = targetDate.getTime();
                    
                    // Search for a period that matches this date
                    for (let i = idx - 1; i >= 0; i--) {
                        if (Math.abs(Date.parse(periods[i]) - targetTime) < 15 * 86400000) { // 15 day tolerance
                            prevIdx = i;
                            break;
                        }
                    }
                }
            }

            if (prevIdx < 0) return '--';
            const prev = parsePercentBase(values[prevIdx]);
            const curr = parsePercentBase(value);
            if (!prev || prev === 0 || !curr || curr === 0) return '--';
            if ((prev < 0 && curr > 0) || (prev > 0 && curr < 0)) return '--';
            const years = annualTtmGrowthYears(periods[idx], periods[prevIdx], statementKey);
            const growth = Math.pow(Math.abs(curr) / Math.abs(prev), 1 / years) - 1;
            return `${(growth * 100).toFixed(1)}%`;
        });
    }

    function annualTtmGrowthYears(currentPeriod, previousPeriod, statementKey = '') {
        if (state.periodicity !== 'annual' || String(currentPeriod || '').toUpperCase() !== 'TTM') return 1;
        const ttmEnd = latestQuarterlyPeriodForStatement(statementKey);
        const previousDate = Date.parse(previousPeriod);
        const ttmEndDate = Date.parse(ttmEnd);
        if (!ttmEnd || Number.isNaN(previousDate) || Number.isNaN(ttmEndDate) || ttmEndDate <= previousDate) return 1;
        return Math.max((ttmEndDate - previousDate) / (365.25 * 24 * 60 * 60 * 1000), 0.01);
    }

    function latestQuarterlyPeriodForStatement(statementKey = '') {
        const data = state.latest || {};
        let statement = data.incomeStatement;
        if (statementKey === 'cash') statement = data.cashFlowStatement;
        else if (statementKey === 'balance') statement = data.balanceStatement;
        else if (statementKey === 'ratios') statement = data.incomeStatement;
        const periods = ((statement || {}).quarterly || {}).periods || [];
        return periods.find((period) => !Number.isNaN(Date.parse(period))) || '';
    }

    function growthRowLabel() {
        if (state.periodicity !== 'quarterly') return 'Growth';
        return state.quarterlyGrowthMode === 'qoq' ? 'QoQ Growth' : 'YoY Growth';
    }

    function marginValues(row, periods, statement, statementKey = '') {
        const denominator = (statement.rows || []).find((r) => {
            if (statementKey === 'balance') return r.label === 'Total Assets';
            return r.label === 'Total Revenue' || r.label === 'Operating Cash Flow';
        });
        return (row.values || []).map((value, idx) => {
            const denom = denominator ? parsePercentBase(denominator.values[idx]) : 0;
            const num = parsePercentBase(value);
            return denom ? `${((num / denom) * 100).toFixed(1)}%` : '--';
        });
    }

    function latestStatementValue(statement, labels) {
        const labelSet = new Set(labels.map((label) => label.toLowerCase()));
        const rows = statement?.rows || [];
        const periods = statement?.periods || [];
        const latestIndex = periods.findIndex((period) => String(period).toUpperCase() === 'TTM' || String(period).toUpperCase() === 'LATEST');
        for (const row of rows) {
            if (!labelSet.has(String(row.label || '').toLowerCase())) continue;
            const values = row.values || [];
            if (latestIndex >= 0 && latestIndex < values.length && values[latestIndex] && values[latestIndex] !== '--') {
                return values[latestIndex];
            }
            for (let idx = values.length - 1; idx >= 0; idx -= 1) {
                if (values[idx] && values[idx] !== '--') return values[idx];
            }
        }
        return '--';
    }

    function latestStatementValueMatching(statement, matcher) {
        const rows = statement?.rows || [];
        const periods = statement?.periods || [];
        const latestIndex = periods.findIndex((period) => String(period).toUpperCase() === 'TTM' || String(period).toUpperCase() === 'LATEST');
        for (const row of rows) {
            if (!matcher(String(row.label || ''))) continue;
            const values = row.values || [];
            if (latestIndex >= 0 && latestIndex < values.length && values[latestIndex] && values[latestIndex] !== '--') {
                return values[latestIndex];
            }
            for (let idx = values.length - 1; idx >= 0; idx -= 1) {
                if (values[idx] && values[idx] !== '--') return values[idx];
            }
        }
        return '--';
    }

    function statementRevenueRaw(data) {
        const annual = (data.incomeStatement || {}).annual;
        return parseMoney(latestStatementValue(annual, ['Total Revenue']))
            || parseMoney(latestStatementValueMatching(annual, (label) => /revenue/i.test(label)));
    }

    function latestAnnualStatementValue(statement, labels) {
        const labelSet = new Set(labels.map((label) => label.toLowerCase()));
        const rows = statement?.rows || [];
        const periods = statement?.periods || [];
        for (const row of rows) {
            if (!labelSet.has(String(row.label || '').toLowerCase())) continue;
            const values = row.values || [];
            const annualIndex = periods.findIndex((period, idx) => !isSummaryPeriod(period) && values[idx] && values[idx] !== '--');
            if (annualIndex >= 0) return values[annualIndex];
            for (const value of values) {
                if (value && value !== '--') return value;
            }
        }
        return '--';
    }

    function lastYearRevenueRaw(data) {
        const annualRevenue = latestAnnualStatementValue((data.incomeStatement || {}).annual, ['Total Revenue']);
        const annualRaw = parseMoney(annualRevenue)
            || parseMoney(latestStatementValueMatching((data.incomeStatement || {}).annual, (label) => /revenue/i.test(label)));
        if (annualRaw) return annualRaw;

        const cyRevenueRaw = parseMoney(metricEntry(data, 'cy_revenue'));
        const cyGrowth = parsePercentValue(metricEntry(data, 'cy_growth'));
        if (cyRevenueRaw && cyGrowth > -1) return cyRevenueRaw / (1 + cyGrowth);
        return 0;
    }

    function latestAnnualPeriodDate(data) {
        const periods = ((data.incomeStatement || {}).annual || {}).periods || [];
        for (const period of periods) {
            if (isSummaryPeriod(period)) continue;
            const date = parseDateOnly(period);
            if (date) return date;
        }
        return null;
    }

    function parseDateOnly(value) {
        const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (!match) return null;
        const date = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
        return Number.isNaN(date.getTime()) ? null : date;
    }

    function payloadDate(data) {
        return parseDateOnly(data.pulledAt || data.dataDate) || new Date();
    }

    function addYears(date, years) {
        if (!date) return null;
        const next = new Date(date.getTime());
        next.setFullYear(next.getFullYear() + years);
        return next;
    }

    function forwardMetricDate(data, period) {
        const latestAnnual = latestAnnualPeriodDate(data);
        return addYears(latestAnnual, period === 'ny' ? 2 : 1);
    }

    function yearsBetween(start, end) {
        if (!start || !end) return 0;
        return Math.max(0, (end.getTime() - start.getTime()) / (365.25 * 24 * 60 * 60 * 1000));
    }

    function forwardDiscountInfo(data, period) {
        const date = forwardMetricDate(data, period);
        const years = yearsBetween(payloadDate(data), date);
        const factor = Math.pow(1 + FORWARD_DISCOUNT_RATE, years);
        return { date, years, factor };
    }

    function formatDateShort(date) {
        if (!date) return '--';
        const month = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][date.getMonth()];
        return `${month} ${date.getDate()}, ${date.getFullYear()}`;
    }

    function discountedIncomeDisplay(value, discountFactor) {
        const raw = parseMoney(value);
        return raw && discountFactor ? formatMoneyFront(raw / discountFactor) : '--';
    }

    function afterTaxIncomeDisplay(value, taxRate) {
        const raw = parseMoney(value);
        const factor = 1 - taxRate;
        return raw && factor > 0 ? formatMoneyFront(raw * factor) : '--';
    }

    function saneTaxRateOrDefault(rate) {
        const raw = Number(rate);
        if (!Number.isFinite(raw) || raw === 0) return raw || 0;
        return raw >= 0 && raw <= 0.40 ? raw : 0.20;
    }

    function discountedAfterTaxIncomeDisplay(value, taxRate, discountFactor) {
        const raw = parseMoney(value);
        const factor = 1 - taxRate;
        return raw && factor > 0 && discountFactor ? formatMoneyFront((raw * factor) / discountFactor) : '--';
    }

    function compactFormulaRows(rows) {
        return rows.filter(([label, value]) => label && value !== undefined && value !== null);
    }

    function targetMove(target, current) {
        const targetRaw = parseMoney(target);
        const currentRaw = parseMoney(current);
        return targetRaw && currentRaw ? formatPercentDecimal(targetRaw / currentRaw - 1) : '--';
    }

    function midpointDisplay(bid, ask) {
        const bidRaw = parseMoney(bid);
        const askRaw = parseMoney(ask);
        return bidRaw && askRaw ? formatMoneyFront((bidRaw + askRaw) / 2) : '--';
    }

    function halfSpreadDisplay(spread) {
        const spreadRaw = parseMoney(spread);
        return spreadRaw ? formatMoneyFront(spreadRaw / 2) : '--';
    }

    function impliedNetIncomeDisplay(eps, dilutedShares) {
        const epsRaw = parseMoney(eps);
        const dilutedSharesRaw = parseMoney(dilutedShares);
        return epsRaw && dilutedSharesRaw ? formatMoneyFront(epsRaw * dilutedSharesRaw) : '--';
    }

    function netCashPresentation(data) {
        const entry = metricEntry(data, 'netCash');
        let rawValue = NaN;
        if (entry && typeof entry === 'object' && 'raw' in entry) {
            rawValue = entry.raw === null || entry.raw === undefined ? NaN : Number(entry.raw);
        } else if (entry && entry !== '--') {
            rawValue = parseMoney(entry);
        }
        if (!Number.isFinite(rawValue)) return { label: 'Net Cash', display: '--', formula: 'Cash & Short Term Investments - Total Debt' };
        if (rawValue < 0) {
            return {
                label: 'Net Debt',
                display: formatMoneyFront(Math.abs(rawValue)),
                formula: 'Total Debt - Cash & Short Term Investments',
            };
        }
        return {
            label: 'Net Cash',
            display: formatMoneyFront(rawValue),
            formula: 'Cash & Short Term Investments - Total Debt',
        };
    }

    function calcDefinitions(data) {
        const val = (key) => metricDisplay(data, key);
        const raw = (key) => metricEntry(data, key);
        const netCash = netCashPresentation(data);
        const cashBucket = latestStatementValue((data.balanceStatement || {}).annual, [
            'Cash, Equivalents & Short Term Investments',
            'Cash & Short Term Investments',
            'Cash Cash Equivalents And Short Term Investments',
        ]);
        const totalDebtRaw = Math.max(parseMoney(cashBucket) - parseMoney(raw('netCash')), 0);
        const totalDebt = totalDebtRaw ? formatMoneyFront(totalDebtRaw) : '--';
        const formulaValue = (formula, rows) => compactFormulaRows([['Formula', formula], ...rows]);
        const valuationLabel = data.valuationNumeratorLabel || 'Valuation Numerator';
        const gpLabel = '3Y Growth';
        const cyRevenueBase = lastYearRevenueRaw(data);
        const cyRevenueBaseDisplay = cyRevenueBase ? formatMoneyFront(cyRevenueBase) : '--';
        const cyDiscount = forwardDiscountInfo(data, 'cy');
        const nyDiscount = forwardDiscountInfo(data, 'ny');
        const discountLabel = `${(FORWARD_DISCOUNT_RATE * 100).toFixed(0)}% Discount Rate`;
        const sourceTaxRate = parsePercentValue(raw('medianTaxRate'));
        const taxRate = saneTaxRateOrDefault(sourceTaxRate);
        const taxDisplay = sourceTaxRate && sourceTaxRate !== taxRate ? formatPercentDecimal(taxRate) : val('medianTaxRate');
        const afterTaxAdjIncome = afterTaxIncomeDisplay(raw('adj_income'), taxRate);
        const cyAfterTaxAdjIncome = afterTaxIncomeDisplay(raw('cy_adj_inc'), taxRate);
        const nyAfterTaxAdjIncome = afterTaxIncomeDisplay(raw('ny_adj_inc'), taxRate);
        const adjustedNetIncomeLabel = 'Adjusted Net Income';
        const cyAdjustedNetIncomeLabel = 'CY Adjusted Net Income';
        const nyAdjustedNetIncomeLabel = 'NY Adjusted Net Income';
        return {
            ev_adj: {
                title: `${data.valuationPrefix || 'EV'} / ${adjustedNetIncomeLabel}`,
                numeratorLabel: valuationLabel,
                numerator: val('ev'),
                divisorLabel: adjustedNetIncomeLabel,
                divisor: afterTaxAdjIncome,
                resultLabel: `${data.valuationPrefix || 'EV'} / ${adjustedNetIncomeLabel}`,
                result: val('ev_adj_ebit'),
                rows: formulaValue(`${valuationLabel} / (Adj Op Inc x (1 - Tax Rate))`, [
                    [valuationLabel, val('ev')],
                    ['Metric Date', `TTM as of ${formatDateShort(payloadDate(data))}`],
                    ['Revenue', val('revenue')],
                    ['Adj Op Inc Margin', val('margin')],
                    ['Adj Op Inc', val('adj_income')],
                    ['Median Tax Rate', taxDisplay],
                    [adjustedNetIncomeLabel, afterTaxAdjIncome],
                ]),
            },
            ev_cy: {
                title: `${data.valuationPrefix || 'EV'} / ${cyAdjustedNetIncomeLabel}`,
                numeratorLabel: valuationLabel,
                numerator: val('ev'),
                divisorLabel: cyAdjustedNetIncomeLabel,
                divisor: cyAfterTaxAdjIncome,
                resultLabel: `${data.valuationPrefix || 'EV'} / ${cyAdjustedNetIncomeLabel}`,
                result: val('ev_cy_ebit'),
                rows: formulaValue(`${valuationLabel} / (((Last Year Revenue x (1 + CY Growth) x Adj Op Inc Margin) x (1 - Tax Rate)) / Discount Factor)`, [
                    [valuationLabel, val('ev')],
                    ['Metric Date', formatDateShort(cyDiscount.date)],
                    ['Years Forward', cyDiscount.years.toFixed(2)],
                    [discountLabel, `${cyDiscount.factor.toFixed(2)}x factor`],
                    ['Last Year Revenue', cyRevenueBaseDisplay],
                    ['CY Growth', val('cy_growth')],
                    ['CY Revenue', val('cy_revenue')],
                    ['Adj Op Inc Margin', val('margin')],
                    ['CY Adj Op Inc', val('cy_adj_inc')],
                    ['Median Tax Rate', taxDisplay],
                    [cyAdjustedNetIncomeLabel, cyAfterTaxAdjIncome],
                    [`Discounted ${cyAdjustedNetIncomeLabel}`, discountedAfterTaxIncomeDisplay(raw('cy_adj_inc'), taxRate, cyDiscount.factor)],
                ]),
            },
            ev_ny: {
                title: `${data.valuationPrefix || 'EV'} / ${nyAdjustedNetIncomeLabel}`,
                numeratorLabel: valuationLabel,
                numerator: val('ev'),
                divisorLabel: nyAdjustedNetIncomeLabel,
                divisor: nyAfterTaxAdjIncome,
                resultLabel: `${data.valuationPrefix || 'EV'} / ${nyAdjustedNetIncomeLabel}`,
                result: val('ev_ny_ebit'),
                rows: formulaValue(`${valuationLabel} / (((CY Revenue x (1 + NY Growth) x Adj Op Inc Margin) x (1 - Tax Rate)) / Discount Factor)`, [
                    [valuationLabel, val('ev')],
                    ['Metric Date', formatDateShort(nyDiscount.date)],
                    ['Years Forward', nyDiscount.years.toFixed(2)],
                    [discountLabel, `${nyDiscount.factor.toFixed(2)}x factor`],
                    ['CY Revenue', val('cy_revenue')],
                    ['NY Growth', val('ny_growth')],
                    ['NY Revenue', val('ny_revenue')],
                    ['Adj Op Inc Margin', val('margin')],
                    ['NY Adj Op Inc', val('ny_adj_inc')],
                    ['Median Tax Rate', taxDisplay],
                    [nyAdjustedNetIncomeLabel, nyAfterTaxAdjIncome],
                    [`Discounted ${nyAdjustedNetIncomeLabel}`, discountedAfterTaxIncomeDisplay(raw('ny_adj_inc'), taxRate, nyDiscount.factor)],
                ]),
            },
            ly_est_net_margin: {
                title: 'LY Estimated Net Margin',
                numeratorLabel: 'LY EPS x Diluted Shares',
                numerator: impliedNetIncomeDisplay(raw('yearAgoEps'), raw('dilutedShares')),
                divisorLabel: 'Last Year Revenue',
                divisor: cyRevenueBaseDisplay,
                resultLabel: 'LY Estimated Net Margin',
                result: val('lastYearEstimatedNetMargin'),
                rows: formulaValue('(LY EPS x Diluted Shares) / Last Year Revenue', [
                    ['LY EPS', val('yearAgoEps')],
                    ['Diluted Shares', val('dilutedShares')],
                    ['Implied LY Net Income', impliedNetIncomeDisplay(raw('yearAgoEps'), raw('dilutedShares'))],
                    ['Last Year Revenue', cyRevenueBaseDisplay],
                    ['LY Estimated Net Margin', val('lastYearEstimatedNetMargin')],
                ]),
            },
            cy_est_net_margin: {
                title: 'CY Estimated Net Margin',
                numeratorLabel: 'CY EPS x Diluted Shares',
                numerator: impliedNetIncomeDisplay(raw('currentYearEps'), raw('dilutedShares')),
                divisorLabel: 'CY Revenue',
                divisor: val('cy_revenue'),
                resultLabel: 'CY Estimated Net Margin',
                result: val('currentYearEstimatedNetMargin'),
                rows: formulaValue('(CY EPS x Diluted Shares) / CY Revenue', [
                    ['CY EPS Estimate', val('currentYearEps')],
                    ['Diluted Shares', val('dilutedShares')],
                    ['Implied CY Net Income', impliedNetIncomeDisplay(raw('currentYearEps'), raw('dilutedShares'))],
                    ['CY Revenue Estimate', val('cy_revenue')],
                    ['CY Estimated Net Margin', val('currentYearEstimatedNetMargin')],
                ]),
            },
            ny_est_net_margin: {
                title: 'NY Estimated Net Margin',
                numeratorLabel: 'NY EPS x Diluted Shares',
                numerator: impliedNetIncomeDisplay(raw('nextYearEps'), raw('dilutedShares')),
                divisorLabel: 'NY Revenue',
                divisor: val('ny_revenue'),
                resultLabel: 'NY Estimated Net Margin',
                result: val('nextYearEstimatedNetMargin'),
                rows: formulaValue('(NY EPS x Diluted Shares) / NY Revenue', [
                    ['NY EPS Estimate', val('nextYearEps')],
                    ['Diluted Shares', val('dilutedShares')],
                    ['Implied NY Net Income', impliedNetIncomeDisplay(raw('nextYearEps'), raw('dilutedShares'))],
                    ['NY Revenue Estimate', val('ny_revenue')],
                    ['NY Estimated Net Margin', val('nextYearEstimatedNetMargin')],
                ]),
            },
            adj_margin: {
                title: 'Adj Op Inc Margin',
                numeratorLabel: 'Adj Op Inc',
                numerator: val('adj_income'),
                divisorLabel: 'Revenue',
                divisor: val('revenue'),
                resultLabel: 'Margin',
                result: val('margin'),
                rows: formulaValue('Adj Op Inc / Revenue', [
                    ['Revenue', val('revenue')],
                    ['Operating Income', val('income')],
                    ['D&A', val('da')],
                    ['Capex', val('capex')],
                    ['D&A Less Capex Addback', val('da_minus_capex')],
                    ['Adj Op Inc', val('adj_income')],
                ]),
            },
            gp_3y_growth: {
                title: gpLabel,
                numeratorLabel: 'Ending Value',
                numerator: val('gp_3y_end'),
                divisorLabel: 'Starting Value',
                divisor: val('gp_3y_start'),
                resultLabel: 'Annual Growth',
                result: val('gp_3y_growth'),
                rows: formulaValue('(Ending Value / Starting Value) ^ (1 / 3) - 1', [
                    ['Starting Value', val('gp_3y_start')],
                    ['Ending Value', val('gp_3y_end')],
                    ['Years', '3'],
                    [gpLabel, val('gp_3y_growth')],
                ]),
            },
            net_cash: {
                title: netCash.label,
                numeratorLabel: netCash.label === 'Net Debt' ? 'Total Debt' : 'Cash & Short Term Investments',
                numerator: netCash.label === 'Net Debt' ? totalDebt : cashBucket,
                divisorLabel: netCash.label === 'Net Debt' ? 'Cash & Short Term Investments' : 'Debt',
                divisor: netCash.label === 'Net Debt' ? cashBucket : totalDebt,
                resultLabel: netCash.label,
                result: netCash.display,
                rows: formulaValue(netCash.formula, [
                    ...(netCash.label === 'Net Debt'
                        ? [['Total Debt', totalDebt], ['Cash & Short Term Investments', cashBucket]]
                        : [['Cash & Short Term Investments', cashBucket], ['Total Debt', totalDebt]]),
                    [netCash.label, netCash.display],
                ]),
            },
            roc: {
                title: 'Return on Capital',
                numeratorLabel: adjustedNetIncomeLabel,
                numerator: afterTaxAdjIncome,
                divisorLabel: 'NWC + Net Fixed Assets',
                divisor: `${val('netWorkingCapital') || '--'} + ${val('netFixedAssets') || '--'}`,
                resultLabel: 'ROC',
                result: val('roc'),
                rows: formulaValue('(Adj Op Inc x (1 - Tax Rate)) / (Net Working Capital + Net Fixed Assets)', [
                    ['Adj Op Inc', val('adj_income')],
                    ['Median Tax Rate', taxDisplay],
                    [adjustedNetIncomeLabel, afterTaxAdjIncome],
                    ['Receivables', val('receivables')],
                    ['Inventory', val('inventory')],
                    ['Accounts Payable', val('accountsPayable')],
                    ['Net Working Capital', val('netWorkingCapital')],
                    ['Net Fixed Assets', val('netFixedAssets')],
                    ['ROC', val('roc')],
                ]),
            },
            adj_ebit_gross_ppe: {
                title: 'ROGPPE',
                numeratorLabel: adjustedNetIncomeLabel,
                numerator: afterTaxAdjIncome,
                divisorLabel: 'Gross PP&E',
                divisor: val('grossPpe'),
                resultLabel: 'ROGPPE',
                result: val('adjEbitGrossPpe'),
                rows: formulaValue('(Adj Op Inc x (1 - Tax Rate)) / Gross PP&E', [
                    ['Adj Op Inc', val('adj_income')],
                    ['Median Tax Rate', taxDisplay],
                    [adjustedNetIncomeLabel, afterTaxAdjIncome],
                    ['Gross PP&E', val('grossPpe')],
                    ['Result', val('adjEbitGrossPpe')],
                ]),
            },
            capex_adj_income: {
                title: 'Investment Rate',
                numeratorLabel: 'Investment Capex',
                numerator: val('investmentCapex'),
                divisorLabel: 'Adj Op Inc',
                divisor: val('adj_income'),
                resultLabel: 'Investment Rate',
                result: val('capexAdjIncome'),
                rows: formulaValue('max(Capex - D&A, 0) / Adj Op Inc', [
                    ['Capex', val('capex')],
                    ['D&A', val('da')],
                    ['Investment Capex', val('investmentCapex')],
                    ['Adj Op Inc', val('adj_income')],
                    ['Investment Rate', val('capexAdjIncome')],
                ]),
            },
            transaction_cost: {
                title: 'Estimated Transaction Cost',
                numeratorLabel: 'Half Bid/Ask Spread',
                numerator: halfSpreadDisplay(raw('bidAskSpread')),
                divisorLabel: 'Bid/Ask Midpoint',
                divisor: midpointDisplay(raw('bidPrice'), raw('askPrice')),
                resultLabel: 'Single Buy/Sell Cost',
                result: val('transactionCost'),
                rows: formulaValue('((Ask - Bid) / 2) / ((Bid + Ask) / 2)', [
                    ['Bid', val('bidPrice')],
                    ['Ask', val('askPrice')],
                    ['Spread', val('bidAskSpread')],
                    ['Half Spread', halfSpreadDisplay(raw('bidAskSpread'))],
                    ['Midpoint', midpointDisplay(raw('bidPrice'), raw('askPrice'))],
                    ['Estimated Cost', val('transactionCost')],
                ]),
            },
            target_bear: {
                title: 'Bear Case Target Move',
                numeratorLabel: 'Bear Target',
                numerator: val('targetLowPrice'),
                divisorLabel: 'Current Price',
                divisor: val('currentPrice'),
                resultLabel: 'Bear Move',
                result: targetMove(val('targetLowPrice'), val('currentPrice')),
                rows: formulaValue('(Bear Target / Current Price) - 1', [
                    ['Current Price', val('currentPrice')],
                    ['Bear Target', val('targetLowPrice')],
                    ['Bear Move', targetMove(val('targetLowPrice'), val('currentPrice'))],
                ]),
            },
            target_base: {
                title: 'Base Case Target Move',
                numeratorLabel: 'Base Target',
                numerator: val('targetMeanPrice'),
                divisorLabel: 'Current Price',
                divisor: val('currentPrice'),
                resultLabel: 'Base Move',
                result: targetMove(val('targetMeanPrice'), val('currentPrice')),
                rows: formulaValue('(Base Target / Current Price) - 1', [
                    ['Current Price', val('currentPrice')],
                    ['Base Target', val('targetMeanPrice')],
                    ['Base Move', targetMove(val('targetMeanPrice'), val('currentPrice'))],
                ]),
            },
            target_bull: {
                title: 'Bull Case Target Move',
                numeratorLabel: 'Bull Target',
                numerator: val('targetHighPrice'),
                divisorLabel: 'Current Price',
                divisor: val('currentPrice'),
                resultLabel: 'Bull Move',
                result: targetMove(val('targetHighPrice'), val('currentPrice')),
                rows: formulaValue('(Bull Target / Current Price) - 1', [
                    ['Current Price', val('currentPrice')],
                    ['Bull Target', val('targetHighPrice')],
                    ['Bull Move', targetMove(val('targetHighPrice'), val('currentPrice'))],
                ]),
            },
        };
    }

    function openCalc(type) {
        if (!state.latest) return;
        state.previousScroll = window.scrollY;
        const data = applyAssumptions(state.latest);
        const item = calcDefinitions(data)[type];
        if (!item) return;
        $('calc-ticker-badge').textContent = data.ticker;
        $('calc-title').textContent = item.title;
        $('calc-numerator-label').textContent = item.numeratorLabel || 'Numerator';
        $('calc-ev-val').textContent = item.numerator || '--';
        $('calc-divisor-label').textContent = item.divisorLabel || 'Divisor';
        $('calc-divisor-val').textContent = item.divisor || '--';
        $('calc-result-label').textContent = item.resultLabel || 'Final Metric Value';
        $('calc-result-val').textContent = item.result || '--';
        $('calc-breakdown-list').innerHTML = (item.rows || [])
            .map(([label, value]) => `<li><span class="calc-label">${label}</span><span class="calc-val">${value || '--'}</span></li>`)
            .join('');
        showView('calc');
        document.querySelector('.tabs').classList.add('hidden');
        window.scrollTo(0, 0);
    }

    $('calc-back-btn').addEventListener('click', () => {
        document.querySelector('.tabs').classList.remove('hidden');
        showView('scanner');
        window.scrollTo(0, state.previousScroll || 0);
    });

    $('fetch-back-btn').addEventListener('click', () => {
        document.querySelector('.tabs').classList.remove('hidden');
        showView('scanner');
        window.scrollTo(0, state.previousScroll || 0);
    });

    loadStarredAccounts();

    window.__stockAnalysisTestApi = {
        state,
        applyAssumptions,
        calcDefinitions,
        renderDataromaCards,
        loadStarredAccounts,
        saveStarredAccounts,
        metricEntry,
        metricDisplay,
        metricValueHtml,
        netCashPresentation,
        parseMoney,
        parsePercentValue,
        accountLabelMatchesSearch,
        buildRatiosStatement,
        growthValues,
        growthRowLabel,
        marginValues,
        startFetchTimer,
        stopFetchTimer,
        resetAssumption,
        resetStatementDefaultsForTicker,
    };

    window.removeTicker = (ticker) => {
        state.watchlist = state.watchlist.filter((item) => item !== ticker);
        save('stock_watchlist', state.watchlist);
        renderTickerTable('watchlist');
    };
});
