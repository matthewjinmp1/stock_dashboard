document.addEventListener('DOMContentLoaded', () => {
    const $ = (id) => document.getElementById(id);
    const state = {
        activeView: 'scanner',
        previousScroll: 0,
        latest: null,
        dataByTicker: JSON.parse(localStorage.getItem('stock_data_by_ticker') || '{}'),
        watchlist: JSON.parse(localStorage.getItem('stock_watchlist') || '[]'),
        starred: JSON.parse(localStorage.getItem('stock_starred_tickers') || '[]'),
        most: JSON.parse(localStorage.getItem('stock_search_counts') || '{}'),
        assumptions: JSON.parse(localStorage.getItem('stock_assumptions') || '{}'),
        statementTab: localStorage.getItem('stock_statement_tab') || 'income',
        periodicity: localStorage.getItem('stock_periodicity') || 'annual',
        statementSearch: localStorage.getItem('stock_statement_search') || '',
        starredAccounts: JSON.parse(localStorage.getItem('stock_starred_accounts') || '{}'),
        statementToggles: JSON.parse(localStorage.getItem('stock_statement_toggles') || '{}'),
        groups: [],
        sort: {},
    };

    const views = {
        scanner: $('view-scanner'),
        watchlist: $('view-watchlist'),
        groups: $('view-groups'),
        starred: $('view-starred'),
        'most-searched': $('view-most-searched'),
        calc: $('view-calc'),
    };

    const tabIds = ['scanner', 'watchlist', 'groups', 'starred', 'most-searched'];
    tabIds.forEach((name) => {
        const tab = $(`tab-${name}`);
        if (!tab) return;
        tab.addEventListener('click', () => showView(name));
    });

    function save(key, value) {
        localStorage.setItem(key, JSON.stringify(value));
    }

    function saveTickerData() {
        save('stock_data_by_ticker', state.dataByTicker);
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
        const date = data.dataDate || '--';
        const time = data.pulledAt ? data.pulledAt.split('T')[1] || data.pulledAt.split(' ')[1] || '' : '';
        return `As of ${date}${time ? ` ${time}` : ''}`;
    }

    function displayFetchInfo(data) {
        const fetches = data.fetchCount === undefined ? '--' : data.fetchCount;
        return `Fetch time: ${data.fetchTime || '--'} • Fetches: ${fetches}`;
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

    function metricInputWidth(value) {
        const length = String(value ?? '').length || 2;
        return `${Math.max(2, Math.min(length, 8))}ch`;
    }

    function metric(label, value, calcType = '', editType = '') {
        const link = calcType ? ' metric-title-link' : '';
        const displayValue = formatSigned(value || '--');
        const editableValue = editType
            ? `<input class="value-display metric-edit-input" type="text" value="${escapeAttr(displayValue)}" style="--metric-input-width: ${metricInputWidth(displayValue)}" data-edit-assumption="${editType}" data-original-value="${escapeAttr(displayValue)}" aria-label="Edit ${escapeAttr(label)}">`
            : `<div class="value-display">${displayValue}</div>`;
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
            metricGroup('EPS Growth', [
                metric('CY EPS Growth', val('currentYearEpsGrowth')),
                metric('NY EPS Growth', val('nextYearEpsGrowth')),
            ]),
            metricGroup('Returns', [
                metric('ROGPPE', val('adjEbitGrossPpe'), 'adj_ebit_gross_ppe'),
                metric('ROC', val('roc'), 'roc'),
            ]),
            metricGroup('Spending', [
                metric('Investment Rate', val('capexAdjIncome'), 'capex_adj_income'),
                metric('R&D / Adj Op Inc', val('rndAdjIncome')),
            ]),
            metricGroup('Short Interest', [
                metric('Short Float', val('shortFloat')),
            ]),
            metricGroup('Market', [
                metric('Market Cap', val('marketCap')),
                metric('Net Cash', val('netCash'), 'net_cash'),
                metric('Our EV', val('derivedEnterpriseValue')),
            ]),
            metricGroup('Valuation', [
                metric(`${data.valuationPrefix || 'EV'}/Adj Op Inc`, val('ev_adj_ebit'), 'ev_adj'),
                metric(`${data.valuationPrefix || 'EV'}/CY Op Inc`, val('ev_cy_ebit'), 'ev_cy'),
                metric(`${data.valuationPrefix || 'EV'}/NY Op Inc`, val('ev_ny_ebit'), 'ev_ny'),
            ]),
            metricGroup('P/E', [
                metric('P/LY EPS', val('priceCurrentEps')),
                metric('P/CY EPS', val('priceCyEps')),
                metric('P/NY EPS', val('priceNyEps')),
            ]),
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
                node.style.setProperty('--metric-input-width', metricInputWidth(node.value || node.dataset.editingOriginalValue || node.dataset.originalValue || '--'));
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
            node.title = 'Edit directly. Press Enter or click away to save.';
        });
    }

    function applyAssumptions(input) {
        const data = { ...input };
        data.metrics = { ...(input.metrics || {}) };
        const ticker = (data.ticker || '').toUpperCase();
        const assumptions = state.assumptions[ticker] || {};
        const margin = assumptions.margin ?? parsePercentValue(metricEntry(data, 'margin'));
        const cyGrowth = assumptions.cy_growth ?? parsePercentValue(metricEntry(data, 'cy_growth'));
        const nyGrowth = assumptions.ny_growth ?? parsePercentValue(metricEntry(data, 'ny_growth'));
        const revenueRaw = parseMoney(metricEntry(data, 'revenue'));
        const valuationRaw = parseMoney(metricEntry(data, 'ev'));
        const grossPpeRaw = parseMoney(metricEntry(data, 'grossPpe'));
        const investmentCapexRaw = parseMoney(metricEntry(data, 'investmentCapex'));
        const rocDenomRaw = parseMoney(metricEntry(data, 'netWorkingCapital')) + parseMoney(metricEntry(data, 'netFixedAssets'));

        if (assumptions.margin !== undefined) {
            const adjRaw = revenueRaw * margin;
            setMetric(data, 'margin', margin, formatPercentDecimal(margin), 'percent');
            setMetric(data, 'adj_income', adjRaw, formatMoneyFront(adjRaw), 'money');
            setMetric(data, 'ev_adj_ebit', valuationRaw && adjRaw ? valuationRaw / adjRaw : null, valuationRaw && adjRaw ? formatRatio(valuationRaw / adjRaw) : '--', 'ratio');
            setMetric(data, 'adjEbitGrossPpe', grossPpeRaw && adjRaw ? adjRaw / grossPpeRaw : null, grossPpeRaw && adjRaw ? formatPercentDecimal(adjRaw / grossPpeRaw) : '--', 'percent');
            setMetric(data, 'capexAdjIncome', adjRaw ? investmentCapexRaw / adjRaw : null, adjRaw ? formatPercentDecimal(investmentCapexRaw / adjRaw) : '--', 'percent');
            setMetric(data, 'roc', rocDenomRaw && adjRaw ? adjRaw / rocDenomRaw : null, rocDenomRaw && adjRaw ? formatPercentDecimal(adjRaw / rocDenomRaw) : '--', 'percent');
        }

        if (assumptions.cy_growth !== undefined) setMetric(data, 'cy_growth', cyGrowth, formatPercentDecimal(cyGrowth), 'percent');
        if (assumptions.ny_growth !== undefined) setMetric(data, 'ny_growth', nyGrowth, formatPercentDecimal(nyGrowth), 'percent');

        const effectiveAdjRaw = revenueRaw * margin;
        const cyRevenueRaw = assumptions.cy_growth !== undefined && revenueRaw
            ? revenueRaw * (1 + cyGrowth)
            : parseMoney(metricEntry(data, 'cy_revenue'));
        const nyRevenueRaw = assumptions.ny_growth !== undefined && cyRevenueRaw
            ? cyRevenueRaw * (1 + nyGrowth)
            : parseMoney(metricEntry(data, 'ny_revenue'));
        const cyAdjRaw = cyRevenueRaw * margin;
        const nyAdjRaw = nyRevenueRaw * margin;
        if (assumptions.margin !== undefined || assumptions.cy_growth !== undefined) {
            setMetric(data, 'cy_revenue', cyRevenueRaw, formatMoneyFront(cyRevenueRaw), 'money');
            setMetric(data, 'cy_adj_inc', cyAdjRaw, formatMoneyFront(cyAdjRaw), 'money');
            setMetric(data, 'ev_cy_ebit', valuationRaw && cyAdjRaw ? valuationRaw / cyAdjRaw : null, valuationRaw && cyAdjRaw ? formatRatio(valuationRaw / cyAdjRaw) : '--', 'ratio');
        }
        if (assumptions.margin !== undefined || assumptions.cy_growth !== undefined || assumptions.ny_growth !== undefined) {
            setMetric(data, 'ny_revenue', nyRevenueRaw, formatMoneyFront(nyRevenueRaw), 'money');
            setMetric(data, 'ny_adj_inc', nyAdjRaw, formatMoneyFront(nyAdjRaw), 'money');
            setMetric(data, 'ev_ny_ebit', valuationRaw && nyAdjRaw ? valuationRaw / nyAdjRaw : null, valuationRaw && nyAdjRaw ? formatRatio(valuationRaw / nyAdjRaw) : '--', 'ratio');
        }
        if (assumptions.margin !== undefined && !effectiveAdjRaw) {
            setMetric(data, 'ev_adj_ebit', null, '--', 'ratio');
            setMetric(data, 'ev_cy_ebit', null, '--', 'ratio');
            setMetric(data, 'ev_ny_ebit', null, '--', 'ratio');
        }
        return data;
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
        save('stock_assumptions', state.assumptions);
        renderStats(state.latest);
    }

    function renderAnalystCards(data) {
        const val = (key) => metricDisplay(data, key);
        const rec = data.analystRecommendations || {};
        const counts = [
            ['Strong Buy', rec.strongBuy || 0, 'strong-buy'],
            ['Buy', rec.buy || 0, 'buy'],
            ['Hold', rec.hold || 0, 'hold'],
            ['Sell', rec.sell || 0, 'sell'],
            ['Strong Sell', rec.strongSell || 0, 'strong-sell'],
        ];
        const total = counts.reduce((sum, item) => sum + Number(item[1] || 0), 0);
        const rating = data.recommendationMean && data.recommendationMean !== '--'
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
                <div class="rec-summary">${data.recommendationKey || '--'} • ${rating}/5 stars</div>
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
        const started = performance.now();
        const url = `/api/short-interest/${ticker}${refresh ? '?refresh=1' : ''}`;
        const response = await fetch(url);
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Failed to fetch data');
        data.fetchTime = `${((performance.now() - started) / 1000).toFixed(2)}s`;
        state.dataByTicker[ticker] = data;
        saveTickerData();
        return data;
    }

    async function scanTicker(ticker, refresh = false) {
        if (!ticker) return;
        ticker = ticker.toUpperCase();
        const hasCurrentResult = refresh
            && state.latest
            && (state.latest.ticker || '').toUpperCase() === ticker
            && !$('result-stats').classList.contains('hidden');
        showView('scanner');
        $('result-container').classList.remove('hidden');
        if (!hasCurrentResult) {
            $('result-stats').classList.add('hidden');
            $('statement-panel').classList.add('hidden');
            state.latest = null;
            $('result-ticker').textContent = ticker;
            $('result-data-date').textContent = 'As of --';
            $('result-fetch-info').textContent = 'Fetch time: -- • Fetches: --';
            $('result-currency-info').textContent = 'Native currency: -- • USD rate: --';
        }
        $('glass-card').classList.toggle('refreshing', hasCurrentResult);
        $('loading-spinner').classList.remove('hidden');
        $('error-message').classList.add('hidden');
        $('glass-card').style.display = 'block';
        incrementSearch(ticker);

        try {
            const data = await fetchTicker(ticker, refresh);
            state.latest = data;
            $('loading-spinner').classList.add('hidden');
            $('glass-card').classList.remove('refreshing');
            $('result-stats').classList.remove('hidden');
            $('result-ticker').textContent = data.ticker || ticker;
            const title = $('result-ticker').parentElement;
            if (title && !title.querySelector('.company-name')) {
                $('result-ticker').insertAdjacentHTML('afterend', '<div class="company-name"></div>');
            }
            const company = title ? title.querySelector('.company-name') : null;
            if (company) company.textContent = data.companyName || '--';
            $('result-data-date').textContent = displayDate(data);
            $('result-fetch-info').textContent = displayFetchInfo(data);
            $('result-currency-info').textContent = displayCurrency(data);
            updateResultStarButton(ticker);
            renderStats(data);
            renderStatements(data);
        } catch (err) {
            $('loading-spinner').classList.add('hidden');
            $('glass-card').classList.remove('refreshing');
            $('error-message').textContent = err.message;
            $('error-message').classList.remove('hidden');
            // Still show dashboard with whatever data we have
            $('result-stats').classList.remove('hidden');
            const emptyData = { ticker, companyName: ticker, incomeStatement: {}, balanceStatement: {}, cashFlowStatement: {} };
            const data = state.latest && (state.latest.ticker || '').toUpperCase() === ticker ? state.latest : emptyData;
            renderStats(data);
            renderStatements(data);
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
            ${sortableHeader('ny_growth', 'NY Growth', kind)}${sortableHeader('shortFloat', 'Short Float', kind)}
            ${sortableHeader('ev_adj_ebit', 'EV/Adj Op Inc', kind)}${sortableHeader('ev_cy_ebit', 'EV/CY', kind)}
            ${sortableHeader('ev_ny_ebit', 'EV/NY', kind)}<th>Actions</th>
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
            const av = sortableValue(metricEntry(state.dataByTicker[a], sort.key));
            const bv = sortableValue(metricEntry(state.dataByTicker[b], sort.key));
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
        localStorage.setItem('stock_statement_search', state.statementSearch);
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
            ['starred', 'Starred'],
        ];
        panel.innerHTML = `<div class="statement-header">
            <div>
                <div style="display: flex; gap: 1rem; align-items: center; margin-bottom: 0.5rem;">
                    <h2>${state.statementTab === 'starred' ? 'Starred Statements' : tabs.find(t => t[0] === state.statementTab)[1]}</h2>
                    <div class="statement-actions" style="margin-left: 1rem;">
                        <button class="mini-btn ${state.periodicity === 'annual' ? 'on blue' : ''}" data-periodicity="annual">Annual</button>
                        <button class="mini-btn ${state.periodicity === 'quarterly' ? 'on blue' : ''}" data-periodicity="quarterly">Quarterly</button>
                    </div>
                </div>
                <p>${state.periodicity === 'annual' ? 'Annual' : 'Quarterly'} figures shown in USD-normalized values</p>
                <label class="statement-search">
                    <span class="sr-only">Search statement line items</span>
                    <input type="search" value="${escapeAttr(state.statementSearch)}" placeholder="Search line items" data-statement-search autocomplete="off">
                </label>
            </div>
            <div class="statement-tabs">${tabs.map(([key, label]) => `<button class="tab-btn ${state.statementTab === key ? 'active' : ''}" data-statement-tab="${key}">${label}</button>`).join('')}</div>
        </div>
        <div id="statement-results">${renderStatementResults(data)}</div>`;
    }

    function renderStatementResults(data) {
        if (!data) return '';
        return state.statementTab === 'starred'
            ? renderStarredStatementTable(data)
            : renderStatementTable(statementForTab(data, state.statementTab), state.statementTab);
    }

    function statementForTab(data, tab) {
        let stmt = {};
        if (tab === 'balance') stmt = data.balanceStatement || {};
        else if (tab === 'cash') stmt = data.cashFlowStatement || {};
        else stmt = data.incomeStatement || {};
        return stmt[state.periodicity || 'annual'] || {};
    }

    function starredKey(statement, label) {
        return `${statement}:${label}`;
    }

    function toggleStarredAccount(statement, label) {
        const key = starredKey(statement, label);
        state.starredAccounts[key] = !state.starredAccounts[key];
        save('stock_starred_accounts', state.starredAccounts);
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
        ].map(([key, label, statement]) => {
            const rows = (statement.rows || []).filter((row) => state.starredAccounts[starredKey(key, row.label)]);
            if (!rows.length) return '';
            return `<h3 class="statement-section-title">${label}</h3>${renderStatementTable({ periods: statement.periods, rows }, key, false)}`;
        }).join('');
        return blocks || '<p class="empty-note">Star accounts from a statement to show them here.</p>';
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
        return rows.filter((row) => String(row.label || '').toLowerCase().includes(term));
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
        const canMargin = statementKey === 'income' || statementKey === 'cash';
        const starred = state.starredAccounts[starredKey(statementKey, row.label)];
        const growthOn = state.statementToggles[`${statementKey}:growth:${row.label}`];
        const marginOn = state.statementToggles[`${statementKey}:margin:${row.label}`];
        const shouldHighlight = starred && state.statementTab !== 'starred';
        let html = `<tr${shouldHighlight ? ' class="starred-row"' : ''}><td class="statement-action-cell"><div class="statement-actions">
            <button class="mini-btn ${starred ? 'on gold' : ''}" data-statement="${statementKey}" data-star-account="${row.label}">${starred ? 'Starred' : 'Star'}</button>
            <button class="mini-btn ${growthOn ? 'on blue' : ''}" data-statement="${statementKey}" data-toggle-ratio="growth" data-label="${row.label}">Growth</button>
            ${canMargin ? `<button class="mini-btn ${marginOn ? 'on green' : ''}" data-statement="${statementKey}" data-toggle-ratio="margin" data-label="${row.label}">Margin</button>` : ''}
        </div></td><td class="statement-label-cell">${row.label}</td>${(row.values || []).map(value => `<td>${formatStatementValue(value)}</td>`).join('')}</tr>`;
        if (growthOn) html += ratioRow('Growth', growthValues(row.values || [], periods));
        if (marginOn) html += ratioRow('Margin', marginValues(row, periods, displayStatement));
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

    function growthValues(values, periods) {
        const lookback = state.periodicity === 'quarterly' ? 4 : 1;
        return values.map((value, idx) => {
            if (idx < lookback) return '--';
            
            // Try to find the actual YoY index by period label if quarterly
            let prevIdx = idx - lookback;
            if (state.periodicity === 'quarterly') {
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
            return (prev && prev !== 0) ? `${((curr / Math.abs(prev) - 1) * 100).toFixed(1)}%` : '--';
        });
    }

    function marginValues(row, periods, statement) {
        const revenue = (statement.rows || []).find(r => r.label === 'Total Revenue' || r.label === 'Operating Cash Flow');
        return (row.values || []).map((value, idx) => {
            const denom = revenue ? parsePercentBase(revenue.values[idx]) : 0;
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

    function compactFormulaRows(rows) {
        return rows.filter(([label, value]) => label && value !== undefined && value !== null);
    }

    function targetMove(target, current) {
        const targetRaw = parseMoney(target);
        const currentRaw = parseMoney(current);
        return targetRaw && currentRaw ? formatPercentDecimal(targetRaw / currentRaw - 1) : '--';
    }

    function calcDefinitions(data) {
        const val = (key) => metricDisplay(data, key);
        const raw = (key) => metricEntry(data, key);
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
        return {
            ev_adj: {
                title: `${data.valuationPrefix || 'EV'} / Adj Op Inc`,
                numeratorLabel: valuationLabel,
                numerator: val('ev'),
                divisorLabel: 'Adj Op Inc',
                divisor: val('adj_income'),
                resultLabel: `${data.valuationPrefix || 'EV'} / Adj Op Inc`,
                result: val('ev_adj_ebit'),
                rows: formulaValue(`${valuationLabel} / Adj Op Inc`, [
                    [valuationLabel, val('ev')],
                    ['Revenue', val('revenue')],
                    ['Adj Op Inc Margin', val('margin')],
                    ['Adj Op Inc', val('adj_income')],
                ]),
            },
            ev_cy: {
                title: `${data.valuationPrefix || 'EV'} / CY Op Inc`,
                numeratorLabel: valuationLabel,
                numerator: val('ev'),
                divisorLabel: 'CY Adj Op Inc',
                divisor: val('cy_adj_inc'),
                resultLabel: `${data.valuationPrefix || 'EV'} / CY Op Inc`,
                result: val('ev_cy_ebit'),
                rows: formulaValue(`${valuationLabel} / (Revenue x (1 + CY Growth) x Adj Op Inc Margin)`, [
                    [valuationLabel, val('ev')],
                    ['Revenue', val('revenue')],
                    ['CY Growth', val('cy_growth')],
                    ['CY Revenue', val('cy_revenue')],
                    ['Adj Op Inc Margin', val('margin')],
                    ['CY Adj Op Inc', val('cy_adj_inc')],
                ]),
            },
            ev_ny: {
                title: `${data.valuationPrefix || 'EV'} / NY Op Inc`,
                numeratorLabel: valuationLabel,
                numerator: val('ev'),
                divisorLabel: 'NY Adj Op Inc',
                divisor: val('ny_adj_inc'),
                resultLabel: `${data.valuationPrefix || 'EV'} / NY Op Inc`,
                result: val('ev_ny_ebit'),
                rows: formulaValue(`${valuationLabel} / (CY Revenue x (1 + NY Growth) x Adj Op Inc Margin)`, [
                    [valuationLabel, val('ev')],
                    ['CY Revenue', val('cy_revenue')],
                    ['NY Growth', val('ny_growth')],
                    ['NY Revenue', val('ny_revenue')],
                    ['Adj Op Inc Margin', val('margin')],
                    ['NY Adj Op Inc', val('ny_adj_inc')],
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
                title: 'Net Cash',
                numeratorLabel: 'Cash & Short Term Investments',
                numerator: cashBucket,
                divisorLabel: 'Debt',
                divisor: totalDebt,
                resultLabel: 'Net Cash',
                result: val('netCash'),
                rows: formulaValue('Cash & Short Term Investments - Total Debt', [
                    ['Cash & Short Term Investments', cashBucket],
                    ['Total Debt', totalDebt],
                    ['Net Cash', val('netCash')],
                ]),
            },
            roc: {
                title: 'Return on Capital',
                numeratorLabel: 'Adj Op Inc',
                numerator: val('adj_income'),
                divisorLabel: 'NWC + Net Fixed Assets',
                divisor: `${val('netWorkingCapital') || '--'} + ${val('netFixedAssets') || '--'}`,
                resultLabel: 'ROC',
                result: val('roc'),
                rows: formulaValue('Adj Op Inc / (Net Working Capital + Net Fixed Assets)', [
                    ['Adj Op Inc', val('adj_income')],
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
                numeratorLabel: 'Adj Op Inc',
                numerator: val('adj_income'),
                divisorLabel: 'Gross PP&E',
                divisor: val('grossPpe'),
                resultLabel: 'ROGPPE',
                result: val('adjEbitGrossPpe'),
                rows: formulaValue('Adj Op Inc / Gross PP&E', [
                    ['Adj Op Inc', val('adj_income')],
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

    window.removeTicker = (ticker) => {
        state.watchlist = state.watchlist.filter((item) => item !== ticker);
        save('stock_watchlist', state.watchlist);
        renderTickerTable('watchlist');
    };
});
