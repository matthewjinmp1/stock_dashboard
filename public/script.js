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
        if (typeof value !== 'string') return value || '--';
        return value.startsWith('+') ? value.slice(1) : value;
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

    function metric(label, value, calcType = '', editType = '') {
        const link = calcType ? ' metric-title-link' : '';
        const displayValue = formatSigned(value || '--');
        const editableValue = editType
            ? `<input class="value-display metric-edit-input" type="text" value="${escapeAttr(displayValue)}" data-edit-assumption="${editType}" data-original-value="${escapeAttr(displayValue)}" aria-label="Edit ${escapeAttr(label)}">`
            : `<div class="value-display">${displayValue}</div>`;
        return `<div class="stat-box">
            <span class="stat-label${link}" data-calc="${calcType}">${label}</span>
            ${editableValue}
        </div>`;
    }

    function metricGroup(title, items) {
        return `<section class="metric-group">
            <h3>${title}</h3>
            <div class="metric-group-grid">${items.join('')}</div>
        </section>`;
    }

    function renderStats(data) {
        data = applyAssumptions(data);
        const stats = $('result-stats');
        if (!stats) return;
        stats.classList.remove('stats-grid');
        stats.innerHTML = [
            metricGroup('Margins', [
                metric('Adj Op Inc Margin', data.margin, 'adj_margin', 'margin'),
                metric('Gross Margin', data.grossMargin),
            ]),
            metricGroup('Growth', [
                metric(data.gp_3y_label || '3Y GP Growth', data.gp_3y_growth || '--', 'gp_3y_growth'),
                metric('CY Growth', data.cy_growth, '', 'cy_growth'),
                metric('NY Growth', data.ny_growth, '', 'ny_growth'),
                metric('CY EPS Growth', data.currentYearEpsGrowth),
                metric('NY EPS Growth', data.nextYearEpsGrowth),
            ]),
            metricGroup('Returns', [
                metric('Adj Op Inc / Gross PP&E', data.adjEbitGrossPpe, 'adj_ebit_gross_ppe'),
                metric('ROC', data.roc, 'roc'),
            ]),
            metricGroup('Spending', [
                metric('Investment Capex / Adj Op Inc', data.capexAdjIncome, 'capex_adj_income'),
                metric('R&D / Adj Op Inc', data.rndAdjIncome || '--'),
            ]),
            metricGroup('Short Interest', [
                metric('Short Float', data.shortFloat),
            ]),
            metricGroup('Market', [
                metric('Market Cap', data.marketCap),
                metric('Net Cash', data.netCash, 'net_cash'),
                metric('Our EV', data.derivedEnterpriseValue),
            ]),
            metricGroup('Valuation', [
                metric(`${data.valuationPrefix || 'EV'}/Adj Op Inc`, data.ev_adj_ebit, 'ev_adj'),
                metric(`${data.valuationPrefix || 'EV'}/CY Op Inc`, data.ev_cy_ebit, 'ev_cy'),
                metric(`${data.valuationPrefix || 'EV'}/NY Op Inc`, data.ev_ny_ebit, 'ev_ny'),
            ]),
            metricGroup('P/E', [
                metric('P/LY EPS', data.priceCurrentEps),
                metric('P/CY EPS', data.priceCyEps),
                metric('P/NY EPS', data.priceNyEps),
            ]),
            renderAnalystCards(data),
        ].join('');

        stats.querySelectorAll('[data-calc]').forEach((node) => {
            node.addEventListener('click', () => openCalc(node.dataset.calc));
        });
        stats.querySelectorAll('[data-edit-assumption]').forEach((node) => {
            node.addEventListener('focus', () => {
                node.dataset.editingOriginalValue = node.value;
                node.value = '';
            });
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
        const ticker = (data.ticker || '').toUpperCase();
        const assumptions = state.assumptions[ticker] || {};
        const margin = assumptions.margin ?? parsePercentValue(data.margin);
        const cyGrowth = assumptions.cy_growth ?? parsePercentValue(data.cy_growth);
        const nyGrowth = assumptions.ny_growth ?? parsePercentValue(data.ny_growth);
        const revenueRaw = parseMoney(data.revenue);
        const valuationRaw = parseMoney(data.ev);
        const grossPpeRaw = parseMoney(data.grossPpe);
        const investmentCapexRaw = parseMoney(data.investmentCapex);
        const rocDenomRaw = parseMoney(data.netWorkingCapital) + parseMoney(data.netFixedAssets);

        if (assumptions.margin !== undefined) {
            const adjRaw = revenueRaw * margin;
            data.margin = formatPercentDecimal(margin);
            data.adj_income = formatMoneyFront(adjRaw);
            data.ev_adj_ebit = valuationRaw && adjRaw ? formatRatio(valuationRaw / adjRaw) : '--';
            data.adjEbitGrossPpe = grossPpeRaw && adjRaw ? formatPercentDecimal(adjRaw / grossPpeRaw) : '--';
            data.capexAdjIncome = adjRaw ? formatPercentDecimal(investmentCapexRaw / adjRaw) : '--';
            data.roc = rocDenomRaw && adjRaw ? formatPercentDecimal(adjRaw / rocDenomRaw) : '--';
        }

        if (assumptions.cy_growth !== undefined) data.cy_growth = formatPercentDecimal(cyGrowth);
        if (assumptions.ny_growth !== undefined) data.ny_growth = formatPercentDecimal(nyGrowth);

        const effectiveAdjRaw = revenueRaw * margin;
        const cyRevenueRaw = assumptions.cy_growth !== undefined && revenueRaw
            ? revenueRaw * (1 + cyGrowth)
            : parseMoney(data.cy_revenue);
        const nyRevenueRaw = assumptions.ny_growth !== undefined && cyRevenueRaw
            ? cyRevenueRaw * (1 + nyGrowth)
            : parseMoney(data.ny_revenue);
        const cyAdjRaw = cyRevenueRaw * margin;
        const nyAdjRaw = nyRevenueRaw * margin;
        if (assumptions.margin !== undefined || assumptions.cy_growth !== undefined) {
            data.cy_revenue = formatMoneyFront(cyRevenueRaw);
            data.cy_adj_inc = formatMoneyFront(cyAdjRaw);
            data.ev_cy_ebit = valuationRaw && cyAdjRaw ? formatRatio(valuationRaw / cyAdjRaw) : '--';
        }
        if (assumptions.margin !== undefined || assumptions.cy_growth !== undefined || assumptions.ny_growth !== undefined) {
            data.ny_revenue = formatMoneyFront(nyRevenueRaw);
            data.ny_adj_inc = formatMoneyFront(nyAdjRaw);
            data.ev_ny_ebit = valuationRaw && nyAdjRaw ? formatRatio(valuationRaw / nyAdjRaw) : '--';
        }
        if (assumptions.margin !== undefined && !effectiveAdjRaw) {
            data.ev_adj_ebit = '--';
            data.ev_cy_ebit = '--';
            data.ev_ny_ebit = '--';
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
                    ${caseButton('Bear', data.targetLowPrice, data.currentPrice)}
                    ${caseButton('Base', data.targetMeanPrice, data.currentPrice)}
                    ${caseButton('Bull', data.targetHighPrice, data.currentPrice)}
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
                title.insertAdjacentHTML('beforeend', '<div class="company-name"></div>');
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
            if (!hasCurrentResult) $('glass-card').style.display = 'none';
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

    function tableHeaders() {
        return `<tr>
            <th data-sort="ticker">Ticker</th><th data-sort="margin">Adj Margin</th>
            <th data-sort="grossMargin">Gross Margin</th><th data-sort="cy_growth">CY Growth</th>
            <th data-sort="ny_growth">NY Growth</th><th data-sort="shortFloat">Short Float</th>
            <th data-sort="ev_adj_ebit">EV/Adj Op Inc</th><th data-sort="ev_cy_ebit">EV/CY</th>
            <th data-sort="ev_ny_ebit">EV/NY</th><th>Actions</th>
        </tr>`;
    }

    function renderTickerTable(kind) {
        const list = kind === 'watchlist' ? state.watchlist : state.groups;
        const body = kind === 'watchlist' ? $('watchlist-body') : $('groups-body');
        const head = kind === 'watchlist' ? document.querySelector('#watchlist-table thead') : $('groups-head');
        if (head) head.innerHTML = tableHeaders();
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
            const av = sortableValue(state.dataByTicker[a]?.[sort.key]);
            const bv = sortableValue(state.dataByTicker[b]?.[sort.key]);
            return sort.direction === 'asc' ? av - bv : bv - av;
        });
    }

    function sortableValue(value) {
        if (value === null || value === undefined || value === '' || value === '--') return Number.NEGATIVE_INFINITY;
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
            <td>${ticker}</td><td>${data.margin || '--'}</td><td>${data.grossMargin || '--'}</td>
            <td>${formatSigned(data.cy_growth)}</td><td>${formatSigned(data.ny_growth)}</td>
            <td>${data.shortFloat || '--'}</td><td>${data.ev_adj_ebit || '--'}</td>
            <td>${data.ev_cy_ebit || '--'}</td><td>${data.ev_ny_ebit || '--'}</td>
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
        const statement = event.target.closest('[data-statement-tab]');
        if (statement) {
            state.statementTab = statement.dataset.statementTab;
            localStorage.setItem('stock_statement_tab', state.statementTab);
            renderStatements(state.latest);
        }
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
            <div><h2>${state.statementTab === 'starred' ? 'Starred Statements' : tabs.find(t => t[0] === state.statementTab)[1]}</h2>
            <p>Annual figures shown in USD-normalized values</p></div>
            <div class="statement-tabs">${tabs.map(([key, label]) => `<button class="tab-btn ${state.statementTab === key ? 'active' : ''}" data-statement-tab="${key}">${label}</button>`).join('')}</div>
        </div>
        ${state.statementTab === 'starred' ? renderStarredStatementTable(data) : renderStatementTable(statementForTab(data, state.statementTab), state.statementTab)}`;
    }

    function statementForTab(data, tab) {
        if (tab === 'balance') return data.balanceStatement || {};
        if (tab === 'cash') return data.cashFlowStatement || {};
        return data.incomeStatement || {};
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
        const blocks = [
            ['income', 'Income Statement', data.incomeStatement || {}],
            ['balance', 'Balance Sheet', data.balanceStatement || {}],
            ['cash', 'Cash Flow Statement', data.cashFlowStatement || {}],
        ].map(([key, label, statement]) => {
            const rows = (statement.rows || []).filter((row) => state.starredAccounts[starredKey(key, row.label)]);
            if (!rows.length) return '';
            return `<h3 class="statement-section-title">${label}</h3>${renderStatementTable({ periods: statement.periods, rows }, key, true)}`;
        }).join('');
        return blocks || '<p class="empty-note">Star accounts from a statement to show them here.</p>';
    }

    function renderStatementTable(statement, statementKey, hideHeader = false) {
        statement = statementForDisplay(statement);
        const periods = statement.periods || [];
        const rows = statement.rows || [];
        if (!rows.length) return '<p class="empty-note">No statement data available.</p>';
        return `<table class="statement-table">
            ${hideHeader ? '' : `<thead><tr><th>Actions</th><th>Line Item</th>${periods.map(p => `<th>${p}</th>`).join('')}</tr></thead>`}
            <tbody>${rows.map(row => renderStatementRow(row, periods, statementKey, statement)).join('')}</tbody>
        </table>`;
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
        let html = `<tr><td class="statement-action-cell"><div class="statement-actions">
            <button class="mini-btn ${starred ? 'on gold' : ''}" data-statement="${statementKey}" data-star-account="${row.label}">${starred ? 'Starred' : 'Star'}</button>
            <button class="mini-btn ${growthOn ? 'on blue' : ''}" data-statement="${statementKey}" data-toggle-ratio="growth" data-label="${row.label}">Growth</button>
            ${canMargin ? `<button class="mini-btn ${marginOn ? 'on green' : ''}" data-statement="${statementKey}" data-toggle-ratio="margin" data-label="${row.label}">Margin</button>` : ''}
        </div></td><td class="statement-label-cell">${row.label}</td>${(row.values || []).map(value => `<td>${formatStatementValue(value)}</td>`).join('')}</tr>`;
        if (growthOn) html += ratioRow('Growth', growthValues(row.values || []));
        if (marginOn) html += ratioRow('Margin', marginValues(row, periods, displayStatement));
        return html;
    }

    function ratioRow(label, values) {
        return `<tr class="ratio-row"><td></td><td>${label}</td>${values.map(v => `<td>${v}</td>`).join('')}</tr>`;
    }

    function parsePercentBase(value) {
        if (!value || value === '--') return 0;
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

    function growthValues(values) {
        return values.map((value, idx) => {
            if (idx === 0) return '--';
            const prev = parsePercentBase(values[idx - 1]);
            const curr = parsePercentBase(value);
            return prev ? `${((curr / Math.abs(prev) - 1) * 100).toFixed(1)}%` : '--';
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

    function openCalc(type) {
        if (!state.latest) return;
        state.previousScroll = window.scrollY;
        const data = applyAssumptions(state.latest);
        const map = {
            ev_adj: ['EV / Adj Op Inc', data.ev, 'Adj Op Inc', data.adj_income, data.ev_adj_ebit],
            ev_cy: ['EV / CY Op Inc', data.ev, 'CY Adj Op Inc', data.cy_adj_inc, data.ev_cy_ebit],
            ev_ny: ['EV / NY Op Inc', data.ev, 'NY Adj Op Inc', data.ny_adj_inc, data.ev_ny_ebit],
            adj_margin: ['Adj Margin', data.adj_income, 'Revenue', data.revenue, data.margin],
            gp_3y_growth: ['3Y GP Growth', data.gp_3y_end || '--', 'Starting Value', data.gp_3y_start || '--', data.gp_3y_growth || '--'],
            net_cash: ['Net Cash', data.netCash, 'Cash - Debt', '', data.netCash],
            roc: ['ROC', data.adj_income, 'NWC + Net Fixed Assets', '', data.roc],
            adj_ebit_gross_ppe: ['Adj Op Inc / Gross PP&E', data.adj_income, 'Gross PP&E', data.grossPpe, data.adjEbitGrossPpe],
            capex_adj_income: ['Investment Capex / Adj Op Inc', data.investmentCapex, 'Adj Op Inc', data.adj_income, data.capexAdjIncome],
        };
        const item = map[type];
        if (!item) return;
        $('calc-ticker-badge').textContent = data.ticker;
        $('calc-title').textContent = item[0];
        $('calc-numerator-label').textContent = item[0].split('/')[0] || item[0];
        $('calc-ev-val').textContent = item[1] || '--';
        $('calc-divisor-label').textContent = item[2] || 'Calculation';
        $('calc-divisor-val').textContent = item[3] || '--';
        $('calc-result-val').textContent = item[4] || '--';
        $('calc-breakdown-list').innerHTML = [
            ['Revenue', data.revenue], ['Operating Income', data.income], ['D&A', data.da],
            ['Capex', data.capex], ['Adj Op Inc', data.adj_income], ['Market Cap', data.marketCap],
            ['Net Cash', data.netCash], ['Our EV', data.derivedEnterpriseValue],
        ].map(([label, value]) => `<li><span class="calc-label">${label}</span><span class="calc-val">${value || '--'}</span></li>`).join('');
        showView('calc');
        document.querySelector('.tabs').classList.add('hidden');
        window.scrollTo(0, 0);
    }

    $('calc-back-btn').addEventListener('click', () => {
        document.querySelector('.tabs').classList.remove('hidden');
        showView('scanner');
        requestAnimationFrame(() => window.scrollTo(0, state.previousScroll || 0));
    });

    window.removeTicker = (ticker) => {
        state.watchlist = state.watchlist.filter((item) => item !== ticker);
        save('stock_watchlist', state.watchlist);
        renderTickerTable('watchlist');
    };
});
