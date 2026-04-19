document.addEventListener('DOMContentLoaded', () => {
    function parseMoneyFormat(str) {
        if (!str || str === '--') return 0;
        let val = parseFloat(str);
        if (str.includes('T')) return val * 1e12;
        if (str.includes('B')) return val * 1e9;
        if (str.includes('M')) return val * 1e6;
        return val;
    }
    function formatMoneyJS(num) {
        if (!num) return '--';
        let absVal = Math.abs(num);
        if (absVal >= 1e12) return (num / 1e12).toPrecision(3) + 'T';
        if (absVal >= 1e9) return (num / 1e9).toPrecision(3) + 'B';
        if (absVal >= 1e6) return (num / 1e6).toPrecision(3) + 'M';
        return num.toPrecision(3);
    }
    // Tabs Navigation
    const tabScanner = document.getElementById('tab-scanner');
    const tabWatchlist = document.getElementById('tab-watchlist');
    const viewScanner = document.getElementById('view-scanner');
    const viewWatchlist = document.getElementById('view-watchlist');

    tabScanner.addEventListener('click', () => {
        tabScanner.classList.add('active');
        tabWatchlist.classList.remove('active');
        viewScanner.classList.remove('hidden');
        viewWatchlist.classList.add('hidden');
    });

    tabWatchlist.addEventListener('click', () => {
        tabWatchlist.classList.add('active');
        tabScanner.classList.remove('active');
        viewWatchlist.classList.remove('hidden');
        viewScanner.classList.add('hidden');
        renderWatchlist();
    });

    // Scanner UI
    const form = document.getElementById('search-form');
    const input = document.getElementById('ticker-input');
    const resultContainer = document.getElementById('result-container');
    const resultTicker = document.getElementById('result-ticker');
    const resultStats = document.getElementById('result-stats');
    const resultValue = document.getElementById('result-value');
    const resultMargin = document.getElementById('result-margin');
    const resultEvEbit = document.getElementById('result-ev-ebit');
    const resultCyg = document.getElementById('result-cyg');
    const resultEvCy = document.getElementById('result-evcy');
    const resultNyg = document.getElementById('result-nyg');
    const resultEvNy = document.getElementById('result-evny');
    const resultGp3yGrowthLabel = document.getElementById('result-gp-3y-growth-label');
    const resultGp3yGrowth = document.getElementById('result-gp-3y-growth');
    const resultEv = document.getElementById('result-ev');
    const resultMarketCap = document.getElementById('result-market-cap');
    const resultNetDebt = document.getElementById('result-net-debt');
    const resultDataDate = document.getElementById('result-data-date');
    const refreshDataBtn = document.getElementById('refresh-data-btn');
    const errorMessage = document.getElementById('error-message');
    const spinner = document.getElementById('loading-spinner');
    const glassCard = document.getElementById('glass-card');

    // Calculation View Logic
    const viewCalc = document.getElementById('view-calc');
    const calcBackBtn = document.getElementById('calc-back-btn');
    const calcTickerBadge = document.getElementById('calc-ticker-badge');
    const calcTitle = document.getElementById('calc-title');
    const calcNumeratorLabel = document.getElementById('calc-numerator-label');
    const calcEvVal = document.getElementById('calc-ev-val');
    const calcDivisorLabel = document.getElementById('calc-divisor-label');
    const calcDivisorVal = document.getElementById('calc-divisor-val');
    const calcResultLabel = document.getElementById('calc-result-label');
    const calcResultVal = document.getElementById('calc-result-val');
    const calcBreakdownList = document.getElementById('calc-breakdown-list');
    
    let previousView = viewScanner;
    let latestResultData = null;
    const watchlistDataByTicker = {};

    function renderCalcBreakdown(items) {
        if (!calcBreakdownList) return;
        if (!items || items.length === 0) {
            calcBreakdownList.innerHTML = '<li><span class="calc-label">Breakdown</span><span class="calc-val">--</span></li>';
            return;
        }
        calcBreakdownList.innerHTML = items.map((item) =>
            `<li><span class="calc-label">${item.label}</span><span class="calc-val">${item.value || '--'}</span></li>`
        ).join('');
    }

    function buildBreakdownFromData(data, metricType) {
        if (!data) return [];
        if (metricType === 'adj') {
            return [
                { label: 'Revenue', value: data.revenue || '--' },
                { label: 'Operating Margin', value: data.operating_margin || '--' },
                { label: 'Operating Income (EBIT)', value: data.income || '--' },
                { label: 'Depreciation & Amort.', value: data.da || '--' },
                { label: 'Capex', value: data.capex || '--' },
                { label: 'Max(0, D&A - Capex)', value: data.da_minus_capex || '--' },
                { label: 'Adj EBIT', value: data.adj_income || '--' }
            ];
        }
        if (metricType === 'cy') {
            return [
                { label: 'CY Revenue Estimate', value: data.cy_revenue || '--' },
                { label: 'Adj Margin Used', value: data.margin || '--' },
                { label: 'CY Adj EBIT', value: data.cy_adj_inc || '--' }
            ];
        }
        if (metricType === 'ny') {
            return [
                { label: 'NY Revenue Estimate', value: data.ny_revenue || '--' },
                { label: 'Adj Margin Used', value: data.margin || '--' },
                { label: 'NY Adj EBIT', value: data.ny_adj_inc || '--' }
            ];
        }
        if (metricType === 'gp_3y_growth') {
            const basis = data.gp_3y_basis || 'Gross Profit';
            return [
                { label: `Starting ${basis}`, value: data.gp_3y_start || '--' },
                { label: `Latest ${basis}`, value: data.gp_3y_end || '--' },
                { label: 'Formula', value: '(Latest / Starting - 1) x 100' }
            ];
        }
        return [];
    }

    function openCalcView(ticker, title, ev, divisorName, divisorVal, result, breakdownItems = [], numeratorLabel = 'Current Enterprise Value', resultLabel = 'Final Valuation Multiple') {
        calcTickerBadge.textContent = ticker;
        calcTitle.textContent = title;
        if (calcNumeratorLabel) calcNumeratorLabel.textContent = numeratorLabel;
        calcEvVal.textContent = ev || '--';
        calcDivisorLabel.textContent = divisorName;
        calcDivisorVal.textContent = divisorVal || '--';
        if (calcResultLabel) calcResultLabel.textContent = resultLabel;
        calcResultVal.textContent = result || '--';
        renderCalcBreakdown(breakdownItems);

        // Track current view to know where to go back
        if (!viewScanner.classList.contains('hidden')) previousView = viewScanner;
        else if (!viewWatchlist.classList.contains('hidden')) previousView = viewWatchlist;

        // Hide main views and show calc view
        viewScanner.classList.add('hidden');
        viewWatchlist.classList.add('hidden');
        document.querySelector('.tabs').classList.add('hidden'); // Hide tabs during calc view
        viewCalc.classList.remove('hidden');
        window.scrollTo(0, 0); // Scroll to top
    }

    calcBackBtn.addEventListener('click', () => {
        viewCalc.classList.add('hidden');
        previousView.classList.remove('hidden');
        document.querySelector('.tabs').classList.remove('hidden');
    });

    const showCalcView = (element, title, divisorName, divisorKey, resultKey) => {
        const ticker = document.getElementById('result-ticker').textContent;
        const ev = element.dataset.ev || '--';
        const divisorVal = element.dataset[divisorKey] || '--';
        const resultVal = element.dataset[resultKey] || '--';
        const metricType = element.dataset.metricType || '';
        const breakdown = buildBreakdownFromData(latestResultData, metricType);
        openCalcView(ticker, title, ev, divisorName, divisorVal, resultVal, breakdown);
    };

    resultEvEbit.addEventListener('click', () => showCalcView(resultEvEbit, 'EV / Adj EBIT', 'TTM Adj. Operating Income', 'adj', 'res_adj'));
    resultEvCy.addEventListener('click', () => showCalcView(resultEvCy, 'EV / CY EBIT', 'CY Adj. EBIT (Estimate)', 'cy', 'res_cy'));
    resultEvNy.addEventListener('click', () => showCalcView(resultEvNy, 'EV / NY EBIT', 'NY Adj. EBIT (Estimate)', 'ny', 'res_ny'));
    if (resultGp3yGrowthLabel) {
        resultGp3yGrowthLabel.addEventListener('click', () => {
            if (!latestResultData) return;
            const basis = latestResultData.gp_3y_basis || 'Gross Profit';
            openCalcView(
                resultTicker.textContent,
                `3Y ${basis} Growth`,
                latestResultData.gp_3y_end || '--',
                `Starting ${basis}`,
                latestResultData.gp_3y_start || '--',
                latestResultData.gp_3y_growth || '--',
                buildBreakdownFromData(latestResultData, 'gp_3y_growth'),
                `Latest ${basis}`,
                'Final Growth Rate'
            );
        });
    }

    window.openTableCalcView = function (ticker, title, ev, divisorName, divisorVal, resultVal, metricType = '') {
        const breakdown = buildBreakdownFromData(watchlistDataByTicker[ticker], metricType);
        openCalcView(ticker, title, ev, divisorName, divisorVal, resultVal, breakdown);
    };

    async function fetchAndRenderTicker(ticker, { refresh = false } = {}) {
        if (!ticker) return;

        // Reset state
        resultContainer.classList.remove('hidden');
        resultStats.classList.add('hidden');
        spinner.classList.remove('hidden');
        errorMessage.classList.add('hidden');
        resultTicker.textContent = ticker;
        if (resultDataDate) resultDataDate.textContent = 'As of --';
        glassCard.style.display = 'block';

        try {
            const url = refresh ? `/api/short-interest/${ticker}?refresh=1` : `/api/short-interest/${ticker}`;
            const response = await fetch(url);
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Failed to fetch data');
            }

            // Success state
            spinner.classList.add('hidden');
            resultStats.classList.remove('hidden');
            resultValue.textContent = data.shortFloat;
            if (resultDataDate) {
                const asOf = data.dataDate || '--';
                const pulledAt = data.pulledAt ? data.pulledAt.replace('T', ' ') : '--';
                resultDataDate.textContent = `As of ${asOf} • pulled ${pulledAt}`;
            }

            // Back-calculate denominators
            const evRaw = parseMoneyFormat(data.ev);
            const cyRatio = parseFloat(data.ev_cy_ebit);
            const nyRatio = parseFloat(data.ev_ny_ebit);
            data.cy_adj_inc = data.cy_adj_inc || ((evRaw && cyRatio) ? formatMoneyJS(evRaw / cyRatio) : '--');
            data.ny_adj_inc = data.ny_adj_inc || ((evRaw && nyRatio) ? formatMoneyJS(evRaw / nyRatio) : '--');

            // Store raw variables for tooltip
            const setTooltips = (el) => {
                el.dataset.ev = data.ev || '--';
                el.dataset.adj = data.adj_income || '--';
                el.dataset.cy = data.cy_adj_inc || '--';
                el.dataset.ny = data.ny_adj_inc || '--';
                el.dataset.res_adj = data.ev_adj_ebit || '--';
                el.dataset.res_cy = data.ev_cy_ebit || '--';
                el.dataset.res_ny = data.ev_ny_ebit || '--';
            };
            setTooltips(resultEvEbit);
            setTooltips(resultEvCy);
            setTooltips(resultEvNy);
            resultEvEbit.dataset.metricType = 'adj';
            resultEvCy.dataset.metricType = 'cy';
            resultEvNy.dataset.metricType = 'ny';
            latestResultData = data;

            resultEvEbit.textContent = data.ev_adj_ebit;
            resultMargin.textContent = data.margin;
            resultCyg.textContent = data.cy_growth;
            resultEvCy.textContent = data.ev_cy_ebit;
            resultNyg.textContent = data.ny_growth;
            if (resultGp3yGrowth) resultGp3yGrowth.textContent = data.gp_3y_growth || '--';
            if (resultGp3yGrowthLabel) {
                resultGp3yGrowthLabel.textContent = data.gp_3y_basis === 'Revenue' ? '3Y Revenue Growth' : '3Y GP Growth';
            }
            resultEvNy.textContent = data.ev_ny_ebit;
            if (resultEv) resultEv.textContent = data.ev;
            if (resultMarketCap) resultMarketCap.textContent = data.marketCap || '--';
            if (resultNetDebt) resultNetDebt.textContent = data.netDebt || '--';

            // Trigger animation
            resultStats.classList.remove('pop');
            void resultStats.offsetWidth; // trigger reflow
            resultStats.classList.add('pop');

        } catch (error) {
            // Error state
            spinner.classList.add('hidden');
            glassCard.style.display = 'none';
            errorMessage.textContent = error.message;
            errorMessage.classList.remove('hidden');
        }
    }

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const ticker = input.value.trim().toUpperCase();
        await fetchAndRenderTicker(ticker, { refresh: false });
    });

    if (refreshDataBtn) {
        refreshDataBtn.addEventListener('click', async () => {
            const ticker = resultTicker.textContent.trim().toUpperCase();
            if (!ticker || ticker === '--') return;
            await fetchAndRenderTicker(ticker, { refresh: true });
        });
    }

    // Watchlist Logic
    const watchlistForm = document.getElementById('watchlist-form');
    const watchlistInput = document.getElementById('watchlist-ticker-input');
    const watchlistBody = document.getElementById('watchlist-body');

    let watchlist = JSON.parse(localStorage.getItem('stock_watchlist')) || [];

    watchlistForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const ticker = watchlistInput.value.trim().toUpperCase();
        if (ticker && !watchlist.includes(ticker)) {
            watchlist.push(ticker);
            localStorage.setItem('stock_watchlist', JSON.stringify(watchlist));
            watchlistInput.value = '';
            renderWatchlist();
        }
    });

    window.removeTicker = function (ticker) {
        watchlist = watchlist.filter(t => t !== ticker);
        localStorage.setItem('stock_watchlist', JSON.stringify(watchlist));
        delete watchlistDataByTicker[ticker];
        renderWatchlist();
    }

    function watchlistLoadingRowHtml(ticker) {
        return `<tr id="watch-row-${ticker}">
            <td>${ticker}</td>
            <td colspan="10" style="text-align:center; color: var(--text-secondary);">Loading...</td>
            <td><button class="remove-btn" onclick="removeTicker('${ticker}')">X</button></td>
        </tr>`;
    }

    function watchlistErrorRowHtml(ticker, message) {
        return `<tr id="watch-row-${ticker}">
            <td>${ticker}</td>
            <td colspan="10" style="color:var(--error-color);">${message}</td>
            <td><button class="remove-btn" onclick="removeTicker('${ticker}')">X</button></td>
        </tr>`;
    }

    function watchlistDataRowHtml(ticker, data) {
        return `<tr id="watch-row-${ticker}">
            <td>${data.ticker}</td>
            <td>${data.shortFloat}</td>
            <td>${data.ev}</td>
            <td>${data.marketCap || '--'}</td>
            <td class="clickable-table-metric" onclick="openTableCalcView('${data.ticker}', 'EV / Adj EBIT', '${data.ev}', 'Adj. Operating Income:', '${data.adj_income}', '${data.ev_adj_ebit}', 'adj')">${data.ev_adj_ebit || '--'}</td>
            <td class="clickable-table-metric" onclick="openTableCalcView('${data.ticker}', 'EV / CY EBIT', '${data.ev}', 'CY Adj. EBIT (Estimate):', '${data.cy_adj_inc}', '${data.ev_cy_ebit}', 'cy')">${data.ev_cy_ebit || '--'}</td>
            <td class="clickable-table-metric" onclick="openTableCalcView('${data.ticker}', 'EV / NY EBIT', '${data.ev}', 'NY Adj. EBIT (Estimate):', '${data.ny_adj_inc}', '${data.ev_ny_ebit}', 'ny')">${data.ev_ny_ebit || '--'}</td>
            <td>${data.margin}</td>
            <td>${data.cy_growth}</td>
            <td>${data.ny_growth}</td>
            <td>${data.netDebt || '--'}</td>
            <td><button class="remove-btn" onclick="removeTicker('${ticker}')">X</button></td>
        </tr>`;
    }

    async function renderWatchlist() {
        if (watchlist.length === 0) {
            watchlistBody.innerHTML = '<tr><td colspan="12" style="text-align:center; color: var(--text-secondary);">No tickers in watchlist. Add some!</td></tr>';
            return;
        }

        // Render immediately with cached rows where available.
        watchlistBody.innerHTML = watchlist.map((ticker) => {
            const cached = watchlistDataByTicker[ticker];
            return cached ? watchlistDataRowHtml(ticker, cached) : watchlistLoadingRowHtml(ticker);
        }).join('');

        // Fetch each ticker independently so one loading row never blocks others.
        for (const ticker of watchlist) {
            try {
                const response = await fetch(`/api/short-interest/${ticker}`);
                if (!response.ok) {
                    const row = document.getElementById(`watch-row-${ticker}`);
                    if (row) row.outerHTML = watchlistErrorRowHtml(ticker, 'Error');
                    continue;
                }
                const data = await response.json();
                const evRaw = parseMoneyFormat(data.ev);
                const cyRatio = parseFloat(data.ev_cy_ebit);
                const nyRatio = parseFloat(data.ev_ny_ebit);
                data.cy_adj_inc = data.cy_adj_inc || ((evRaw && cyRatio) ? formatMoneyJS(evRaw / cyRatio) : '--');
                data.ny_adj_inc = data.ny_adj_inc || ((evRaw && nyRatio) ? formatMoneyJS(evRaw / nyRatio) : '--');
                watchlistDataByTicker[ticker] = data;
                const row = document.getElementById(`watch-row-${ticker}`);
                if (row) row.outerHTML = watchlistDataRowHtml(ticker, data);
            } catch (err) {
                const row = document.getElementById(`watch-row-${ticker}`);
                if (row) row.outerHTML = watchlistErrorRowHtml(ticker, 'Network Error');
            }
        }
    }
});
