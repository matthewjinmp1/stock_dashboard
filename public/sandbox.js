(function attachStockSandbox(global) {
    'use strict';

    const STORAGE_KEY = 'stock_sandbox_layout_v1';
    const LAYOUT_VERSION = 3;
    const GRID_COLUMNS = 1200;
    const GRID_ROW_HEIGHT = 1;
    const PREVIOUS_GRID_COLUMNS = 120;
    const PREVIOUS_GRID_ROW_HEIGHT = 4;
    const LEGACY_COLUMNS = 12;
    const LEGACY_ROW_HEIGHT = 74;
    const FORMATS = new Set(['number', 'percent', 'money', 'multiple']);
    const ALLOWED_FUNCTIONS = new Set(['abs', 'ceil', 'floor', 'max', 'min', 'pow', 'round', 'sqrt']);
    const ALLOWED_OPERATORS = new Set(['add', 'subtract', 'multiply', 'divide', 'pow', 'mod', 'unaryMinus', 'unaryPlus']);
    const METRIC_LABELS = {
        margin: 'Adjusted Operating Margin',
        grossMargin: 'Gross Margin',
        cy_growth: 'CY Revenue Growth',
        ny_growth: 'NY Revenue Growth',
        marketCap: 'Market Cap',
        derivedEnterpriseValue: 'Our Enterprise Value',
        revenue: 'Revenue',
        grossProfit: 'Gross Profit',
        adj_income: 'Adjusted Operating Income',
        afterTaxAdjIncome: 'Adjusted Net Income',
        medianTaxRate: 'Median Tax Rate',
        currentPrice: 'Current Price',
        dividendYield: 'Dividend Yield',
        beta: 'Beta',
        shortFloat: 'Short % Shares Outstanding',
        priceSales: 'Price / Sales',
        priceGrossProfit: 'Price / Gross Profit',
        ev_adj_ebit: 'Adjusted PE',
        ev_cy_ebit: 'CY Adjusted PE',
        ev_ny_ebit: 'NY Adjusted PE',
        netDebtAdjIncome: 'Net Debt / Adjusted Net Income',
        capexAdjIncome: 'Investment Rate',
        rndAdjIncome: 'R&D / Adjusted Operating Income',
        roc: 'ROC',
        adjEbitGrossPpe: 'ROGPPE',
    };

    function escapeHtml(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function sandboxVariableName(value) {
        const variable = String(value || '')
            .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
            .replace(/[^A-Za-z0-9_]+/g, '_')
            .replace(/^_+|_+$/g, '')
            .replace(/_+/g, '_')
            .toLowerCase();
        if (!variable) return 'value';
        return /^\d/.test(variable) ? `value_${variable}` : variable;
    }

    function titleFromKey(key) {
        return METRIC_LABELS[key] || String(key || '')
            .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
            .replace(/[_-]+/g, ' ')
            .replace(/\b\w/g, (letter) => letter.toUpperCase());
    }

    function shouldShowMetricOption(selected, query, searchText) {
        const normalizedQuery = String(query || '').trim().toLowerCase();
        return Boolean(selected) || Boolean(normalizedQuery && String(searchText || '').toLowerCase().includes(normalizedQuery));
    }

    function entryRaw(entry) {
        const raw = entry && typeof entry === 'object' && 'raw' in entry ? entry.raw : entry;
        if (raw === null || raw === undefined || raw === '') return null;
        const number = Number(raw);
        return Number.isFinite(number) ? number : null;
    }

    function entryDisplay(entry) {
        if (entry && typeof entry === 'object' && entry.display !== undefined) return String(entry.display || '--');
        if (entry === null || entry === undefined || entry === '') return '--';
        return String(entry);
    }

    function sandboxMetricCatalog(data) {
        const metrics = data?.metrics || {};
        const seenVariables = new Set();
        return Object.keys(metrics).sort((left, right) => titleFromKey(left).localeCompare(titleFromKey(right))).map((key) => {
            let variable = sandboxVariableName(key);
            let suffix = 2;
            while (seenVariables.has(variable)) {
                variable = `${sandboxVariableName(key)}_${suffix}`;
                suffix += 1;
            }
            seenVariables.add(variable);
            const entry = metrics[key];
            return {
                key,
                label: titleFromKey(key),
                variable,
                raw: entryRaw(entry),
                display: entryDisplay(entry),
                kind: entry && typeof entry === 'object' ? entry.kind || 'number' : 'number',
            };
        });
    }

    function createId(prefix) {
        const random = global.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
        return `${prefix}-${random}`;
    }

    function cleanFormula(formula) {
        return {
            id: String(formula?.id || createId('formula')),
            name: String(formula?.name || 'Custom metric').slice(0, 80),
            expression: String(formula?.expression || '').slice(0, 500),
            format: FORMATS.has(formula?.format) ? formula.format : 'number',
        };
    }

    function cleanWidget(widget, index) {
        const numeric = (value, fallback, min, max) => {
            const number = Number(value);
            return Math.min(max, Math.max(min, Number.isFinite(number) ? number : fallback));
        };
        return {
            id: String(widget?.id || createId('widget')),
            title: String(widget?.title || `Box ${index + 1}`).slice(0, 80),
            x: numeric(widget?.x, (index * Math.round(GRID_COLUMNS / 3)) % GRID_COLUMNS, 0, GRID_COLUMNS - 1),
            y: numeric(widget?.y, 0, 0, 400000),
            w: numeric(widget?.w, Math.round(GRID_COLUMNS / 3), Math.round(GRID_COLUMNS / 12), GRID_COLUMNS),
            h: numeric(widget?.h, 296, 80, 40000),
            metrics: Array.isArray(widget?.metrics) ? [...new Set(widget.metrics.map(String))].slice(0, 30) : [],
            formulas: Array.isArray(widget?.formulas) ? widget.formulas.slice(0, 20).map(cleanFormula) : [],
        };
    }

    function normalizeSandboxLayout(layout) {
        const widgets = Array.isArray(layout?.widgets) ? layout.widgets.slice(0, 30) : [];
        let normalizedWidgets = widgets;
        if (Number(layout?.version) < LAYOUT_VERSION) {
            const isLegacyLayout = Number(layout?.version) < 2;
            const sourceColumns = isLegacyLayout ? LEGACY_COLUMNS : PREVIOUS_GRID_COLUMNS;
            const sourceRowHeight = isLegacyLayout ? LEGACY_ROW_HEIGHT : PREVIOUS_GRID_ROW_HEIGHT;
            const horizontalScale = GRID_COLUMNS / sourceColumns;
            const verticalScale = sourceRowHeight / GRID_ROW_HEIGHT;
            const reasonableLegacyLastRow = Math.max(12, widgets.length * 12);
            const sourceNumber = (value, fallback, min, max) => {
                const number = Number(value);
                return Math.min(max, Math.max(min, Number.isFinite(number) ? number : fallback));
            };
            normalizedWidgets = widgets.map((widget, index) => {
                const sourceY = sourceNumber(widget?.y, 0, 0, isLegacyLayout ? 1000 : 100000);
                const safeY = isLegacyLayout && sourceY > reasonableLegacyLastRow ? index * 4 : sourceY;
                return {
                    ...widget,
                    x: Math.round(sourceNumber(widget?.x, (index * sourceColumns / 3) % sourceColumns, 0, sourceColumns - 1) * horizontalScale),
                    y: Math.round(safeY * verticalScale),
                    w: Math.round(sourceNumber(widget?.w, sourceColumns / 3, sourceColumns / 12, sourceColumns) * horizontalScale),
                    h: Math.round(sourceNumber(widget?.h, isLegacyLayout ? 4 : 74, isLegacyLayout ? 2 : 20, isLegacyLayout ? 12 : 10000) * verticalScale),
                };
            });
        }
        const cleaned = normalizedWidgets.map(cleanWidget);
        return { version: LAYOUT_VERSION, widgets: cleaned };
    }

    function parseLayout(storage) {
        try {
            const parsed = JSON.parse(storage?.getItem(STORAGE_KEY) || '{}');
            const normalized = normalizeSandboxLayout(parsed);
            if (parsed?.version !== normalized.version) {
                storage?.setItem?.(STORAGE_KEY, JSON.stringify(normalized));
            }
            return normalized;
        } catch (error) {
            storage?.removeItem?.(STORAGE_KEY);
            return normalizeSandboxLayout({});
        }
    }

    function validateExpression(expression, knownVariables) {
        if (!String(expression || '').trim()) return { node: null, dependencies: [], error: 'Add a formula.' };
        if (!global.math?.parse) return { node: null, dependencies: [], error: 'Formula engine unavailable.' };
        try {
            const node = global.math.parse(expression);
            const dependencies = new Set();
            node.traverse((child) => {
                const allowedTypes = new Set(['ConstantNode', 'SymbolNode', 'OperatorNode', 'ParenthesisNode', 'FunctionNode']);
                if (!allowedTypes.has(child.type)) throw new Error(`${child.type.replace('Node', '')} expressions are not allowed.`);
                if (child.type === 'OperatorNode' && !ALLOWED_OPERATORS.has(child.fn)) throw new Error('That operator is not allowed.');
                if (child.type === 'FunctionNode') {
                    const functionName = child.fn?.name;
                    if (!ALLOWED_FUNCTIONS.has(functionName)) throw new Error(`Function "${functionName || ''}" is not allowed.`);
                }
                if (child.type === 'SymbolNode') {
                    if (ALLOWED_FUNCTIONS.has(child.name) || child.name === 'pi' || child.name === 'e') return;
                    if (!knownVariables.has(child.name)) throw new Error(`Unknown value "${child.name}".`);
                    dependencies.add(child.name);
                }
            });
            return { node, dependencies: [...dependencies], error: '' };
        } catch (error) {
            return { node: null, dependencies: [], error: error.message || 'Invalid formula.' };
        }
    }

    function evaluateFormulaSet(data, layout) {
        const catalog = sandboxMetricCatalog(data);
        const scope = new Map();
        catalog.forEach((metric) => {
            if (metric.raw !== null) scope.set(metric.variable, metric.raw);
        });
        scope.set('pi', Math.PI);
        scope.set('e', Math.E);

        const formulas = layout.widgets.flatMap((widget) => widget.formulas.map((formula) => ({ ...formula, widgetId: widget.id })));
        const metricVariables = new Set(catalog.map((metric) => metric.variable));
        const aliases = new Map();
        formulas.forEach((formula) => {
            const alias = sandboxVariableName(formula.name);
            aliases.set(alias, (aliases.get(alias) || 0) + 1);
        });
        const knownVariables = new Set([...catalog.map((metric) => metric.variable), ...aliases.keys(), 'pi', 'e']);
        const results = new Map();
        let pending = formulas.map((formula) => ({ ...formula, alias: sandboxVariableName(formula.name) }));

        pending.filter((formula) => aliases.get(formula.alias) > 1).forEach((formula) => {
            results.set(formula.id, { raw: null, error: `Formula name "${formula.name}" is duplicated.` });
        });
        pending.filter((formula) => metricVariables.has(formula.alias)).forEach((formula) => {
            results.set(formula.id, { raw: null, error: `Formula name "${formula.name}" conflicts with an existing value.` });
        });
        pending = pending.filter((formula) => aliases.get(formula.alias) === 1 && !metricVariables.has(formula.alias));

        for (let pass = 0; pass <= formulas.length && pending.length; pass += 1) {
            const next = [];
            let progressed = false;
            pending.forEach((formula) => {
                const validation = validateExpression(formula.expression, knownVariables);
                if (validation.error) {
                    results.set(formula.id, { raw: null, error: validation.error });
                    progressed = true;
                    return;
                }
                const waiting = validation.dependencies.some((name) => aliases.has(name) && !scope.has(name));
                const unavailable = validation.dependencies.find((name) => !aliases.has(name) && !scope.has(name));
                if (unavailable) {
                    results.set(formula.id, { raw: null, error: `${unavailable} is unavailable for this stock.` });
                    progressed = true;
                    return;
                }
                if (waiting) {
                    next.push(formula);
                    return;
                }
                try {
                    const raw = Number(validation.node.compile().evaluate(scope));
                    if (!Number.isFinite(raw)) throw new Error('Formula did not return a finite number.');
                    scope.set(formula.alias, raw);
                    results.set(formula.id, { raw, error: '' });
                } catch (error) {
                    results.set(formula.id, { raw: null, error: error.message || 'Formula could not be calculated.' });
                }
                progressed = true;
            });
            pending = next;
            if (!progressed) break;
        }
        pending.forEach((formula) => results.set(formula.id, { raw: null, error: 'Circular formula reference.' }));
        return { catalog, results };
    }

    function compactNumber(value) {
        return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);
    }

    function formatFormulaValue(value, format) {
        if (!Number.isFinite(value)) return '--';
        if (format === 'percent') return `${compactNumber(value * 100)}%`;
        if (format === 'money') {
            const absolute = Math.abs(value);
            if (absolute >= 1e12) return `${compactNumber(value / 1e12)}T`;
            if (absolute >= 1e9) return `${compactNumber(value / 1e9)}B`;
            if (absolute >= 1e6) return `${compactNumber(value / 1e6)}M`;
            return compactNumber(value);
        }
        if (format === 'multiple') return `${compactNumber(value)}x`;
        return compactNumber(value);
    }

    function createSandbox(options) {
        const storage = options?.storage || global.localStorage;
        let layout = parseLayout(storage);
        let root = null;
        let data = null;
        let grid = null;
        let draft = null;

        function persist() {
            try {
                storage?.setItem?.(STORAGE_KEY, JSON.stringify(layout));
            } catch (error) {
                console.warn('Unable to save Sandbox layout.', error);
            }
        }

        function destroyGrid() {
            if (grid?.destroy) grid.destroy(false);
            grid = null;
        }

        function metricValue(metric) {
            return `<div class="sandbox-value"><span>${escapeHtml(metric.label)}</span><strong>${escapeHtml(metric.display)}</strong></div>`;
        }

        function formulaValue(formula, result) {
            const display = result?.error ? '--' : formatFormulaValue(result?.raw, formula.format);
            const error = result?.error ? `<small class="sandbox-formula-error">${escapeHtml(result.error)}</small>` : '';
            return `<div class="sandbox-value"><span>${escapeHtml(formula.name)}</span><strong>${escapeHtml(display)}</strong>${error}</div>`;
        }

        function widgetHtml(widget, evaluated) {
            const byKey = new Map(evaluated.catalog.map((metric) => [metric.key, metric]));
            const metrics = widget.metrics.map((key) => byKey.get(key)).filter(Boolean).map(metricValue).join('');
            const formulas = widget.formulas.map((formula) => formulaValue(formula, evaluated.results.get(formula.id))).join('');
            const empty = metrics || formulas ? '' : '<p class="sandbox-widget-empty">Edit this box to add metrics or formulas.</p>';
            return `<div class="grid-stack-item" gs-id="${escapeHtml(widget.id)}" gs-x="${widget.x}" gs-y="${widget.y}" gs-w="${widget.w}" gs-h="${widget.h}">
                <section class="grid-stack-item-content sandbox-widget">
                    <div class="sandbox-widget-header sandbox-widget-drag">
                        <h3>${escapeHtml(widget.title)}</h3>
                        <div class="sandbox-widget-actions">
                            <button type="button" data-sandbox-edit="${escapeHtml(widget.id)}">Edit</button>
                            <button type="button" data-sandbox-delete="${escapeHtml(widget.id)}">Delete</button>
                        </div>
                    </div>
                    <div class="sandbox-widget-values">${metrics}${formulas}${empty}</div>
                </section>
            </div>`;
        }

        function workspaceHtml() {
            const evaluated = evaluateFormulaSet(data, layout);
            const widgets = layout.widgets.map((widget) => widgetHtml(widget, evaluated)).join('');
            const empty = layout.widgets.length ? '' : `<div class="sandbox-empty">
                <h3>Build your own metric board</h3>
                <p>Add a box, select live stock metrics, and combine raw values with formulas.</p>
                <button type="button" data-sandbox-add>Add your first box</button>
            </div>`;
            return `<section class="sandbox-workspace">
                <div class="sandbox-toolbar">
                    <div><h2>Sandbox</h2><p>Custom boxes use the current stock's raw values.</p></div>
                    <button type="button" data-sandbox-add>Add box</button>
                </div>
                ${empty}<div class="grid-stack sandbox-grid">${widgets}</div>
                <dialog class="sandbox-dialog" aria-labelledby="sandbox-dialog-title"></dialog>
            </section>`;
        }

        function updateLayoutFromGrid(items) {
            (items || []).forEach((item) => {
                const id = item.id || item.el?.getAttribute?.('gs-id');
                const widget = layout.widgets.find((candidate) => candidate.id === id);
                if (!widget) return;
                ['x', 'y', 'w', 'h'].forEach((key) => {
                    if (Number.isFinite(item[key])) widget[key] = item[key];
                });
            });
            persist();
        }

        function wireGrid() {
            if (!global.GridStack || !root?.querySelector?.('.sandbox-grid')) return;
            grid = global.GridStack.init({
                column: GRID_COLUMNS,
                cellHeight: GRID_ROW_HEIGHT,
                float: true,
                margin: 10,
                minRow: 220,
                handle: '.sandbox-widget-drag',
                resizable: { handles: 'e,se,s,sw,w' },
                columnOpts: {
                    columnMax: GRID_COLUMNS,
                    breakpoints: [{ w: 720, c: 1 }],
                },
            }, root.querySelector('.sandbox-grid'));
            grid.on('change', (_event, items) => updateLayoutFromGrid(items));
        }

        function rerender() {
            if (root && data) render(root, data);
        }

        function addBox() {
            const nextRow = layout.widgets.reduce((lastRow, widget) => Math.max(lastRow, widget.y + widget.h), 0);
            const widget = cleanWidget({
                title: `Box ${layout.widgets.length + 1}`,
                y: nextRow,
                metrics: [],
            }, layout.widgets.length);
            layout.widgets.push(widget);
            persist();
            rerender();
            openEditor(widget.id);
        }

        function removeBox(id) {
            layout.widgets = layout.widgets.filter((widget) => widget.id !== id);
            persist();
            rerender();
        }

        function captureDraft(dialog) {
            if (!draft || !dialog) return;
            const title = dialog.querySelector('[name="sandbox-title"]');
            if (title) draft.title = title.value;
            draft.metrics = [...dialog.querySelectorAll('[name="sandbox-metric"]:checked')].map((input) => input.value);
            dialog.querySelectorAll('[data-formula-id]').forEach((row) => {
                const formula = draft.formulas.find((item) => item.id === row.dataset.formulaId);
                if (!formula) return;
                formula.name = row.querySelector('[name="formula-name"]')?.value || '';
                formula.expression = row.querySelector('[name="formula-expression"]')?.value || '';
                formula.format = row.querySelector('[name="formula-format"]')?.value || 'number';
            });
        }

        function formulaRows(catalog) {
            const references = [
                ...catalog.map((metric) => ({ variable: metric.variable, label: metric.label })),
                ...draft.formulas.map((formula) => ({ variable: sandboxVariableName(formula.name), label: formula.name })),
            ];
            return draft.formulas.map((formula) => `<div class="sandbox-formula-row" data-formula-id="${escapeHtml(formula.id)}">
                <label>Name<input name="formula-name" maxlength="80" value="${escapeHtml(formula.name)}"></label>
                <label>Formula<input name="formula-expression" maxlength="500" spellcheck="false" value="${escapeHtml(formula.expression)}" placeholder="market_cap / revenue"></label>
                <label>Display<select name="formula-format">
                    ${['number', 'percent', 'money', 'multiple'].map((format) => `<option value="${format}"${formula.format === format ? ' selected' : ''}>${titleFromKey(format)}</option>`).join('')}
                </select></label>
                <label>Insert value<select data-insert-reference><option value="">Choose value</option>${references.map((item) => `<option value="${escapeHtml(item.variable)}">${escapeHtml(item.label)} (${escapeHtml(item.variable)})</option>`).join('')}</select></label>
                <button type="button" class="sandbox-remove-formula" data-remove-formula="${escapeHtml(formula.id)}">Remove formula</button>
                <code class="sandbox-formula-alias">Result name: ${escapeHtml(sandboxVariableName(formula.name))}</code>
            </div>`).join('');
        }

        function editorHtml(catalog) {
            const searchItems = catalog.map((metric) => `<label class="sandbox-metric-option${draft.metrics.includes(metric.key) ? '' : ' hidden'}" data-metric-option="${escapeHtml(`${metric.label} ${metric.variable}`.toLowerCase())}">
                <input type="checkbox" name="sandbox-metric" value="${escapeHtml(metric.key)}"${draft.metrics.includes(metric.key) ? ' checked' : ''}>
                <span>${escapeHtml(metric.label)}<code>${escapeHtml(metric.variable)}</code></span>
            </label>`).join('');
            return `<form class="sandbox-editor-form">
                <div class="sandbox-editor-header"><div><h2 id="sandbox-dialog-title">Edit box</h2><p>Choose metrics or create formulas from raw values.</p></div><button type="button" class="sandbox-dialog-close" data-sandbox-cancel aria-label="Close">x</button></div>
                <label class="sandbox-title-field">Box name<input name="sandbox-title" maxlength="80" value="${escapeHtml(draft.title)}" required></label>
                <section class="sandbox-editor-section"><div class="sandbox-editor-heading"><div><h3>Metrics</h3><p>Selected metrics stay visible. Search to add another.</p></div><input type="search" name="metric-search" placeholder="Search to add a metric"></div><div class="sandbox-metric-picker">${searchItems}</div><p class="sandbox-metric-empty" data-metric-empty${draft.metrics.length ? ' hidden' : ''}>Search to add metrics.</p></section>
                <section class="sandbox-editor-section"><div class="sandbox-editor-heading"><div><h3>Formulas</h3><p>Use value names such as <code>market_cap</code>. Percent values are decimals.</p></div><button type="button" data-add-formula>Add formula</button></div><div class="sandbox-formulas">${formulaRows(catalog)}</div></section>
                <footer><button type="button" data-sandbox-cancel>Cancel</button><button type="submit" class="sandbox-primary-action">Save box</button></footer>
            </form>`;
        }

        function openEditor(id) {
            const widget = layout.widgets.find((candidate) => candidate.id === id);
            const dialog = root?.querySelector?.('.sandbox-dialog');
            if (!widget || !dialog) return;
            draft = cleanWidget(JSON.parse(JSON.stringify(widget)), 0);
            const catalog = sandboxMetricCatalog(data);

            function refreshEditor() {
                dialog.innerHTML = editorHtml(catalog);
                wireEditor();
            }

            function wireEditor() {
                const form = dialog.querySelector('.sandbox-editor-form');
                const search = dialog.querySelector('[name="metric-search"]');
                const filterMetricOptions = () => {
                    const query = search?.value || '';
                    let visibleCount = 0;
                    dialog.querySelectorAll('[data-metric-option]').forEach((option) => {
                        const selected = Boolean(option.querySelector('[name="sandbox-metric"]')?.checked);
                        const visible = shouldShowMetricOption(selected, query, option.dataset.metricOption);
                        option.classList.toggle('hidden', !visible);
                        if (visible) visibleCount += 1;
                    });
                    const empty = dialog.querySelector('[data-metric-empty]');
                    if (empty) {
                        empty.textContent = String(query || '').trim() ? 'No matching metrics.' : 'Search to add metrics.';
                        empty.classList.toggle('hidden', visibleCount > 0);
                    }
                };
                form?.addEventListener('input', () => captureDraft(dialog));
                form?.addEventListener('change', () => captureDraft(dialog));
                form?.addEventListener('submit', (event) => {
                    event.preventDefault();
                    captureDraft(dialog);
                    const target = layout.widgets.find((candidate) => candidate.id === id);
                    if (!target) return;
                    Object.assign(target, cleanWidget(draft, 0), { id: target.id, x: target.x, y: target.y, w: target.w, h: target.h });
                    persist();
                    dialog.close();
                    draft = null;
                    rerender();
                });
                dialog.querySelectorAll('[data-sandbox-cancel]').forEach((button) => button.addEventListener('click', () => dialog.close()));
                dialog.querySelector('[data-add-formula]')?.addEventListener('click', () => {
                    captureDraft(dialog);
                    draft.formulas.push(cleanFormula({ name: `Formula ${draft.formulas.length + 1}` }));
                    refreshEditor();
                });
                dialog.querySelectorAll('[data-remove-formula]').forEach((button) => button.addEventListener('click', () => {
                    captureDraft(dialog);
                    draft.formulas = draft.formulas.filter((formula) => formula.id !== button.dataset.removeFormula);
                    refreshEditor();
                }));
                dialog.querySelectorAll('[data-insert-reference]').forEach((select) => select.addEventListener('change', () => {
                    const row = select.closest('[data-formula-id]');
                    const input = row?.querySelector('[name="formula-expression"]');
                    if (!input || !select.value) return;
                    const start = input.selectionStart ?? input.value.length;
                    input.value = `${input.value.slice(0, start)}${select.value}${input.value.slice(input.selectionEnd ?? start)}`;
                    input.focus();
                    input.setSelectionRange(start + select.value.length, start + select.value.length);
                    select.value = '';
                    captureDraft(dialog);
                }));
                search?.addEventListener('input', filterMetricOptions);
                dialog.querySelectorAll('[name="sandbox-metric"]').forEach((checkbox) => checkbox.addEventListener('change', filterMetricOptions));
            }

            refreshEditor();
            if (!dialog.open) dialog.showModal();
        }

        function render(target, nextData) {
            destroyGrid();
            root = target;
            data = nextData || {};
            root.innerHTML = workspaceHtml();
            root.querySelectorAll('[data-sandbox-add]').forEach((button) => button.addEventListener('click', addBox));
            root.querySelectorAll('[data-sandbox-edit]').forEach((button) => button.addEventListener('click', () => openEditor(button.dataset.sandboxEdit)));
            root.querySelectorAll('[data-sandbox-delete]').forEach((button) => button.addEventListener('click', () => removeBox(button.dataset.sandboxDelete)));
            wireGrid();
        }

        return {
            render,
            destroy: destroyGrid,
            getLayout: () => JSON.parse(JSON.stringify(layout)),
        };
    }

    global.StockSandbox = {
        STORAGE_KEY,
        createSandbox,
        evaluateFormulaSet,
        formatFormulaValue,
        normalizeSandboxLayout,
        sandboxMetricCatalog,
        sandboxVariableName,
        shouldShowMetricOption,
        validateExpression,
    };
}(window));
