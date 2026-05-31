#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

started_at="$(python3 -c 'import time; print(time.time())')"
python_test_count="$(python3 - <<'PY'
import unittest
suite = unittest.defaultTestLoader.discover("tests")
print(suite.countTestCases())
PY
)"
frontend_assert_count="$(python3 - <<'PY'
from pathlib import Path
text = Path("tests/frontend_assumptions_test.js").read_text()
print(text.count("assert"))
PY
)"
coverage_available=0
if python3 -c "import coverage" >/dev/null 2>&1; then
    coverage_available=1
fi

echo "Running stock_analysis test suite..."
echo "Python tests discovered: ${python_test_count}"
echo "Frontend assertion references: ${frontend_assert_count}"

if [[ "${coverage_available}" == "1" ]]; then
    python3 -m coverage erase
    python3 -m coverage run --source=server,statements,formatters,cache_store -m unittest discover -s tests -v
else
    echo "Coverage: unavailable (install Python package 'coverage' to enable it)"
    python3 -m unittest discover -s tests -v
fi

node tests/frontend_assumptions_test.js

if [[ "${coverage_available}" == "1" ]]; then
    echo
    echo "Python coverage:"
    python3 -m coverage report -m server.py statements.py formatters.py cache_store.py
fi

finished_at="$(python3 -c 'import time; print(time.time())')"
elapsed="$(python3 - <<PY
started = float("${started_at}")
finished = float("${finished_at}")
print(f"{finished - started:.2f}s")
PY
)"

echo
echo "Test summary:"
echo "  Python tests: ${python_test_count}"
echo "  Frontend assertion references: ${frontend_assert_count}"
echo "  Coverage enabled: $([[ "${coverage_available}" == "1" ]] && echo yes || echo no)"
echo "  Elapsed: ${elapsed}"
