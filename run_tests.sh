#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Running stock_analysis test suite..."
python3 -m unittest discover -s tests -v
node tests/frontend_assumptions_test.js
