#!/bin/sh

export YENOT_PORT=8088
export YENOT_DB_URL=postgresql:///my_coverage_test

rm -rf .coverage
rm -rf .coverage.*
rm -rf htmlcov
COVERAGE_PROCESS_START=.coveragerc pytest tests
COVERAGE_PROCESS_START=.coveragerc python tests/end-to-end.py
coverage combine
coverage html
xdg-open htmlcov/index.html &>/dev/null
