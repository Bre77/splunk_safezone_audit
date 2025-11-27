#!/bin/bash
ucc-gen build --ta-version $1
find output/safezone_audit -name ".*" -type f -delete
find output/safezone_audit -name "__pycache__" -type d -exec rm -rf {} +
ucc-gen package --path output/safezone_audit
