ucc-gen build --ta-version $1
find output/safezone_audit -name ".*" -type f -delete
ucc-gen package --path output/safezone_audit
