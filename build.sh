ucc-gen build --ta-version $1
find output/qualtrics_audit -name ".*" -type f -delete
ucc-gen package --path output/qualtrics_audit
