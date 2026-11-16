#!/bin/sh
# `legion local`'s signatures and attestations checked by real cosign, and
# its SBOM by an SPDX validator: what Legion makes is what other tools
# read. From Legion's root, on a host with cosign and pyspdxtools (from
# spdx-tools) on PATH, or named by $COSIGN and $PYSPDXTOOLS:
#
#   scripts/attest-check.sh EMPTY-DIR
#
# It builds legion and the handoff test's registry (plain HTTP, in
# memory), pushes a signed image, attests its provenance and SBOM, then has
# cosign verify the signature and both attestations with the key, refuse
# them with another key, and the validator read the SBOM. It exits 0 only
# if every check came out as it should.
set -eu

dir=${1:?usage: $0 EMPTY-DIR}
COSIGN=${COSIGN:-cosign}
PYSPDXTOOLS=${PYSPDXTOOLS:-pyspdxtools}
command -v "$COSIGN" > /dev/null || { echo "no cosign (set COSIGN)" >&2; exit 2; }
command -v "$PYSPDXTOOLS" > /dev/null || { echo "no pyspdxtools (set PYSPDXTOOLS)" >&2; exit 2; }
mkdir -p "$dir"
[ -z "$(ls -A "$dir")" ] || { echo "$dir is not empty" >&2; exit 2; }
dir=$(cd "$dir" && pwd)
root=$(pwd)

cargo build -q --bin legion
legion=$root/target/debug/legion
# The handoff test's binary, which serves the registry as its helper.
handoff=$(cargo test -q -p legion --test handoff --no-run --message-format=json |
    python3 -c 'import json,sys
for l in sys.stdin:
    m = json.loads(l)
    if m.get("reason") == "compiler-artifact" and m["target"]["name"] == "handoff" and m.get("executable"):
        print(m["executable"])')
[ -x "$handoff" ] || { echo "no handoff test binary" >&2; exit 2; }

LEGION_HANDOFF_REGISTRY=$dir/registry LEGION_HANDOFF_REGISTRY_HOST=127.0.0.1 \
    "$handoff" registry --ignored --exact > "$dir/registry.log" 2>&1 &
registry=$!
trap 'kill $registry 2> /dev/null' EXIT
i=0
until [ -s "$dir/registry" ]; do
    i=$((i + 1))
    [ $i -lt 50 ] || { echo "the registry did not start" >&2; exit 1; }
    sleep 0.2
done
R=$(cat "$dir/registry")

failed=0
check() { # what, then the command, which must succeed
    what=$1; shift
    if "$@" > "$dir/out" 2>&1; then echo "ok    $what"; else
        echo "FAIL  $what"; sed 's/^/      /' "$dir/out" | tail -5; failed=1
    fi
}
refused() { # what, then the command, which must fail
    what=$1; shift
    if "$@" > "$dir/out" 2>&1; then echo "FAIL  $what (accepted)"; failed=1; else echo "ok    $what"; fi
}
l() { "$legion" local --no-zfs --plain-http "$R" --state-dir "$dir/state" "$@"; }

mkdir "$dir/ctx"
printf 'hello\n' > "$dir/ctx/f"
printf 'FROM scratch\nCOPY f /f\n' > "$dir/ctx/Containerfile"
l key generate "$dir/k" > /dev/null
l key generate "$dir/other" > /dev/null
l build -t app:1 --record "$dir/r.json" "$dir/ctx" > /dev/null
l push --record "$dir/r.json" --sign-key "$dir/k.key" app:1 "$R/app:1" > /dev/null
pinned=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["published"][0]["pinned"])' "$dir/r.json")
l attest --key "$dir/k.key" --type provenance --record "$dir/r.json" "$pinned" > /dev/null
l attest --key "$dir/k.key" --type sbom --image app:1 --record "$dir/r.json" "$pinned" > /dev/null
l sbom -o "$dir/sbom.json" app:1 > /dev/null 2>&1

c() { COSIGN_PASSWORD= "$COSIGN" "$@" --insecure-ignore-tlog --allow-http-registry; }
check "cosign verify: the signature" c verify --key "$dir/k.pub" "$pinned"
check "cosign verify-attestation: SLSA provenance" \
    c verify-attestation --key "$dir/k.pub" --type slsaprovenance1 "$pinned"
check "cosign verify-attestation: SPDX SBOM" \
    c verify-attestation --key "$dir/k.pub" --type spdxjson "$pinned"
refused "cosign refuses the signature by another key" c verify --key "$dir/other.pub" "$pinned"
refused "cosign refuses the provenance by another key" \
    c verify-attestation --key "$dir/other.pub" --type slsaprovenance1 "$pinned"
check "the SBOM is valid SPDX" "$PYSPDXTOOLS" -i "$dir/sbom.json"
exit $failed
