#!/bin/sh
#
# Builds a release tarball of Legion on a FreeBSD host, for its release and
# architecture: legion-<version>-freebsd<major>-<arch>.tar.xz and its
# SHA-256, laid out as /usr/local under one top directory:
#
#   bin/legion
#   etc/rc.d/legion
#   etc/newsyslog.conf.d/legion.conf
#   libexec/legion/volume/          (volume plugins go here)
#   share/doc/legion/
#
# Install it with
#
#   tar -xf legion-<...>.tar.xz --strip-components 1 -C /usr/local
#
# Run from the repository root: packaging/freebsd/release.sh [out dir]
# (default target/dist). CARGO_BUILD_JOBS limits the build's jobs.

set -eu

out=${1:-target/dist}
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)
major=$(uname -r | cut -d . -f 1)
arch=$(uname -p)
name=legion-${version}-freebsd${major}-${arch}

cargo build --release --locked -p legion

stage=$(mktemp -d -t legion-release)
trap 'rm -rf "$stage"' EXIT
root=$stage/$name
install -d "$root/bin" "$root/etc/rc.d" "$root/etc/newsyslog.conf.d" \
    "$root/libexec/legion/volume" "$root/share/doc/legion"
install -s -m 755 target/release/legion "$root/bin/legion"
install -m 555 packaging/freebsd/rc.d/legion "$root/etc/rc.d/legion"
install -m 644 packaging/freebsd/newsyslog.conf.d/legion.conf \
    "$root/etc/newsyslog.conf.d/legion.conf"
install -m 644 README.md LICENSE docs/DESIGN.md "$root/share/doc/legion/"

mkdir -p "$out"
# Owned by root, whoever builds it.
tar -C "$stage" --uid 0 --gid 0 --uname root --gname wheel \
    -cJf "$out/$name.tar.xz" "$name"
(cd "$out" && sha256 -r "$name.tar.xz" > "$name.tar.xz.sha256")
echo "$out/$name.tar.xz"
