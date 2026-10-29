#!/bin/sh
# Builds the guest of the bhyve test (bhyve.rs): a UFS image of this host's
# kernel, /rescue, nc and the rc below, which is its whole userland.
#   bhyve-guest.sh <image>
set -eu
out=$1
R=$(mktemp -d /var/tmp/bhyve-guest.XXXXXX)
trap 'rm -rf "$R"' EXIT
mkdir -p "$R/boot/kernel" "$R/rescue" "$R/sbin" "$R/bin" "$R/etc" "$R/dev" \
	"$R/lib" "$R/libexec" "$R/usr/bin"
cp /boot/kernel/kernel "$R/boot/kernel/"
cp -R /boot/lua /boot/defaults /boot/device.hints "$R/boot/"
cat > "$R/boot/loader.conf" <<'EOF'
autoboot_delay="0"
beastie_disable="YES"
vfs.root.mountfrom="ufs:/dev/vtbd0"
EOF
cp /rescue/rescue "$R/rescue/"
for t in sh kenv ifconfig route sleep; do
	ln -s rescue "$R/rescue/$t"
done
ln -s /rescue/rescue "$R/sbin/init"
ln -s /rescue/rescue "$R/bin/sh"
cp /usr/bin/nc "$R/usr/bin/"
cp /libexec/ld-elf.so.1 "$R/libexec/"
for l in $(ldd -f '%p\n' /usr/bin/nc | grep '^/'); do
	cp "$l" "$R$l"
done
# Networking from the loader's legion.* variables (bhyveload -e), a server
# on 8080 and, every second, a probe of legion.probe and legion.probe6
# ("address port"), each logged to the console.
cat > "$R/etc/rc" <<'EOF'
#!/bin/sh
PATH=/rescue
export PATH
v() { kenv -q "legion.$1"; }
ifconfig lo0 inet 127.0.0.1/8 up
ifconfig vtnet0 inet "$(v ip)/$(v prefix)" mtu "$(v mtu)" up
ifconfig vtnet0 inet6 -ifdisabled no_dad
ifconfig vtnet0 inet6 "$(v ip6)/64" no_dad
route -q add default "$(v gateway)"
route -q add -inet6 default "$(v gateway6)"
echo "legion-guest: up $(v ip) $(v ip6)"
for f in -4 -6; do
	(while :; do echo from-the-guest | /usr/bin/nc $f -N -l 8080 >/dev/null 2>&1; done) &
done
n=0
while :; do
	n=$((n + 1))
	for p in probe probe6; do
		t=$(v $p)
		[ -n "$t" ] || continue
		got=$(/usr/bin/nc -w 1 -N $t </dev/null 2>/dev/null)
		echo "legion-guest: $p $n ${got:-refused}"
	done
	sleep 1
done
EOF
chmod 555 "$R/etc/rc"
makefs -t ffs -s 80m "$out" "$R" >/dev/null
