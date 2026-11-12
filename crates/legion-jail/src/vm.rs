//! Containers that are bhyve virtual machines (DESIGN.md §6.8).
//!
//! A VM container has a jail like any other, its root a clone of its
//! image, which holds the guest's disk, `/disk.img`: a task writes its own
//! copy. Its command is no program in that jail but [`SCRIPT`], run by its
//! supervisor on the host: it boots the guest with `bhyveload` (FreeBSD's
//! loader, the guest's address in `legion.*` loader variables) or UEFI
//! firmware (a cloud-init NoCloud seed for the address), and runs `bhyve`
//! with the console as the container's output and the pod's tap as its
//! network. A reboot boots it again; a power-off ends the container, 0;
//! the stop signal is the ACPI power button.

use legion_runtime::VmConfig;

/// Where the UEFI firmware is (the `edk2-bhyve` package).
pub const UEFI_FIRMWARE: &str = "/usr/local/share/uefi-firmware/BHYVE_UEFI.fd";

/// The VM's name for bhyve: one per container.
pub fn name(id: &str) -> String {
    format!("legion-{}", &id[..id.len().min(12)])
}

/// Runs the guest until it powers off or is stopped. Its settings are in
/// the environment (`VM_*`, see [`env`]).
pub const SCRIPT: &str = r#"
set -u
stop=0 pid=
trap 'stop=1; [ -n "$pid" ] && kill -TERM $pid' TERM
mem=$((VM_MEMORY / 1048576))M
seed=
if [ "$VM_BOOT" = uefi ]; then
	[ -r "$VM_FIRMWARE" ] || { echo "no UEFI firmware at $VM_FIRMWARE: install edk2-bhyve" >&2; exit 127; }
	seed=$VM_DIR/seed.iso
	mkdir -p "$VM_DIR/seed"
	printf 'instance-id: %s\nlocal-hostname: %s\n' "$VM_NAME" "$VM_HOSTNAME" > "$VM_DIR/seed/meta-data"
	printf '#cloud-config\n' > "$VM_DIR/seed/user-data"
	{
		printf 'version: 2\nethernets:\n  pod:\n    match: { name: "*" }\n'
		printf '    addresses: [%s%s]\n' "$VM_IP" "${VM_IP6:+, $VM_IP6/64}"
		printf '    gateway4: %s\n' "$VM_GATEWAY"
		[ -n "${VM_GATEWAY6:-}" ] && printf '    gateway6: %s\n' "$VM_GATEWAY6"
		printf '    mtu: %s\n    nameservers: { addresses: [%s] }\n' "$VM_MTU" "$VM_DNS"
	} > "$VM_DIR/seed/network-config"
	makefs -t cd9660 -o rockridge -o label=cidata "$seed" "$VM_DIR/seed" || exit 127
fi
rc=0
while :; do
	bhyvectl --vm="$VM_NAME" --destroy >/dev/null 2>&1
	if [ "$VM_BOOT" = freebsd ]; then
		bhyveload -m "$mem" -d "$VM_DISK" \
			-e "legion.ip=${VM_IP%/*}" -e "legion.prefix=${VM_IP#*/}" \
			-e "legion.gateway=$VM_GATEWAY" -e "legion.ip6=${VM_IP6:-}" \
			-e "legion.gateway6=${VM_GATEWAY6:-}" -e "legion.mtu=$VM_MTU" \
			-e "legion.dns=$VM_DNS" -e "legion.hostname=$VM_HOSTNAME" \
			"$VM_NAME" </dev/null || { rc=$?; break; }
		boot=
	else
		boot="-l bootrom,$VM_FIRMWARE"
	fi
	bhyve -c "$VM_CPUS" -m "$mem" -A -H -P -s 0,hostbridge -s 31,lpc \
		-l com1,stdio $boot -s "2,virtio-net,$VM_TAP" -s "3,virtio-blk,$VM_DISK" \
		${seed:+-s 4,ahci-cd,$seed} "$VM_NAME" </dev/null &
	pid=$!
	while :; do
		wait $pid
		rc=$?
		kill -0 $pid 2>/dev/null || break
	done
	pid=
	# 0: rebooted, booted again unless stopping.
	[ $rc -eq 0 ] && [ $stop -eq 0 ] && continue
	break
done
bhyvectl --vm="$VM_NAME" --destroy >/dev/null 2>&1
# 1: powered off, 2: halted; anything else is how it failed.
case $rc in 1|2) exit 0 ;; *) exit $rc ;; esac
"#;

/// The command a VM container's supervisor runs on the host.
pub fn argv() -> Vec<String> {
    vec![
        "/bin/sh".into(),
        "-c".into(),
        SCRIPT.into(),
        "legion-vm".into(),
    ]
}

/// [`SCRIPT`]'s settings: the VM's, its disk in `root`, its seed in `dir`.
pub fn env(id: &str, vm: &VmConfig, root: &std::path::Path, dir: &std::path::Path) -> Vec<String> {
    let mut env = vec![
        "PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin".to_owned(),
        format!("VM_NAME={}", name(id)),
        format!("VM_DISK={}", root.join("disk.img").display()),
        format!("VM_DIR={}", dir.display()),
        format!("VM_CPUS={}", vm.cpus),
        format!("VM_MEMORY={}", vm.memory),
        format!("VM_BOOT={}", if vm.uefi { "uefi" } else { "freebsd" }),
        format!("VM_FIRMWARE={UEFI_FIRMWARE}"),
        format!("VM_TAP={}", vm.tap),
        format!("VM_IP={}", vm.ip),
        format!("VM_GATEWAY={}", vm.gateway),
        format!("VM_MTU={}", vm.mtu),
        format!("VM_DNS={}", vm.dns),
        format!("VM_HOSTNAME={}", vm.hostname),
    ];
    if let Some(ip6) = &vm.ip6 {
        env.push(format!("VM_IP6={ip6}"));
    }
    if let Some(g) = &vm.gateway6 {
        env.push(format!("VM_GATEWAY6={g}"));
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vm_runs_by_its_settings() {
        let vm = VmConfig {
            cpus: 2,
            memory: 512 << 20,
            uefi: false,
            tap: "tap3".into(),
            ip: "10.200.1.5/24".into(),
            gateway: "10.200.1.1".into(),
            ip6: None,
            gateway6: None,
            mtu: 1420,
            dns: "10.200.1.1".into(),
            hostname: "web-1".into(),
        };
        let env = env(
            "0123456789abcdef",
            &vm,
            std::path::Path::new("/var/db/legion/c/root"),
            std::path::Path::new("/var/db/legion/c"),
        );
        for want in [
            "VM_NAME=legion-0123456789ab",
            "VM_DISK=/var/db/legion/c/root/disk.img",
            "VM_TAP=tap3",
            "VM_BOOT=freebsd",
            "VM_MEMORY=536870912",
        ] {
            assert!(env.iter().any(|e| e == want), "{want} in {env:?}");
        }
        assert!(!env.iter().any(|e| e.starts_with("VM_IP6=")));
        assert_eq!(argv()[..2], ["/bin/sh", "-c"]);
    }
}
