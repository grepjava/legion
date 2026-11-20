# Starts PostgreSQL on $PGDATA, making the cluster first if it is empty:
# the superuser postgres, its password the file /run/secrets/pg-password,
# asked of every connection from the network.
set -e
: "${PGDATA:=/var/db/postgres/data}"
if [ ! -s "$PGDATA/PG_VERSION" ]; then
	password=/run/secrets/pg-password
	if [ ! -s "$password" ]; then
		echo "no password in $password: give the Service the secret pg-password" >&2
		exit 1
	fi
	initdb -D "$PGDATA" -U postgres -E UTF8 --no-locale \
	    --auth-local=trust --auth-host=scram-sha-256 --pwfile="$password" >/dev/null
	printf 'host all all 0.0.0.0/0 scram-sha-256\nhost all all ::/0 scram-sha-256\n' \
	    >>"$PGDATA/pg_hba.conf"
fi
exec postgres -D "$PGDATA" -c listen_addresses='*' -c log_destination=stderr
