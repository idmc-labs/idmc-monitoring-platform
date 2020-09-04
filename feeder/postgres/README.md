Place `idmc.sql` into `postgres/docker-entrypoint-initdb.d` directory, to initiailize the `monitoring_platform` schema.

In production, postgres service will be deemed not required, as feed service will directly connect the real database.
