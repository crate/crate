CREATE SUBSCRIPTION sub
CONNECTION 'crate://localhost:5432?mode=pg_tunnel&user=repl&password=repl&sslmode=disable'
PUBLICATION pub;
