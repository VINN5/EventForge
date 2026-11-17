$env:DATABASE_URL = "postgres://eventforge:eventforge@localhost:5433/eventforge?sslmode=disable"
$env:TEST_DATABASE_URL = "postgres://eventforge:eventforge@localhost:5433/eventforge_test?sslmode=disable"
$env:GOOSE_DRIVER = "postgres"
$env:GOOSE_DBSTRING = $env:DATABASE_URL