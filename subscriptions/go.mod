module github.com/booleanism/surr/subscriptions

go 1.25.12

require (
	github.com/booleanism/surr v0.0.0
	github.com/rabbitmq/amqp091-go v1.14.0
)

require (
	github.com/google/uuid v1.6.0
	github.com/mattn/go-sqlite3 v1.14.52
	github.com/rs/zerolog v1.35.1
	github.com/urfave/cli/v3 v3.11.0
)

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.29.0 // indirect
)

replace github.com/booleanism/surr => ../
