module github.com/booleanism/surr/rx

go 1.27.1

replace github.com/booleanism/surr => ../

require (
	fortio.org/safecast v1.2.0
	github.com/booleanism/surr v0.0.0
	github.com/google/uuid v1.6.0
	github.com/haivision/srtgo v0.0.0-20260907105832-ab4639b789b4
	github.com/rabbitmq/amqp091-go v1.14.0
	github.com/rs/zerolog v1.35.1
	github.com/urfave/cli/v3 v3.11.0
)

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-pointer v0.0.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
