module github.com/booleanism/surr/hub

go 1.26.0

require (
	github.com/booleanism/surr v0.0.0
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/uuid v1.6.0
	github.com/mattn/go-sqlite3 v1.14.52
	github.com/rabbitmq/amqp091-go v1.14.0
	github.com/urfave/cli/v3 v3.11.0
	golang.org/x/crypto v0.56.0
)

require (
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/booleanism/surr => ../
