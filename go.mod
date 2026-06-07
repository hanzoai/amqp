module github.com/hanzoai/amqp

go 1.26.4

require (
	github.com/hanzoai/cloud v0.0.0
	github.com/hanzoai/zip v0.0.0
	github.com/luxfi/log v1.4.3
	github.com/nats-io/nats.go v1.50.0
)

// HIP-0106 cloud Mount() — point at sibling cloud + zip checkouts.
// CI overrides via GOPROXY once cloud is published.
replace (
	github.com/hanzoai/cloud => ../cloud
	github.com/hanzoai/zip => ../zip
)

require (
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/gofiber/fiber/v3 v3.2.0 // indirect
	github.com/gofiber/schema v1.7.1 // indirect
	github.com/gofiber/utils/v2 v2.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.21 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.70.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)
