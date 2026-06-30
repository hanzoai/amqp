module github.com/hanzoai/amqp

go 1.26.4

require (
	github.com/hanzoai/cloud v0.0.0
	github.com/hanzoai/zip v0.2.0
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
	github.com/dlclark/regexp2/v2 v2.2.1 // indirect
	github.com/dop251/goja v0.0.0-20260607120635-348e6bea910d // indirect
	github.com/evanw/esbuild v0.28.1 // indirect
	github.com/fasthttp/websocket v1.5.12 // indirect
	github.com/go-sourcemap/sourcemap v2.1.4+incompatible // indirect
	github.com/gofiber/fiber/v3 v3.2.0 // indirect
	github.com/gofiber/schema v1.7.1 // indirect
	github.com/gofiber/utils/v2 v2.0.4 // indirect
	github.com/google/pprof v0.0.0-20260402051712-545e8a4df936 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/savsgio/gotils v0.0.0-20240704082632-aef3928b8a38 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.70.0 // indirect
	github.com/zap-proto/go v1.3.0 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)
