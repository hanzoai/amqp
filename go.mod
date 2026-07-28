module github.com/hanzoai/amqp

go 1.26.5

require (
	github.com/hanzoai/cloud v1.801.218
	github.com/luxfi/log v1.6.0
	github.com/nats-io/nats.go v1.50.0
	github.com/zap-proto/zip v1.17.2
)

// HIP-0106 cloud Mount() — point at sibling cloud + zip checkouts.
// CI overrides via GOPROXY once cloud is published.
replace github.com/hanzoai/cloud => ../cloud

require (
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudflare/circl v1.6.3 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dlclark/regexp2/v2 v2.2.1 // indirect
	github.com/dop251/goja v0.0.0-20260627200808-0b76000cabdb // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/evanw/esbuild v0.28.1 // indirect
	github.com/fasthttp/websocket v1.5.12 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.4+incompatible // indirect
	github.com/gofiber/schema v1.7.1 // indirect
	github.com/gofiber/utils/v2 v2.0.4 // indirect
	github.com/google/pprof v0.0.0-20260402051712-545e8a4df936 // indirect
	github.com/google/uuid v1.6.1-0.20241114170450-2d3c2a9cc518 // indirect
	github.com/gorilla/rpc v1.2.1 // indirect
	github.com/grandcat/zeroconf v1.0.0 // indirect
	github.com/hanzo-ds/go v1.0.1 // indirect
	github.com/hanzo-ds/native v0.72.0 // indirect
	github.com/hanzoai/account v0.2.0 // indirect
	github.com/hanzoai/commerce v1.49.29 // indirect
	github.com/hanzoai/csqlite v0.1.0 // indirect
	github.com/hanzoai/dbx v1.17.2 // indirect
	github.com/hanzoai/decimal v0.1.1 // indirect
	github.com/hanzoai/go-openai v1.41.0 // indirect
	github.com/hanzoai/ha v0.1.1 // indirect
	github.com/hanzoai/iam v1.33.26 // indirect
	github.com/hanzoai/money v0.2.1 // indirect
	github.com/hanzoai/orm v0.6.18 // indirect
	github.com/hanzoai/s3-go v1.0.0 // indirect
	github.com/hanzoai/sqlcipher v0.1.1 // indirect
	github.com/hanzoai/sqlite v0.4.0 // indirect
	github.com/hanzoai/tasks v1.51.4 // indirect
	github.com/hanzoai/vfs v0.6.6 // indirect
	github.com/hanzokv/go/v9 v9.22.0 // indirect
	github.com/holiman/uint256 v1.3.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/luxfi/accel v1.2.4 // indirect
	github.com/luxfi/bft v0.1.5 // indirect
	github.com/luxfi/cache v1.3.1 // indirect
	github.com/luxfi/compress v0.1.1 // indirect
	github.com/luxfi/concurrent v0.1.1 // indirect
	github.com/luxfi/consensus v1.36.11 // indirect
	github.com/luxfi/constants v1.6.2 // indirect
	github.com/luxfi/container v0.2.1 // indirect
	github.com/luxfi/crypto v1.20.2 // indirect
	github.com/luxfi/database v1.21.1 // indirect
	github.com/luxfi/geth v1.20.1 // indirect
	github.com/luxfi/ids v1.3.2 // indirect
	github.com/luxfi/math v1.5.1 // indirect
	github.com/luxfi/math/big v0.1.0 // indirect
	github.com/luxfi/mdns v0.1.1 // indirect
	github.com/luxfi/metric v1.9.0 // indirect
	github.com/luxfi/mock v0.1.1 // indirect
	github.com/luxfi/p2p v1.22.1 // indirect
	github.com/luxfi/pq v1.1.0 // indirect
	github.com/luxfi/sampler v1.1.0 // indirect
	github.com/luxfi/validators v1.3.1 // indirect
	github.com/luxfi/version v1.0.1 // indirect
	github.com/luxfi/warp v1.24.1 // indirect
	github.com/luxfi/zap v1.2.6 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/miekg/dns v1.1.72 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mr-tron/base58 v1.3.0 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/savsgio/gotils v0.0.0-20240704082632-aef3928b8a38 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/supranational/blst v0.3.16 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.72.0 // indirect
	github.com/zap-proto/fiber/v3 v3.2.1 // indirect
	github.com/zap-proto/go v1.3.0 // indirect
	github.com/zap-proto/http v0.3.1 // indirect
	github.com/zap-proto/md v0.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/log v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/mock v0.6.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/exp v0.0.0-20260529124908-c761662dc8c9 // indirect
	golang.org/x/mod v0.37.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/tools v0.47.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	modernc.org/libc v1.72.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.51.0 // indirect
)
