package relay

import (
	"os"

	"github.com/naoina/toml"
)

type Config struct {
	HTTPRelays     []HTTPConfig     `toml:"http"`
	UDPRelays      []UDPConfig      `toml:"udp"`
	BeringeiRelays []BeringeiConfig `toml:"beringei"`
	GraphiteRelays []GraphiteConfig `toml:"graphite"`
	FdbRelays      []FdbConfig      `toml:"fdb"`
}

type HTTPConfig struct {
	// Name identifies the HTTP relay
	Name string `toml:"name"`

	// Addr should be set to the desired listening host:port
	Addr string `toml:"bind-addr"`

	// Set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`

	// Default retention policy to set for forwarded requests
	DefaultRetentionPolicy string `toml:"default-retention-policy"`

	// EnableMetering toggles metering stats to Rabbitmq. You have to setup AMQPUrl for this
	EnableMetering bool `toml:"enable-metering"`

	// AMQPUrl will be used to connect to Rabbitmq (amqp://guest:guest@127.0.0.1:5672/)
	AMQPUrl string `toml:"amqp-url"`

	// DropUnauthorized will drop samples that do not come from traefik
	// If set to false, it will create an "Unknown" directory in graphite
	DropUnauthorized bool `toml:"drop-unauthorized"`

	CronSchedule string `toml:"cron-schedule"`

	// Outputs is a list of backed servers where writes will be forwarded
	Outputs []HTTPOutputConfig `toml:"output"`
}

type HTTPOutputConfig struct {
	// Name of the backend server
	Name string `toml:"name"`

	// Location should be set to the URL of the backend server's write endpoint
	Location string `toml:"location"`

	// Type of the backend server e.g. influxdb, graphite, etc.
	BackendType string `toml:"type"`

	// Type of the backend server e.g. influxdb, graphite, etc.
	FailOnError bool `toml:"fail-on-error"`

	// Timeout sets a per-backend timeout for write requests. (Default 1s)
	// The format used is the same seen in time.ParseDuration
	Timeout string `toml:"timeout"`

	// Buffer failed writes up to maximum count. (Default 0, retry/buffering disabled)
	BufferSizeMB int `toml:"buffer-size-mb"`

	// Maximum batch size in KB (Default 512)
	MaxBatchKB int `toml:"max-batch-kb"`

	// Maximum delay between retry attempts.
	// The format used is the same seen in time.ParseDuration (Default 1s)
	MaxDelayInterval string `toml:"max-delay-interval"`

	// Maximum retry attempts.
	MaxRetries int `toml:"max-retries"`

	// Skip TLS verification in order to use self signed certificate.
	// WARNING: It's insecure. Use it only for developing and don't use in production.
	SkipTLSVerification bool `toml:"skip-tls-verification"`
}

type UDPConfig struct {
	// Name identifies the UDP relay
	Name string `toml:"name"`

	// Addr is where the UDP relay will listen for packets
	Addr string `toml:"bind-addr"`

	// Precision sets the precision of the timestamps (input and output)
	Precision string `toml:"precision"`

	// ReadBuffer sets the socket buffer for incoming connections
	ReadBuffer int `toml:"read-buffer"`

	// Outputs is a list of backend servers where writes will be forwarded
	Outputs []UDPOutputConfig `toml:"output"`
}

type UDPOutputConfig struct {
	// Name identifies the UDP backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`

	// MTU sets the maximum output payload size, default is 1024
	MTU int `toml:"mtu"`
}

type BeringeiConfig struct {
	//Name identifies the beringei relay
	Name string `toml:"name"`

	// Addr is where the UDP relay will listen for packets
	Addr string `toml:"bind-addr"`

	// SSLCombinedPem set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`

	// AMQPUrl will be used to connect to Rabbitmq (amqp://guest:guest@127.0.0.1:5672/)
	AMQPUrl string `toml:"amqp-url"`

	BeringeiUpdateURL string `toml:"beringei-update-url"`

	// Outputs is a list of backend servers where writes will be forwarded
	Outputs []BeringeiOutputConfig `toml:"output"`

	// GraphiteOutput is a list of graphite backends
	GraphiteOutput string `toml:"graphite-output"`
}

type BeringeiOutputConfig struct {
	// Name identifies the Beringei backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`
}

type GraphiteConfig struct {
	// Name identifies the graphite relay
	Name string `toml:"name"`

	// Addr is where the Graphite Relay will listen for packets
	Addr string `toml:"bind-addr"`

	// SSLCombinedPem set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`

	// EnableMetering toggles metering stats to Rabbitmq. You have to setup AMQPUrl for this
	EnableMetering bool `toml:"enable-metering"`

	// AMQPUrl will be used to connect to Rabbitmq (amqp://guest:guest@127.0.0.1:5672/)
	AMQPUrl string `toml:"amqp-url"`

	// DropUnauthorized will drop samples that do not come from traefik
	// If set to false, it will create an "Unknown" directory in graphite
	DropUnauthorized bool `toml:"drop-unauthorized"`

	CronSchedule string `toml:"cron-schedule"`
	// A list of graphite backend servers
	Outputs []GraphiteOutputConfig `toml:"output"`
}

type GraphiteOutputConfig struct {
	// Name identifies the graphite backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`
}

type FdbConfig struct {
	// Name identifies the Fdb relay
	Name string `toml:"name"`

	// Addr is where the Fdb Relay will listen for packets
	Addr string `toml:"bind-addr"`

	// SSLCombinedPem set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`

	// EnableMetering toggles metering stats to Rabbitmq. You have to setup AMQPUrl for this
	EnableMetering bool `toml:"enable-metering"`

	// AMQPUrl will be used to connect to Rabbitmq (amqp://guest:guest@127.0.0.1:5672/)
	AMQPUrl string `toml:"amqp-url"`

	// DropUnauthorized will drop samples that do not come from traefik
	// If set to false, it will create an "Unknown" directory in Fdb
	DropUnauthorized bool `toml:"drop-unauthorized"`

	CronSchedule string `toml:"cron-schedule"`
	// A list of Fdb backend servers
	Outputs []FdbOutputConfig `toml:"output"`
}

type FdbOutputConfig struct {
	// Name identifies the Fdb backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`
}

// LoadConfigFile parses the specified file into a Config object
func LoadConfigFile(filename string) (cfg Config, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	return cfg, toml.NewDecoder(f).Decode(&cfg)
}
