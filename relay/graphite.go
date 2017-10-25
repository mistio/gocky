package relay

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/telegraf/plugins/outputs/graphite"
)

// GraphiteRelay is a relay for graphite backends
type GraphiteRelay struct {
	addr string
	name string

	schema string
	cert   string

	closing int64
	l       net.Listener

	backends []*graphiteBackend
}

func (g *GraphiteRelay) Name() string {
	if g.name == "" {
		return fmt.Sprintf("%s://%s", g.schema, g.addr)
	}

	return g.name
}

func (g *GraphiteRelay) Run() error {
	log.Println("Graphi")
	l, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if g.cert != "" {
		cert, err := tls.LoadX509KeyPair(g.cert, g.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}
	g.l = l

	log.Printf("Starting Graphite relay %q on %v", g.Name(), g.addr)
	err = http.Serve(l, g)
	if atomic.LoadInt64(&g.closing) != 0 {
		return nil
	}
	return err

}

func (g *GraphiteRelay) Stop() error {
	atomic.StoreInt64(&g.closing, 1)
	return g.l.Close()

}

func (g *GraphiteRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// start := time.Now()

	graphiteServers := make([]string, len(g.backends))
	for i := range g.backends {
		graphiteServers = append(graphiteServers, g.backends[i].name)
	}
	graphiteClient := &graphite.Graphite{
		Servers: graphiteServers,
		Prefix:  "bucky",
	}

	log.Println(graphiteClient.Template)

}

func NewGraphiteRelay(cfg GraphiteConfig) (Relay, error) {
	g := new(GraphiteRelay)

	g.addr = cfg.Addr
	g.name = cfg.Name

	g.cert = cfg.SSLCombinedPem

	g.schema = "http"
	if g.cert != "" {
		g.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := NewGraphiteBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}

		g.backends = append(g.backends, backend)
	}

	return g, nil
}

// NewGraphiteBackend Initializes a new Graphite Backend
func NewGraphiteBackend(cfg *GraphiteOutputConfig) (*graphiteBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	return &graphiteBackend{
		name: cfg.Name,
	}, nil
}

type graphiteBackend struct {
	// poster
	name string
}
