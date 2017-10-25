package relay

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/telegraf"
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
	start := time.Now()

	graphiteServers := make([]string, len(g.backends))
	for i := range g.backends {
		graphiteServers = append(graphiteServers, g.backends[i].location)
	}
	graphiteClient := &graphite.Graphite{
		Servers: graphiteServers,
		Prefix:  "bucky",
	}

	err := graphiteClient.Connect()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "unable to connect to graphite")
		log.Fatalf("Could not connect to graphite: %s", err)
	}

	queryParams := r.URL.Query()

	if r.URL.Path != "/write" {
		jsonError(w, 204, "Dummy response for db creation")
		return
	}

	var body = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
		}
		defer b.Close()
		body = b
	}

	bodyBuf := getBuf()
	_, err = bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusInternalServerError, "problem reading request body")
		return
	}

	precision := queryParams.Get("precision")
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusBadRequest, "unable to parse points")
		return
	}

	go pushToGraphite(points, graphiteClient)
	// telegraf expects a 204 response on write
	w.WriteHeader(204)
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
		name:     cfg.Name,
		location: cfg.Location,
	}, nil
}

type graphiteBackend struct {
	// poster
	name string

	location string
}

func pushToGraphite(points []models.Point, g *graphite.Graphite) {
	for _, p := range points {
		tags := make(map[string]string)
		for _, v := range p.Tags() {
			tags[string(v.Key)] = string(v.Value)
		}
		graphiteMetrics := make([]telegraf.Metric, 0, len(points))
		fi := p.FieldIterator()
		for fi.Next() {
			switch fi.Type() {
			case models.Float:
				v, _ := fi.FloatValue()
				if utf8.ValidString(string(fi.FieldKey())) {
					grphPoint := GraphiteMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
					if grphPoint != nil {
						graphiteMetrics = append(graphiteMetrics, grphPoint)
					}
				}
			case models.Integer:
				v, _ := fi.IntegerValue()
				if utf8.ValidString(string(fi.FieldKey())) {
					grphPoint := GraphiteMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
					if grphPoint != nil {
						graphiteMetrics = append(graphiteMetrics, grphPoint)
					}
				}
				// case models.String:
				// 	log.Println("String values not supported")
				// case models.Boolean:
				// 	log.Println("Boolean values not supported")
				// case models.Empty:
				// 	log.Println("Empry values not supported")
				// default:
				// 	log.Println("Unknown value type")
			}
		}

		err := g.Write(graphiteMetrics)
		if err != nil {
			log.Println(err)
		}
	}

}
