package relay

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs/graphite"
	"github.com/robfig/cron"
)

// Metering Read-Write mutex
var mu = &sync.RWMutex{}
var metering = make(map[string]map[string]int)
var amqpURL string

// GraphiteRelay is a relay for graphite backends
type GraphiteRelay struct {
	addr string
	name string

	schema string
	cert   string

	closing int64
	l       net.Listener

	enableMetering bool
	ampqURL        string

	dropUnauthorized bool

	cronJob      *cron.Cron
	cronSchedule string

	backends []*graphiteBackend
}

func (g *GraphiteRelay) Name() string {
	if g.name == "" {
		return fmt.Sprintf("%s://%s", g.schema, g.addr)
	}

	return g.name
}

func (g *GraphiteRelay) Run() error {
	l, err := net.Listen("tcp", g.addr)

	if g.cronSchedule != "" {
		g.cronJob.AddFunc(g.cronSchedule, pushToAmqp)
		g.cronJob.Start()
	}

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
	if g.cronSchedule != "" {
		g.cronJob.Stop()
	}
	return g.l.Close()

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

	g.enableMetering = cfg.EnableMetering
	g.ampqURL = cfg.AMQPUrl
	amqpURL = cfg.AMQPUrl

	if g.enableMetering && g.ampqURL == "" {
		g.enableMetering = false
		log.Println("You have to set AMQPUrl in config for metering to work")
		log.Println("Disabling metering for now")
	}

	g.dropUnauthorized = cfg.DropUnauthorized

	g.cronSchedule = cfg.CronSchedule

	if g.cronSchedule != "" {
		g.cronJob = cron.New()
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

	conErr := graphiteClient.Connect()
	if conErr != nil {
		jsonError(w, http.StatusInternalServerError, "unable to connect to graphite")
		log.Fatalf("Could not connect to graphite: %s", conErr)
	}

	queryParams := r.URL.Query()

	// if r.URL.Path == "/metrics" {
	// 	resp := ""
	// 	t := time.Now().UnixNano()
	// 	mu.RLock()
	// 	for orgID, machineMap := range metering {
	// 		for machineID, samples := range machineMap {
	// 			resp += fmt.Sprintf("gocky_samples_total{relay=graphite,orgID=%s,machineID=%s} %d %d\n", orgID, machineID, samples, t)
	// 		}
	// 	}
	// 	mu.RUnlock()
	// 	io.WriteString(w, resp)
	// 	return
	// }

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
	_, err := bodyBuf.ReadFrom(body)
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

	machineID := ""
	if r.Header["X-Gocky-Tag-Machine-Id"] != nil {
		machineID = r.Header["X-Gocky-Tag-Machine-Id"][0]
	} else {
		if g.dropUnauthorized {
			log.Println("Gocky Headers are missing. Dropping packages...")
			jsonError(w, http.StatusForbidden, "cannot find Gocky headers")
			return
		}
	}

	sourceType := "unix"

	if r.Header["X-Gocky-Tag-Source-Type"][0] == "windows" {
		sourceType = "windows"
	}

	go pushToGraphite(points, graphiteClient, machineID, sourceType)

	if g.enableMetering {
		orgID := "Unauthorized"
		if r.Header["X-Gocky-Tag-Org-Id"] != nil {
			orgID = r.Header["X-Gocky-Tag-Org-Id"][0]
		}
		machineID := ""
		if r.Header["X-Gocky-Tag-Machine-Id"] != nil {
			machineID = r.Header["X-Gocky-Tag-Machine-Id"][0]
		}

		mu.Lock()

		_, orgExists := metering[orgID]
		if !orgExists {
			metering[orgID] = make(map[string]int)
		}

		_, machExists := metering[orgID][machineID]
		if !machExists {
			metering[orgID][machineID] = len(points)
		} else {
			metering[orgID][machineID] += len(points)
		}

		mu.Unlock()
	}

	// telegraf expects a 204 response on write
	w.WriteHeader(204)
}

func pushToGraphite(points []models.Point, g *graphite.Graphite, machineID, sourceType string) {
	for _, p := range points {
		tags := make(map[string]string)
		for _, v := range p.Tags() {
			tags[string(v.Key)] = string(v.Value)
		}
		if machineID == "" {
			machineID = "Unknown." + tags["machine_id"]
		}
		tags["machine_id"] = machineID
		graphiteMetrics := make([]telegraf.Metric, 0, len(points))
		fi := p.FieldIterator()
		for fi.Next() {
			switch fi.Type() {
			case models.Float:
				v, _ := fi.FloatValue()
				if utf8.ValidString(string(fi.FieldKey())) {
					switch sourceType {
					case "windows":
						grphPoint := GraphiteWindowsMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
						if grphPoint != nil {
							graphiteMetrics = append(graphiteMetrics, grphPoint)
						}
					default:
						grphPoint := GraphiteMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
						if grphPoint != nil {
							graphiteMetrics = append(graphiteMetrics, grphPoint)
						}
					}
				}
			case models.Integer:
				v, _ := fi.IntegerValue()
				if utf8.ValidString(string(fi.FieldKey())) {
					switch sourceType {
					case "windows":
						grphPoint := GraphiteWindowsMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
						if grphPoint != nil {
							graphiteMetrics = append(graphiteMetrics, grphPoint)
						}
					default:
						grphPoint := GraphiteMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
						if grphPoint != nil {
							graphiteMetrics = append(graphiteMetrics, grphPoint)
						}
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
