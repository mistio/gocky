package relay

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"strconv"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"
	"database/sql"

	"github.com/influxdata/influxdb/models"
	"github.com/robfig/cron"
	_"github.com/lib/pq"
)

// TimescaleRelay is a relay for timescaledb backends
type TimescaleRelay struct {
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

	backends []*TimescaleBackend
}

type TimescaleBackend struct {
	name string
	connect_parameters string
	db *sql.DB
}

func (tr *TimescaleRelay) Name() string {
	if tr.name == "" {
		return fmt.Sprintf("%s://%s", tr.schema, tr.addr)
	}

	return tr.name
}

func (tr *TimescaleRelay) Run() error {
	l, err := net.Listen("tcp", tr.addr)

	if tr.cronSchedule != "" {
		tr.cronJob.AddFunc(tr.cronSchedule, pushToAmqp)
		tr.cronJob.Start()
	}

	if err != nil {
		return err
	}

	// support HTTPS
	if tr.cert != "" {
		cert, err := tls.LoadX509KeyPair(tr.cert, tr.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}
	tr.l = l

	//connect to db backends
	for _, backend := range tr.backends {
		db, err := sql.Open("postgres", backend.connect_parameters)
		if err != nil {
			return err
		}
		backend.db = db
		err = backend.db.Ping()
		if err != nil {
			log.Printf("Backend %s doesn't respond", backend.name)
			return err
		}
		log.Printf("Backend %s is up", backend.name) 
	}

	log.Printf("Starting Timescale relay %q on %v", tr.Name(), tr.addr)
	err = http.Serve(l, tr)
	if atomic.LoadInt64(&tr.closing) != 0 {
		return nil
	}
	return err

}

func (tr *TimescaleRelay) Stop() error {
	atomic.StoreInt64(&tr.closing, 1)
	if tr.cronSchedule != "" {
		tr.cronJob.Stop()
	}
	return tr.l.Close()

}

func NewTimescaleRelay(cfg TimescaleConfig) (Relay, error) {
	tr := new(TimescaleRelay)

	tr.addr = cfg.Addr
	tr.name = cfg.Name

	tr.cert = cfg.SSLCombinedPem

	tr.schema = "http"
	if tr.cert != "" {
		tr.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := NewTimescaleBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}
		tr.backends = append(tr.backends, backend)
	}

	tr.enableMetering = cfg.EnableMetering
	tr.ampqURL = cfg.AMQPUrl

	if tr.enableMetering && tr.ampqURL == "" {
		tr.enableMetering = false
		log.Println("You have to set AMQPUrl in config for metering to work")
		log.Println("Disabling metering for now")
	}

	tr.dropUnauthorized = cfg.DropUnauthorized
	tr.cronSchedule = cfg.CronSchedule

	if tr.cronSchedule != "" {
		tr.cronJob = cron.New()
	}

	return tr, nil
}

// NewTimescaleBackend Initializes a new Timescale Backend
func NewTimescaleBackend(cfg *TimescaleOutputConfig) (*TimescaleBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}
	connStr := "host=timescaledb dbname=tutorial user=root password=example sslmode=disable"

	return &TimescaleBackend{
		name: cfg.Name,
		connect_parameters: connStr,
		db:	nil,
	}, nil
}

func (tr *TimescaleRelay) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	start := time.Now()
	log.Println("Request Arrived")
	queryParams := req.URL.Query()

	if req.URL.Path != "/write" {
		jsonError(resp, 204, "Dummy response for db creation")
		return
	}

	var body = req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(req.Body)
		if err != nil {
			jsonError(resp, http.StatusBadRequest, "unable to decode gzip body")
		}
		defer b.Close()
		body = b
	}

	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(resp, http.StatusInternalServerError, "problem reading request body")
		return
	}

	precision := queryParams.Get("precision")
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(resp, http.StatusBadRequest, "unable to parse points")
		return
	}

	// handle data points
	go makeQuery(points, tr.backends)

	//update metering data
	if tr.enableMetering {
		orgID := "Unauthorized"
		if req.Header["X-Gocky-Tag-Org-Id"] != nil {
			orgID = req.Header["X-Gocky-Tag-Org-Id"][0]
		}
		machineID := ""
		if req.Header["X-Gocky-Tag-Machine-Id"] != nil {
			machineID = req.Header["X-Gocky-Tag-Machine-Id"][0]
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
	resp.WriteHeader(204)
}

func jsonFromTags(point models.Point) string {
	buffer := []byte("{")

	// insert key-value pairs in buffer
	for _, v := range point.Tags() { 
		buffer = append(buffer, '"')
		buffer = append(buffer, v.Key...)
		buffer = append(buffer, "\": \""...)
		buffer = append(buffer, v.Value...)
		buffer = append(buffer, "\", "...)
	}
	buffer[len(buffer)-2] = '}'
	return string(buffer)
}

func jsonFromFields(point models.Point) string {
	buffer := []byte("{")

	fi := point.FieldIterator()
	for fi.Next() {
		buffer = append(buffer, '"')
		buffer = append(buffer, fi.FieldKey()...)
		buffer = append(buffer, "\": \""...)
		switch fi.Type() {
		case models.Float:
			val, _ := fi.FloatValue()
			buffer = append(buffer, strconv.FormatFloat(val,'f',-1,64)...)
		case models.Integer:
			val, _ := fi.IntegerValue()
			buffer = append(buffer, strconv.FormatInt(val,10)...)
		case models.Unsigned:
			val, _ := fi.UnsignedValue()
			buffer = append(buffer, strconv.FormatUint(val,10)...)
		case models.Boolean:
			val, _ := fi.BooleanValue()
			buffer = append(buffer, strconv.FormatBool(val)...)
		case models.String:
			buffer = append(buffer, string(fi.StringValue())...)
		default:
			buffer = append(buffer, "unsupported_type"...)
		}
		buffer = append(buffer, "\", "...)
	}
	buffer[len(buffer)-2] = '}'
	return string(buffer)
}

func makeQuery(points []models.Point, backends []*TimescaleBackend) {
	buffer := []byte("INSERT INTO metrics VALUES ")
	// Each point is a timescaledb record
	for _, p := range points {
		time := fmt.Sprintf("%v", p.Time())
		buffer = append(buffer, "(to_timestamp('"...)
		buffer = append(buffer, time...)
		buffer = append(buffer, "','YYYY-MM-DD HH24:MI:SS'),'"...)
		buffer = append(buffer, string(p.Name())...)
		buffer = append(buffer, "','"...)
		buffer = append(buffer, jsonFromTags(p)...)
		buffer = append(buffer, "','"...)
		buffer = append(buffer, jsonFromFields(p)...)
		buffer = append(buffer, "'),"...)
	}
	buffer[len(buffer)-1] = ';'
	// log.Printf("%v",string(buffer))
	insert_query := string(buffer)

	for _, backend := range backends {
		_, err := backend.db.Query(insert_query)
		if err != nil {
			log.Printf("Insertion failed at %v, query %v:", backend.name, insert_query)
			log.Printf("Cause of: %v", err)
		}
	}
}