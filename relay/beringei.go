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

	"github.com/influxdata/influxdb/models"
)

// Beringei is a relay for Beringei writes
type Beringei struct {
	addr   string
	name   string
	schema string

	cert string

	closing int64
	l       net.Listener

	backends []*beringeiBackend
}

func NewBeringei(cfg BeringeiConfig) (Relay, error) {
	b := new(Beringei)

	b.addr = cfg.Addr
	b.name = cfg.Name

	b.cert = cfg.SSLCombinedPem

	b.schema = "http"
	if b.cert != "" {
		b.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := NewBeringeiBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}

		b.backends = append(b.backends, backend)
	}

	return b, nil

}

func (b *Beringei) Name() string {
	if b.name == "" {
		return fmt.Sprintf("%s://%s", b.schema, b.addr)
	}
	return b.name
}

func (b *Beringei) Run() error {
	l, err := net.Listen("tcp", b.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if b.cert != "" {
		cert, err := tls.LoadX509KeyPair(b.cert, b.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}
	b.l = l

	log.Printf("Starting Beringei relay %q on %v", b.Name(), b.addr)
	err = http.Serve(l, b)
	if atomic.LoadInt64(&b.closing) != 0 {
		return nil
	}
	return err
}

func (b *Beringei) Stop() error {
	atomic.StoreInt64(&b.closing, 1)
	return b.l.Close()
}

func (b *Beringei) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	start := time.Now()

	queryParams := r.URL.Query()
	log.Println(queryParams)

	// fail early if we're missing the database
	if queryParams.Get("db") == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: db")
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

	log.Println(body)

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

	for _, p := range points {
		log.Println("-----------------START-----------------")
		a := InfluxToBeringeiPoint{point: p}
		a.Transform()
		log.Println("----------------------------------")
	}

}

// type poster interface {
// 	post([]byte, string, string) (*responseData, error)
// }

type beringeiBackend struct {
	// poster
	name string
}

func NewBeringeiBackend(cfg *BeringeiOutputConfig) (*beringeiBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	return &beringeiBackend{
		name: cfg.Name,
	}, nil

}
