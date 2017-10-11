package relay

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs/graphite"
	cache "github.com/patrickmn/go-cache"
	"github.com/streadway/amqp"
)

// Beringei is a relay for Beringei writes
type Beringei struct {
	addr   string
	name   string
	schema string

	cert    string
	ampqURL string

	beringeiUpdateURL string

	closing int64
	l       net.Listener

	backends        []*beringeiBackend
	graphiteBackend string
}

var pointsCh chan *BeringeiPoint

func NewBeringei(cfg BeringeiConfig) (Relay, error) {
	b := new(Beringei)

	b.addr = cfg.Addr
	b.name = cfg.Name

	b.cert = cfg.SSLCombinedPem

	b.schema = "http"
	if b.cert != "" {
		b.schema = "https"
	}

	b.ampqURL = cfg.AMQPUrl
	b.beringeiUpdateURL = cfg.BeringeiUpdateURL
	b.graphiteBackend = cfg.GraphiteOutput

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

// Stop stops the Beringei Relay
func (b *Beringei) Stop() error {
	atomic.StoreInt64(&b.closing, 1)
	return b.l.Close()
}

func (b *Beringei) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	start := time.Now()

	g := &graphite.Graphite{
		Servers: []string{b.graphiteBackend},
		Prefix:  "bucky",
	}

	err := g.Connect()
	if err != nil {
		log.Fatalf("Could not connect to graphite: %s", err)
	}

	queryParams := r.URL.Query()

	// fail early if we cannot connect to Rabbitmq
	conn, err := amqp.Dial(b.ampqURL)
	if err != nil {
		log.Fatalf("%s: %s", "Could not connect to Rabbitmq", err)
	}
	// defer conn.Close()

	rabbitmqCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Could not open a channel", err)
	}

	_, err = rabbitmqCh.QueueDeclare(
		"beringei", // name
		false,      // durable
		false,      // delete when usused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)

	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare Queue", err)
	}

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

	// log.Println(body)

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
	// for _, p := range points {
	// 	log.Print(p)
	// }
	go pushPoints(points, b.ampqURL, g, b.beringeiUpdateURL)

}

// type poster interface {
// 	post([]byte, string, string) (*responseData, error)
// }

type beringeiBackend struct {
	// poster
	name string
}

// NewBeringeiBackend Initializes a new Beringei Backend
func NewBeringeiBackend(cfg *BeringeiOutputConfig) (*beringeiBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	return &beringeiBackend{
		name: cfg.Name,
	}, nil

}

func pushPoints(points []models.Point, amqpURL string, g *graphite.Graphite, beringeiUpdateURL string) {
	for _, p := range points {
		tags := make(map[string]string)
		for _, v := range p.Tags() {
			tags[string(v.Key)] = string(v.Value)
		}
		parsedPoints := make([]string, 0, len(points))
		graphiteMetrics := make([]telegraf.Metric, 0, len(points))
		fi := p.FieldIterator()
		for fi.Next() {
			switch fi.Type() {
			case models.Float:
				v, _ := fi.FloatValue()
				tmpPoint := NewBeringeiPoint(string(p.Name()), string(fi.FieldKey()), p.UnixNano(), tags, v)
				if utf8.ValidString(string(fi.FieldKey())) {
					grphPoint := GraphiteMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
					graphiteMetrics = append(graphiteMetrics, grphPoint)
				}
				tmpPoint.generateID(tmpPoint, p.Key())
				pushToCache(tmpPoint, amqpURL)
				if tmpPoint.ID != "" {
					s := tmpPoint.ID + " " + strconv.FormatFloat(v, 'E', -1, 64) + " " + strconv.FormatInt(tmpPoint.Timestamp, 10)
					parsedPoints = append(parsedPoints, s)
				}
			case models.Integer:
				v, _ := fi.IntegerValue()
				tmpPoint := NewBeringeiPoint(string(p.Name()), string(fi.FieldKey()), p.UnixNano(), tags, v)
				if utf8.ValidString(string(fi.FieldKey())) {
					grphPoint := GraphiteMetric(string(p.Name()), tags, p.UnixNano(), v, string(fi.FieldKey()))
					graphiteMetrics = append(graphiteMetrics, grphPoint)
				}
				tmpPoint.generateID(tmpPoint, p.Key())
				pushToCache(tmpPoint, amqpURL)
				if tmpPoint.ID != "" {
					s := tmpPoint.ID + " " + strconv.FormatInt(v, 10) + " " + strconv.FormatInt(tmpPoint.Timestamp, 10)
					parsedPoints = append(parsedPoints, s)
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
		pushToBeringei(parsedPoints, beringeiUpdateURL)
		err := g.Write(graphiteMetrics)
		if err != nil {
			log.Println(err)
		}
	}

}

func pushToBeringei(points []string, targetURL string) {
	var bodyString string
	for _, p := range points {
		bodyString += p + "\n"
	}
	// This needs to be done, as beringei plain server does not accept empty line
	bodyString = strings.Trim(bodyString, "\n")

	bodyBuf := bytes.NewBufferString(bodyString)

	http.Post(targetURL, "", bodyBuf)
	return
}

func pushToCache(p *BeringeiPoint, amqpURL string) {
	if _, found := RelayCache.Get(p.ID); found {
		return
	}

	RelayCache.Set(p.ID, p.Value, cache.DefaultExpiration)
	pushToRabbitmq(p, amqpURL)
	return
}

func pushToRabbitmq(p *BeringeiPoint, amqpURL string) {
	// fail early if we cannot connect to Rabbitmq
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("%s: %s", "Could not connect to Rabbitmq", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Could not open a channel", err)
	}
	defer ch.Close()

	b, _ := json.Marshal(p)
	err = ch.Publish(
		"",         // exchange
		"beringei", // routing Key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        b,
		})

	if err != nil {
		log.Println(err)
	} else {
		// log.Println("Success pushing to Rabbitmq")
	}

}
