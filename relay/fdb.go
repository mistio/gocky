package relay

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/influxdata/influxdb/models"
	"github.com/robfig/cron"
)

//FdbRelay is a relay for graphite backends
type FdbRelay struct {
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

	backends []*FdbBackend
}

type FdbBackend struct {
	name  string
	table string
	db    *fdb.Database //not sure about that
}

func (f *FdbRelay) Name() string {
	if f.name == "" {
		return "fmt.Sprintf(\"%s://%s\", g.schema, g.addr)"
	}

	return f.name
}

func (f *FdbRelay) Run() error {
	log.Printf("Fdb Run() on address %s\n", f.addr)
	l, err := net.Listen("tcp", f.addr)

	if f.cronSchedule != "" {
		f.cronJob.AddFunc(f.cronSchedule, pushToAmqp)
		f.cronJob.Start()
	}

	if err != nil {
		return err
	}

	// support HTTPS
	if f.cert != "" {
		cert, err := tls.LoadX509KeyPair(f.cert, f.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}
	f.l = l

	//check db connections
	for _, backend := range f.backends {
		//err = backend.db.Ping()
		err = nil
		// TODO find something to replace ping with
		if err != nil {
			log.Printf("Backend %s ping failed, error: %v", backend.name, err)
			// return err
		}
		log.Printf("Backend %s ping succeded", backend.name)
	}

	log.Printf("Starting Fdb relay %q on %v", f.Name(), f.addr)
	err = http.Serve(l, f)
	if atomic.LoadInt64(&f.closing) != 0 {
		return nil
	}
	return err

}

func (f *FdbRelay) Stop() error {
	atomic.StoreInt64(&f.closing, 1)
	if f.cronSchedule != "" {
		f.cronJob.Stop()
	}
	return f.l.Close()

}

func NewFdbRelay(cfg FdbConfig) (Relay, error) {
	f := new(FdbRelay)

	f.addr = cfg.Addr
	f.name = cfg.Name

	f.cert = cfg.SSLCombinedPem

	f.schema = "http"
	if f.cert != "" {
		f.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := NewFdbBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}
		f.backends = append(f.backends, backend)
	}

	f.enableMetering = cfg.EnableMetering
	f.ampqURL = cfg.AMQPUrl

	if f.enableMetering && f.ampqURL == "" {
		f.enableMetering = false
		log.Println("You have to set AMQPUrl in config for metering to work")
		log.Println("Disabling metering for now")
	}

	f.dropUnauthorized = cfg.DropUnauthorized
	f.cronSchedule = cfg.CronSchedule

	if f.cronSchedule != "" {
		f.cronJob = cron.New()
	}

	return f, nil
}

// FdbBackend Initializes a new Fdb Backend
func NewFdbBackend(cfg *FdbOutputConfig) (*FdbBackend, error) {

	log.Printf("Setting up connection (fdb.go)")
	fdb.MustAPIVersion(610)
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	return &FdbBackend{
		name: cfg.Name,
		//table: cfg.Table, // TODO check which names it should have
		db: &db,
	}, nil
}

func parseMeasurementAndTags(p models.Point) string {
	keysToIgnore := map[string]bool{"machine_id": true, "host": true}
	var b strings.Builder
	b.WriteString(string(p.Name()))

	for _, tag := range p.Tags() {
		if !keysToIgnore[string(tag.Key)] {
			fmt.Fprintf(&b, ".%s-%s", string(tag.Key), string(tag.Value))
		}
	}

	replaceSlashWithDash := func(r rune) rune {
		if r == '/' {
			return '-'
		}
		return r
	}

	return strings.Map(replaceSlashWithDash, b.String())
}

func (f *FdbRelay) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	log.Printf("request: %s", req)
	start := time.Now()
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

	machineID := ""
	if req.Header["X-Gocky-Tag-Machine-Id"] != nil {
		machineID = req.Header["X-Gocky-Tag-Machine-Id"][0]
	} else {
		if f.dropUnauthorized {
			log.Println("Gocky Headers are missing. Dropping packages...")
			jsonError(resp, http.StatusForbidden, "cannot find Gocky headers")
			return
		}
	}

	log.Printf("=================START========================\n")
	log.Println(machineID)
	for _, point := range points {

		makeQuery(machineID, point, f.backends)

		/*log.Printf("name: %s\n", string(point.Name()))
		log.Printf("tags:\n")
		for _, tag := range point.Tags() {
			log.Printf("	key: %s val: %s\n", string(tag.Key), string(tag.Value))
		}
		log.Printf("fields:\n")
		log.Println(strconv.Itoa(point.Time().Year()), strconv.Itoa(int(point.Time().Month())), strconv.Itoa(point.Time().Day()),
			strconv.Itoa(point.Time().Hour()), strconv.Itoa(point.Time().Minute()), strconv.Itoa(point.Time().Second()))*/
	}
	log.Printf("=================END========================\n")

	// handle data points

	//update metering data
	if f.enableMetering {
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

	// telegraf expects a 204 response on write*/
	resp.WriteHeader(204)
}

func createSecondValue(iter models.FieldIterator) ([]byte, error) {
	if len(iter.FieldKey()) == 0 {
		return nil, nil
	}
	switch iter.Type() {
	case models.Float:
		v, err := iter.FloatValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		return tuple.Tuple{v}.Pack(), nil
	case models.Integer:
		v, err := iter.IntegerValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		return tuple.Tuple{v}.Pack(), nil
	case models.Unsigned:
		v, err := iter.UnsignedValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		return tuple.Tuple{v}.Pack(), nil
	case models.String:
		return tuple.Tuple{iter.StringValue()}.Pack(), nil
	case models.Boolean:
		v, err := iter.BooleanValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		log.Printf("	%s = %t\n", string(iter.FieldKey()), v)
		return tuple.Tuple{v}.Pack(), nil
	}
	return nil, nil
}

func createSumValue(currentValue []byte, iter models.FieldIterator) ([]byte, error) {

	switch iter.Type() {
	case models.Float:
		v, err := iter.FloatValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		return createSumValueFloat(currentValue, v)
	case models.Integer:
		v, err := iter.IntegerValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		return createSumValueInteger(currentValue, v)
	case models.Unsigned:
		v, err := iter.UnsignedValue()
		if err != nil {
			log.Println("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			return nil, err
		}
		return createSumValueUnsigned(currentValue, v)
	}
	return nil, nil
}

// Converts tuple element to uint64 because of type inconsistency in the default behaviour of the unpacking process.
// Default behaviour returns int64 instead of uint64 when the value is small enough.
// More details here: https://forums.foundationdb.org/t/using-unsigned-64-bit-integers-with-the-go-tuple-package/468/3
func convertTupleElemToUint64(tupleElement interface{}) (uint64, error) {
	switch v := tupleElement.(type) {
	case int64:
		return uint64(v), nil
	case uint64:
		return v, nil
	default:
		return 0, fmt.Errorf("Can't convert tupleElement interface{} to uint64, no valid integer value found")
	}
}

func createSumValueFloat(currentValue []byte, value float64) ([]byte, error) {
	var sum, min, max float64
	var count uint64
	if currentValue != nil {
		currentMinuteTuple, err := tuple.Unpack(currentValue)
		if err != nil {
			log.Printf("Can't convert []byte to tuple, error: %v", err)
			return nil, err
		}
		sum = currentMinuteTuple[0].(float64)
		count, err = convertTupleElemToUint64(currentMinuteTuple[1])
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		min = currentMinuteTuple[2].(float64)
		max = currentMinuteTuple[3].(float64)

		sum += value
		count++
		if min > value {
			min = value
		}
		if max < value {
			max = value
		}
	} else {
		sum, min, max = value, value, value
		count = 1
	}
	return tuple.Tuple{sum, count, min, max}.Pack(), nil
}

func createSumValueInteger(currentValue []byte, value int64) ([]byte, error) {
	var sum, min, max int64
	var count uint64
	if currentValue != nil {
		currentMinuteTuple, err := tuple.Unpack(currentValue)
		if err != nil {
			log.Printf("Can't convert []byte to tuple, error: %v", err)
			return nil, err
		}
		sum = currentMinuteTuple[0].(int64)
		count, err = convertTupleElemToUint64(currentMinuteTuple[1])
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		min = currentMinuteTuple[2].(int64)
		max = currentMinuteTuple[3].(int64)

		sum += value
		count++
		if min > value {
			min = value
		}
		if max < value {
			max = value
		}
	} else {
		sum, min, max = value, value, value
		count = 1
	}
	return tuple.Tuple{sum, count, min, max}.Pack(), nil
}

func createSumValueUnsigned(currentValue []byte, value uint64) ([]byte, error) {
	var sum, count, min, max uint64
	if currentValue != nil {
		currentMinuteTuple, err := tuple.Unpack(currentValue)
		if err != nil {
			log.Printf("Can't convert []byte to tuple, error: %v", err)
			return nil, err
		}
		sum, err = convertTupleElemToUint64(currentMinuteTuple[0])
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		count, err = convertTupleElemToUint64(currentMinuteTuple[1])
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		min, err = convertTupleElemToUint64(currentMinuteTuple[2])
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		max, err = convertTupleElemToUint64(currentMinuteTuple[3])
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}

		sum += value
		count++
		if min > value {
			min = value
		}
		if max < value {
			max = value
		}
	} else {
		sum, min, max = value, value, value
		count = 1
	}
	return tuple.Tuple{sum, count, min, max}.Pack(), nil
}

// Transforms data points into a single sql insert query, executes it for every backend
func makeQuery(machineID string, point models.Point, backends []*FdbBackend) {
	for _, backend := range backends {
		// Each backend could have different table name for metrics data
		monitoring, err := directory.CreateOrOpen(backend.db, []string{"monitoring"}, nil)
		if err != nil {
			log.Printf("Can't open directory, error: %v", err)
			return
		}

		log.Println("Inside the goroutine")
		availableMetrics := monitoring.Sub("available_metrics")
		monitoringMinute := monitoring.Sub("metric_per_minute")
		//monitoringHour := monitoring.Sub("metric_per_hour")
		metric := parseMeasurementAndTags(point)
		_, err = backend.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

			var secondValue, minuteValue []byte
			iter := point.FieldIterator()
			for iter.Next() {
				secondValue, err = createSecondValue(iter)
				if err != nil {
					return
				}
				if secondValue == nil {
					continue
				}
				if iter.Type() == models.Float || iter.Type() == models.Integer || iter.Type() == models.Unsigned {
					previousMinuteValue := tr.Get(monitoringMinute.Pack(tuple.Tuple{machineID,
						metric + "." + string(iter.FieldKey()), point.Time().Year(), int(point.Time().Month()), point.Time().Day(),
						point.Time().Hour(), point.Time().Minute()})).MustGet()

					minuteValue, err = createSumValue(previousMinuteValue, iter)
					if err != nil {
						return
					}

					tr.Set(monitoringMinute.Pack(tuple.Tuple{machineID,
						metric + "." + string(iter.FieldKey()), point.Time().Year(), int(point.Time().Month()), point.Time().Day(),
						point.Time().Hour(), point.Time().Minute()}), []byte(minuteValue))
				}
				tr.Set(monitoring.Pack(tuple.Tuple{machineID,
					metric + "." + string(iter.FieldKey()), point.Time().Year(), int(point.Time().Month()), point.Time().Day(),
					point.Time().Hour(), point.Time().Minute(), point.Time().Second()}), []byte(secondValue))

				tr.Set(availableMetrics.Pack(tuple.Tuple{machineID, iter.Type().String(),
					metric, string(iter.FieldKey())}), []byte(tuple.Tuple{""}.Pack()))
			}

			return
		})
		if err != nil {
			log.Printf("Insertion failed at %v", backend.name)
			log.Printf("Cause of: %v", err)
		}
	}
}
