package relay

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
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

type LineInfo struct {
	metric string
	fields []string
	values []string
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
	} // TODO WILL UNCOMMENT SOON

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

func NewLineInfo() (*LineInfo, error) {

	l := new(LineInfo)
	return l, nil
}

func IsBoolean(s *string) bool {
	booleansTrue := map[string]bool{"t": true, "T": true, "true": true,
		"True": true, "TRUE": true}
	booleansFalse := map[string]bool{"f": true, "F": true, "false": true,
		"False": true, "FALSE": true}
	boolean := booleansTrue[*s] || booleansFalse[*s]
	if boolean {
		if booleansTrue[*s] {
			*s = "true"
		} else {
			*s = "false"
		}
	}
	return boolean

}

func DetectType(s *string) string {
	if strings.HasPrefix(*s, "\"") && strings.HasSuffix(*s, "\"") {
		*s = strings.TrimPrefix(*s, "\"")
		*s = strings.TrimSuffix(*s, "\"")
		return "string"
	} else if strings.HasSuffix(*s, "i") {
		*s = strings.TrimSuffix(*s, "i")
		return "int"
	} else if IsBoolean(s) {
		return "bool"
	} else {
		return "float"
	}
}

func (l *LineInfo) ParseInfuxDBLine(line string) error {
	tagsMap := make(map[string]string)
	keysToRemove := []string{"machine_id", "host"}

	splittedSpaces := strings.Split(line, " ")
	measurementAndTagsList := strings.SplitN(splittedSpaces[0], ",", 2)
	measurement := measurementAndTagsList[0]
	tags := strings.Split(measurementAndTagsList[1], ",")

	for _, tag := range tags {
		tagKV := strings.Split(tag, "=")
		tagsMap[tagKV[0]] = tagKV[1]
	}

	for _, key := range keysToRemove {
		delete(tagsMap, key)
	}

	tagsSorted := make([]string, 0, len(tagsMap))

	for k, v := range tagsMap {
		tagsSorted = append(tagsSorted, k+"-"+v)
	}

	sort.Strings(tagsSorted)

	fieldsRaw := strings.Split(splittedSpaces[1], ",")
	var b strings.Builder
	b.WriteString(measurement)
	for _, tag := range tagsSorted {
		fmt.Fprintf(&b, ".%s", tag)
	}

	replaceSlashWithDash := func(r rune) rune {
		if r == '/' {
			return '-'
		}
		return r
	}

	l.metric = strings.Map(replaceSlashWithDash, b.String())
	l.fields = make([]string, len(fieldsRaw))
	l.values = make([]string, len(fieldsRaw))

	for i, fieldRaw := range fieldsRaw {
		field := strings.Split(fieldRaw, "=")
		l.fields[i] = field[0]
		l.values[i] = field[1]
	}

	return nil
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

	for i := 0; ; i++ {
		line, err := bodyBuf.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("buf error on line %d: %s", i, err)
			}
			break
		}
		log.Printf("=================START========================\n")
		log.Printf("line: %s\n", line)

		lineInfo, err := NewLineInfo()
		if err != nil {
			log.Printf("unable to create LineInfo %s", err)
		}
		lineInfo.ParseInfuxDBLine(line)

		log.Printf("metric: %s\n", lineInfo.metric)
		log.Printf("fields:\n")
		for _, field := range lineInfo.fields {
			log.Printf("%s\n", field)
		}

		go makeQuery(machineID, lineInfo, start, f.backends)

		log.Printf("=================END========================\n")
	}

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

// Transforms data points into a single sql insert query, executes it for every back end
func makeQuery(machineID string, lineInfo *LineInfo, start time.Time, backends []*FdbBackend) {
	//I think there should be only one backend
	for _, backend := range backends {
		// Each backend could have different table name for metrics data
		monitoring, err := directory.CreateOrOpen(backend.db, []string{"monitoring"}, nil)
		if err != nil {
			log.Printf("Can't open directory, error: %v", err)
			return
		}

		log.Println("Inside the goroutine")
		availableMetrics := monitoring.Sub("available_metrics")

		//monitoringMinute := monitoring.Sub("metric_per_minute")
		//monitoringHour := monitoring.Sub("metric_per_hour")
		_, err = backend.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

			for i, field := range lineInfo.fields {
				valueType := DetectType(&lineInfo.values[i])
				var tupleToInsert []byte
				if valueType == "int" || valueType == "float" {
					var value float64
					value, err = strconv.ParseFloat(lineInfo.values[i], 64)
					if err != nil {
						log.Printf("Can't conver string to float, error: %v", err)
						return
					}
					tupleToInsert = tuple.Tuple{value}.Pack()
				} else if valueType == "bool" {
					var value bool
					value, err = strconv.ParseBool(lineInfo.values[i])
					tupleToInsert = tuple.Tuple{value}.Pack()
				} else if valueType == "string" {
					tupleToInsert = tuple.Tuple{lineInfo.values[i]}.Pack()
				} else {
					log.Printf("Unknown data type")
					return
				}
				tr.Set(monitoring.Pack(tuple.Tuple{machineID,
					lineInfo.metric + "." + field, start.Year(), int(start.Month()), start.Day(),
					start.Hour(), start.Minute(), start.Second()}), []byte(tupleToInsert))

				tr.Set(availableMetrics.Pack(tuple.Tuple{machineID, valueType,
					lineInfo.metric, field}), []byte(tuple.Tuple{""}.Pack()))
				/*tr.Set(monitoringMinute.Pack(tuple.Tuple{machineID,
					"system.load1", strconv.Itoa(start.Year()), strconv.Itoa(int(start.Month())), strconv.Itoa(start.Day()),
					strconv.Itoa(start.Hour() + 3), strconv.Itoa(start.Minute())}), []byte("15"))
				tr.Set(monitoringHour.Pack(tuple.Tuple{machineID,
					"system.load1", strconv.Itoa(start.Year()), strconv.Itoa(int(start.Month())), strconv.Itoa(start.Day()),
					strconv.Itoa(start.Hour() + 3)}), []byte("15"))*/
				log.Println(machineID,
					lineInfo.metric+"."+field, strconv.Itoa(start.Year()), strconv.Itoa(int(start.Month())), strconv.Itoa(start.Day()),
					strconv.Itoa(start.Hour()), strconv.Itoa(start.Minute()), strconv.Itoa(start.Second()), lineInfo.values[i])
			}
			return
		})
		/*_, err = backend.db.ReadTransact(func(rtr fdb.ReadTransaction) (ret interface{}, err error) {
			ri := rtr.GetRange(monitoring, fdb.RangeOptions{}).Iterator()
			for ri.Advance() {
				kv := ri.MustGet()
				t, err := monitoring.Unpack(kv.Key)
				if err != nil {
					return nil, err
				}
				log.Printf("value: %s\n", t[0].(string))
			}
			return
		})*/
		if err != nil {
			log.Printf("Insertion failed at %v", backend.name)
			log.Printf("Cause of: %v", err)
		}
		break
	}
}
