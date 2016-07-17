package bqloadbatcher

import (
	"encoding/json"
	"fmt"
	"time"

	"code.google.com/p/goauth2/oauth/jwt"
	"google.golang.org/api/bigquery/v2"
)

// Row is a row that will be inserted into BigQuery.
type Row interface {
	DatasetID() string
	TableID() string
	Data() *json.RawMessage
}

type Error struct {
	Name     string
	Err      error
	Size     int
	Duration time.Duration
}

// Result wraps information regarding successful loads.
type Result struct {
	Job      *bigquery.Job
	Name     string
	Duration time.Duration
	Size     int
}

// Options is the configuration struct for BQ.
type Options struct {
	// The BQ project id to use.
	ProjectID string

	// The duration of time that rows should be batched.
	WindowDuration time.Duration

	// Where to store the files that BQ batches the rows.
	FileDir string

	// The PEM to authenticate with BQ.
	PEM []byte

	// The email to authenticate with BQ.
	Email string

	// Whether BQ should try to detect your schema from your data, false by default.
	AutoDetectSchema bool

	// To define your own schema for BQ, nil by default.
	Schema *bigquery.TableSchema

	// The max number of concurrent jobs sent to BQ.
	MaxJobs int

	// Number of workers.
	Workers int

	// Buffer size.
	BufferSize int
}

// Loader batches rows and inserts them into Google's BigQuery.
type Loader struct {
	*Options

	service   *bigquery.Service
	successes chan Result
	errors    chan Error
	jobSem    chan bool
	ticker    *time.Ticker
	input     chan Row
	buffer    *buffer
}

// New creates a new Loader using the given options.
func New(opts *Options) (*Loader, error) {
	token := jwt.NewToken(opts.Email, bigquery.BigqueryScope, opts.PEM)
	transport, err := jwt.NewTransport(token)
	if err != nil {
		return nil, err
	}
	client := transport.Client()

	bigqueryService, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(opts.WindowDuration)

	l := &Loader{
		Options:   opts,
		service:   bigqueryService,
		ticker:    ticker,
		errors:    make(chan Error, opts.BufferSize),
		jobSem:    make(chan bool, opts.MaxJobs),
		successes: make(chan Result, opts.BufferSize),
		input:     make(chan Row, opts.BufferSize),
		buffer:    newBuffer(),
	}

	for i := 0; i < opts.Workers; i++ {
		go l.dispatcher()
	}

	go l.tick()

	return l, nil
}

// Close triggers a shutdown, flushing any messages it may have
// buffered. The shutdown has completed when both the Errors and
// Successes channels have been closed. When calling Close, you *must*
// continue to read from those channels in order to drain the results
// of any messages in flight.
func (l *Loader) Close() {
	l.ticker.Stop()
	close(l.input)

	l.insertAll()

	close(l.successes)
	close(l.errors)
}

// Input is the input channel for you to write rows to that you wish
// to send to BQ.
func (l *Loader) Input() chan<- Row {
	return l.input
}

// Successes is the success output channel back to you when a load job
// successfully completes.  You MUST read from this channel or the
// things will deadlock.
func (l *Loader) Successes() <-chan Result {
	return l.successes
}

// Errors is the success output channel back to you when a load job
// fails.  You MUST read from this channel or the things will
// deadlock.
func (l *Loader) Errors() <-chan Error {
	return l.errors
}

func (l *Loader) tick() {
	for {
		select {
		case _, ok := <-l.ticker.C:
			if !ok {
				return
			}

			l.insertAll()
		}
	}
}

func (l *Loader) dispatcher() {
	for r := range l.input {
		if err := l.write(r); err != nil {
			l.errors <- Error{
				Err:  err,
				Size: 1,
			}
		}
	}
}

func (l *Loader) insertAll() {
	l.buffer.shift()

	n := l.buffer.iter()

	for k, v := range l.buffer.readers(n) {
		l.jobSem <- true

		go func(k string, v *file) {
			res, err := l.insert(v)

			if err.Err != nil {
				l.errors <- err
			} else {
				l.successes <- res
			}

			l.buffer.remove(n, k)

			<-l.jobSem
		}(k, v)
	}
}

func (l *Loader) insert(f *file) (res Result, err Error) {
	configuration := &bigquery.JobConfigurationLoad{
		CreateDisposition: "CREATE_IF_NEEDED",
		WriteDisposition:  "WRITE_APPEND",
		Autodetect:        l.AutoDetectSchema,
		DestinationTable: &bigquery.TableReference{
			ProjectId: l.ProjectID,
			DatasetId: f.datasetID,
			TableId:   f.tableID,
		},
		SourceFormat: "NEWLINE_DELIMITED_JSON",
	}

	if l.Schema != nil {
		configuration.Schema = l.Schema
	}

	f.Flush()

	size := f.Len()

	t := time.Now()

	_, e := l.service.Jobs.Insert(l.ProjectID, &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: configuration,
		},
	}).Media(f).Do()

	err.Size = size
	err.Name = f.name
	err.Duration = time.Since(t)

	if e != nil {
		err.Err = e

		return res, err
	}

	if e = f.Close(); e != nil {
		err.Err = e

		return res, err
	}

	if e = f.Remove(); e != nil {
		err.Err = e

		return res, err
	}

	return Result{
		Size:     size,
		Name:     f.name,
		Duration: time.Since(t),
	}, err
}

func (l *Loader) write(r Row) error {
	name := l.name(r)
	f, err := l.buffer.writer(name)

	if err != nil {
		return err
	}

	err = f.Write(*r.Data())

	return err
}

func (bq *Loader) prefix() int64 {
	return time.Now().Truncate(bq.WindowDuration).Unix()
}

func (bq *Loader) name(r Row) string {
	return fmt.Sprintf("%d-%s-%s", bq.prefix(), r.DatasetID(), r.TableID())
}
