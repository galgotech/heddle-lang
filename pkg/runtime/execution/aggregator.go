package execution

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// BatchConfig limits the aggregation buffer.
type BatchConfig struct {
	MaxBatchSize int           // Maximum number of requests to accumulate
	TimeWindow   time.Duration // Maximum time to wait before flushing
}

// Promise represents the future result for an asynchronous batched execution.
type Promise struct {
	ResCh chan arrow.Record
	ErrCh chan error
}

type AggregationBuffer struct {
	mu        sync.Mutex
	signature string
	records   []arrow.Record
	promises  []Promise
	totalRows int
	flushCh   chan struct{}
	timer     *time.Timer
}

// Executor represents the external function that will be executed with the unified Table.
type Executor func(ctx context.Context, signature string, table arrow.Table) (arrow.Table, error)

type Aggregator struct {
	config   BatchConfig
	buffers  sync.Map // map[signature]*AggregationBuffer
	pool     memory.Allocator
	executor Executor
}

// NewAggregator creates a new Aggregator.
func NewAggregator(config BatchConfig, pool memory.Allocator, executor Executor) *Aggregator {
	return &Aggregator{
		config:   config,
		pool:     pool,
		executor: executor,
	}
}

// Add pushes a new Arrow record into the buffer for the target signature and returns a Promise.
func (a *Aggregator) Add(signature string, rec arrow.Record) *Promise {
	// Retain the record while we buffer it
	rec.Retain()

	promise := Promise{
		ResCh: make(chan arrow.Record, 1),
		ErrCh: make(chan error, 1),
	}

	val, _ := a.buffers.LoadOrStore(signature, &AggregationBuffer{
		signature: signature,
		records:   make([]arrow.Record, 0),
		promises:  make([]Promise, 0),
		flushCh:   make(chan struct{}, 1),
	})

	buffer := val.(*AggregationBuffer)

	buffer.mu.Lock()
	buffer.records = append(buffer.records, rec)
	buffer.promises = append(buffer.promises, promise)
	buffer.totalRows += int(rec.NumRows())
	currentLen := len(buffer.records)

	// If this is the first record, start the timer
	if currentLen == 1 {
		buffer.timer = time.AfterFunc(a.config.TimeWindow, func() {
			select {
			case buffer.flushCh <- struct{}{}:
			default:
			}
		})
		
		// Start flush routine for this batch
		go a.flushRoutine(buffer)
	}

	// Trigger flush if size limit reached
	if currentLen >= a.config.MaxBatchSize {
		if buffer.timer != nil {
			buffer.timer.Stop()
		}
		select {
		case buffer.flushCh <- struct{}{}:
		default:
		}
	}

	buffer.mu.Unlock()

	return &promise
}

func (a *Aggregator) flushRoutine(buffer *AggregationBuffer) {
	<-buffer.flushCh

	// Detach the buffer from the map so new incoming requests start a fresh batch
	a.buffers.Delete(buffer.signature)

	buffer.mu.Lock()
	records := buffer.records
	promises := buffer.promises
	buffer.mu.Unlock()

	// Execute batch
	err := a.executeBatch(buffer.signature, records, promises)
	if err != nil {
		// Fallback to individual execution
		for i, rec := range records {
			a.executeIndividual(buffer.signature, rec, promises[i])
		}
	}

	// Release buffered records
	for _, rec := range records {
		rec.Release()
	}
}

func (a *Aggregator) executeBatch(signature string, records []arrow.Record, promises []Promise) error {
	if len(records) == 0 {
		return nil
	}

	table := array.NewTableFromRecords(records[0].Schema(), records)
	defer table.Release()

	resTable, err := a.executor(context.Background(), signature, table)
	if err != nil {
		return err // batch failed
	}
	defer resTable.Release()

	// Demultiplexing based on row counts
	tr := array.NewTableReader(resTable, 0)
	defer tr.Release()

	var resRecords []arrow.Record
	for tr.Next() {
		rec := tr.Record()
		rec.Retain()
		resRecords = append(resRecords, rec)
	}

	if len(resRecords) == 0 {
		return errors.New("executor returned empty table")
	}

	resTableCombined := array.NewTableFromRecords(resTable.Schema(), resRecords)
	defer resTableCombined.Release()
	for _, r := range resRecords {
		r.Release()
	}

	for i, _ := range records {
		if len(resRecords) == len(records) {
			resRec := resRecords[i]
			resRec.Retain()
			promises[i].ResCh <- resRec
			promises[i].ErrCh <- nil
		} else {
			return errors.New("shape mismatch from executor") // fallback
		}
	}

	return nil
}

func (a *Aggregator) executeIndividual(signature string, rec arrow.Record, promise Promise) {
	table := array.NewTableFromRecords(rec.Schema(), []arrow.Record{rec})
	defer table.Release()

	resTable, err := a.executor(context.Background(), signature, table)
	if err != nil {
		promise.ErrCh <- err
		promise.ResCh <- nil
		return
	}
	defer resTable.Release()

	tr := array.NewTableReader(resTable, 0)
	defer tr.Release()

	if tr.Next() {
		resRec := tr.Record()
		resRec.Retain()
		promise.ResCh <- resRec
		promise.ErrCh <- nil
	} else {
		promise.ErrCh <- errors.New("empty result from executor")
		promise.ResCh <- nil
	}
}
