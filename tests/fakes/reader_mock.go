package fakes

// Code generated by http://github.com/gojuno/minimock (dev). DO NOT EDIT.

//go:generate minimock -i io.Reader -o ./reader_mock.go

import (
	"sync"
	mm_atomic "sync/atomic"
	mm_time "time"

	"github.com/gojuno/minimock/v3"
)

// ReaderMock implements io.Reader
type ReaderMock struct {
	t minimock.Tester

	funcRead          func(p []byte) (n int, err error)
	inspectFuncRead   func(p []byte)
	afterReadCounter  uint64
	beforeReadCounter uint64
	ReadMock          mReaderMockRead
}

// NewReaderMock returns a mock for io.Reader
func NewReaderMock(t minimock.Tester) *ReaderMock {
	m := &ReaderMock{t: t}
	if controller, ok := t.(minimock.MockController); ok {
		controller.RegisterMocker(m)
	}

	m.ReadMock = mReaderMockRead{mock: m}
	m.ReadMock.callArgs = []*ReaderMockReadParams{}

	return m
}

type mReaderMockRead struct {
	mock               *ReaderMock
	defaultExpectation *ReaderMockReadExpectation
	expectations       []*ReaderMockReadExpectation

	callArgs []*ReaderMockReadParams
	mutex    sync.RWMutex
}

// ReaderMockReadExpectation specifies expectation struct of the Reader.Read
type ReaderMockReadExpectation struct {
	mock    *ReaderMock
	params  *ReaderMockReadParams
	results *ReaderMockReadResults
	Counter uint64
}

// ReaderMockReadParams contains parameters of the Reader.Read
type ReaderMockReadParams struct {
	p []byte
}

// ReaderMockReadResults contains results of the Reader.Read
type ReaderMockReadResults struct {
	n   int
	err error
}

// Expect sets up expected params for Reader.Read
func (mmRead *mReaderMockRead) Expect(p []byte) *mReaderMockRead {
	if mmRead.mock.funcRead != nil {
		mmRead.mock.t.Fatalf("ReaderMock.Read mock is already set by Set")
	}

	if mmRead.defaultExpectation == nil {
		mmRead.defaultExpectation = &ReaderMockReadExpectation{}
	}

	mmRead.defaultExpectation.params = &ReaderMockReadParams{p}
	for _, e := range mmRead.expectations {
		if minimock.Equal(e.params, mmRead.defaultExpectation.params) {
			mmRead.mock.t.Fatalf("Expectation set by When has same params: %#v", *mmRead.defaultExpectation.params)
		}
	}

	return mmRead
}

// Inspect accepts an inspector function that has same arguments as the Reader.Read
func (mmRead *mReaderMockRead) Inspect(f func(p []byte)) *mReaderMockRead {
	if mmRead.mock.inspectFuncRead != nil {
		mmRead.mock.t.Fatalf("Inspect function is already set for ReaderMock.Read")
	}

	mmRead.mock.inspectFuncRead = f

	return mmRead
}

// Return sets up results that will be returned by Reader.Read
func (mmRead *mReaderMockRead) Return(n int, err error) *ReaderMock {
	if mmRead.mock.funcRead != nil {
		mmRead.mock.t.Fatalf("ReaderMock.Read mock is already set by Set")
	}

	if mmRead.defaultExpectation == nil {
		mmRead.defaultExpectation = &ReaderMockReadExpectation{mock: mmRead.mock}
	}
	mmRead.defaultExpectation.results = &ReaderMockReadResults{n, err}
	return mmRead.mock
}

//Set uses given function f to mock the Reader.Read method
func (mmRead *mReaderMockRead) Set(f func(p []byte) (n int, err error)) *ReaderMock {
	if mmRead.defaultExpectation != nil {
		mmRead.mock.t.Fatalf("Default expectation is already set for the Reader.Read method")
	}

	if len(mmRead.expectations) > 0 {
		mmRead.mock.t.Fatalf("Some expectations are already set for the Reader.Read method")
	}

	mmRead.mock.funcRead = f
	return mmRead.mock
}

// When sets expectation for the Reader.Read which will trigger the result defined by the following
// Then helper
func (mmRead *mReaderMockRead) When(p []byte) *ReaderMockReadExpectation {
	if mmRead.mock.funcRead != nil {
		mmRead.mock.t.Fatalf("ReaderMock.Read mock is already set by Set")
	}

	expectation := &ReaderMockReadExpectation{
		mock:   mmRead.mock,
		params: &ReaderMockReadParams{p},
	}
	mmRead.expectations = append(mmRead.expectations, expectation)
	return expectation
}

// Then sets up Reader.Read return parameters for the expectation previously defined by the When method
func (e *ReaderMockReadExpectation) Then(n int, err error) *ReaderMock {
	e.results = &ReaderMockReadResults{n, err}
	return e.mock
}

// Read implements io.Reader
func (mmRead *ReaderMock) Read(p []byte) (n int, err error) {
	mm_atomic.AddUint64(&mmRead.beforeReadCounter, 1)
	defer mm_atomic.AddUint64(&mmRead.afterReadCounter, 1)

	if mmRead.inspectFuncRead != nil {
		mmRead.inspectFuncRead(p)
	}

	mm_params := &ReaderMockReadParams{p}

	// Record call args
	mmRead.ReadMock.mutex.Lock()
	mmRead.ReadMock.callArgs = append(mmRead.ReadMock.callArgs, mm_params)
	mmRead.ReadMock.mutex.Unlock()

	for _, e := range mmRead.ReadMock.expectations {
		if minimock.Equal(e.params, mm_params) {
			mm_atomic.AddUint64(&e.Counter, 1)
			return e.results.n, e.results.err
		}
	}

	if mmRead.ReadMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmRead.ReadMock.defaultExpectation.Counter, 1)
		mm_want := mmRead.ReadMock.defaultExpectation.params
		mm_got := ReaderMockReadParams{p}
		if mm_want != nil && !minimock.Equal(*mm_want, mm_got) {
			mmRead.t.Errorf("ReaderMock.Read got unexpected parameters, want: %#v, got: %#v%s\n", *mm_want, mm_got, minimock.Diff(*mm_want, mm_got))
		}

		mm_results := mmRead.ReadMock.defaultExpectation.results
		if mm_results == nil {
			mmRead.t.Fatal("No results are set for the ReaderMock.Read")
		}
		return (*mm_results).n, (*mm_results).err
	}
	if mmRead.funcRead != nil {
		return mmRead.funcRead(p)
	}
	mmRead.t.Fatalf("Unexpected call to ReaderMock.Read. %v", p)
	return
}

// ReadAfterCounter returns a count of finished ReaderMock.Read invocations
func (mmRead *ReaderMock) ReadAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmRead.afterReadCounter)
}

// ReadBeforeCounter returns a count of ReaderMock.Read invocations
func (mmRead *ReaderMock) ReadBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmRead.beforeReadCounter)
}

// Calls returns a list of arguments used in each call to ReaderMock.Read.
// The list is in the same order as the calls were made (i.e. recent calls have a higher index)
func (mmRead *mReaderMockRead) Calls() []*ReaderMockReadParams {
	mmRead.mutex.RLock()

	argCopy := make([]*ReaderMockReadParams, len(mmRead.callArgs))
	copy(argCopy, mmRead.callArgs)

	mmRead.mutex.RUnlock()

	return argCopy
}

// MinimockReadDone returns true if the count of the Read invocations corresponds
// the number of defined expectations
func (m *ReaderMock) MinimockReadDone() bool {
	for _, e := range m.ReadMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.ReadMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterReadCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcRead != nil && mm_atomic.LoadUint64(&m.afterReadCounter) < 1 {
		return false
	}
	return true
}

// MinimockReadInspect logs each unmet expectation
func (m *ReaderMock) MinimockReadInspect() {
	for _, e := range m.ReadMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Errorf("Expected call to ReaderMock.Read with params: %#v", *e.params)
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.ReadMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterReadCounter) < 1 {
		if m.ReadMock.defaultExpectation.params == nil {
			m.t.Error("Expected call to ReaderMock.Read")
		} else {
			m.t.Errorf("Expected call to ReaderMock.Read with params: %#v", *m.ReadMock.defaultExpectation.params)
		}
	}
	// if func was set then invocations count should be greater than zero
	if m.funcRead != nil && mm_atomic.LoadUint64(&m.afterReadCounter) < 1 {
		m.t.Error("Expected call to ReaderMock.Read")
	}
}

// MinimockFinish checks that all mocked methods have been called the expected number of times
func (m *ReaderMock) MinimockFinish() {
	if !m.minimockDone() {
		m.MinimockReadInspect()
		m.t.FailNow()
	}
}

// MinimockWait waits for all mocked methods to be called the expected number of times
func (m *ReaderMock) MinimockWait(timeout mm_time.Duration) {
	timeoutCh := mm_time.After(timeout)
	for {
		if m.minimockDone() {
			return
		}
		select {
		case <-timeoutCh:
			m.MinimockFinish()
			return
		case <-mm_time.After(10 * mm_time.Millisecond):
		}
	}
}

func (m *ReaderMock) minimockDone() bool {
	done := true
	return done &&
		m.MinimockReadDone()
}