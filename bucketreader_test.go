package vpcflow

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aws "github.com/aws/aws-sdk-go/aws"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestSemaphore(t *testing.T) {
	var size = 10
	var lock sync.Locker = &Semaphore{C: make(chan interface{}, size)}
	var concurrentCount int64
	var wg = &sync.WaitGroup{}
	var done = make(chan interface{})
	for x := 0; x < (size * 2); x = x + 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock.Lock()
			_ = atomic.AddInt64(&concurrentCount, 1)
			<-done
			lock.Unlock()
		}()
	}
	var after = time.After(10 * time.Millisecond)
	var ticker = time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var now = atomic.LoadInt64(&concurrentCount)
WAITLOOP:
	for now != int64(size) {
		select {
		case <-ticker.C:
		case <-after:
			assert.Failf(t, "lock failed", "%d concurrent calls but lock is set for %d", now, size)
			break WAITLOOP
		}
		now = atomic.LoadInt64(&concurrentCount)
	}
	close(done)
	wg.Wait()
}

func TestBucketIteratorReaderExhaustedWhenPolicyReturnsNil(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var iter = NewMockBucketIterator(ctrl)
	var manager = NewMockFileManager(ctrl)
	var policy = func(BucketIterator) FileManager {
		return manager
	}
	var b = make([]byte, 4096)
	var br = &BucketIteratorReader{
		BucketIterator: iter,
		FetchPolicy:    policy,
	}

	manager.EXPECT().Get().Return(nil, nil)
	var _, e = br.Read(b)
	assert.Equal(t, e, io.EOF, "exhausted reader did not return io.EOF")
	_, e = br.Read(b)
	assert.Equal(t, e, io.EOF, "reader did not return io.EOF again after first returning it")
}

func TestBucketIteratorReaderSignalsFileManagerErrors(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var iter = NewMockBucketIterator(ctrl)
	var manager = NewMockFileManager(ctrl)
	var policy = func(BucketIterator) FileManager {
		return manager
	}
	var b = make([]byte, 4096)
	var br = &BucketIteratorReader{
		BucketIterator: iter,
		FetchPolicy:    policy,
	}

	// The reader is supposed to expose any error that it receives
	// from the file manager to that consumers can be aware that
	// at least one file did not load correctly. Continuing to use
	// the reader after getting any error other than io.EOF is an
	// indicator that the consumer is accepting of possible data loss
	// and wants to continue with any files that were loaded successfully.
	// To handle this, the reader proxies errors from the manager but
	// allows subsequent calls to read to work a though an error was
	// not encountered.
	manager.EXPECT().Get().Return(nil, errors.New(""))
	var _, e = br.Read(b)
	assert.NotNil(t, e, "reader did not expose file errors")
	manager.EXPECT().Get().Return(nil, nil)
	_, e = br.Read(b)
	assert.Equal(t, e, io.EOF, "reader did not recover from file error")
}

func TestBucketIteratorReaderAutoFetchesNewFilesOnEOF(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var reader = NewMockReader(ctrl)
	var iter = NewMockBucketIterator(ctrl)
	var manager = NewMockFileManager(ctrl)
	var policy = func(BucketIterator) FileManager {
		return manager
	}
	var b = make([]byte, 4096)
	var br = &BucketIteratorReader{
		BucketIterator: iter,
		FetchPolicy:    policy,
	}

	// The scenario here is that the file manager returns
	// a valid file that we try to read from but that
	// file gives us an io.EOF with no bytes read. The
	// correct behavior is to then fetch the next file
	// and attempt to read from it instead. To terminate
	// the scenario we return the stop signal for the
	// second file.
	manager.EXPECT().Get().Return(reader, nil)
	reader.EXPECT().Read(b).Return(0, io.EOF)
	manager.EXPECT().Put(reader)
	manager.EXPECT().Get().Return(nil, nil)

	// The expected outcome from this scenario is an io.EOF
	// error from the reader.
	var _, e = br.Read(b)
	assert.Equal(t, e, io.EOF)
}

func TestBucketIteratorReaderReturnsIfEOFAndNonEmptyRead(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var reader = NewMockReader(ctrl)
	var iter = NewMockBucketIterator(ctrl)
	var manager = NewMockFileManager(ctrl)
	var policy = func(BucketIterator) FileManager {
		return manager
	}
	var b = make([]byte, 4096)
	var br = &BucketIteratorReader{
		BucketIterator: iter,
		FetchPolicy:    policy,
	}

	// This scenario replicates what happens when
	// a reader returned from the file manager returns
	// a non-zero value for bytes read in addition to the
	// io.EOF marker. This is allowed by the io.Reader
	// interface specification and the choice to return
	// a non-zero "n" value with an io.EOF versus returning
	// a non-zero "n" with "nil" followed by a zero "n" and
	// io.EOF is determined by the reader implementation.
	manager.EXPECT().Get().Return(reader, nil)
	reader.EXPECT().Read(b).Return(5, io.EOF)
	manager.EXPECT().Put(reader)

	// The expected outcome from this scenario is a non-zero
	// "n" value and a nil error. This behavior was chosen,
	// over returning both "n" and io.EOF together, because
	// there may still be additional content to read from
	// other underlying files.
	var n, e = br.Read(b)
	assert.Nil(t, e)
	assert.Equal(t, n, 5)
}

func TestBucketIteratorReaderSuccess(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var reader = NewMockReader(ctrl)
	var iter = NewMockBucketIterator(ctrl)
	var manager = NewMockFileManager(ctrl)
	var policy = func(BucketIterator) FileManager {
		return manager
	}
	var b = make([]byte, 4096)
	var br = &BucketIteratorReader{
		BucketIterator: iter,
		FetchPolicy:    policy,
	}

	manager.EXPECT().Get().Return(reader, nil)
	reader.EXPECT().Read(b).Return(len(b), nil)

	var n, e = br.Read(b)
	assert.Nil(t, e)
	assert.Equal(t, n, len(b))
}

func TestBucketIteratorReaderClose(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var iter = NewMockBucketIterator(ctrl)
	var manager = NewMockFileManager(ctrl)
	var policy = func(BucketIterator) FileManager {
		return manager
	}
	var b = make([]byte, 4096)
	var br = &BucketIteratorReader{
		BucketIterator: iter,
		FetchPolicy:    policy,
	}
	var closeErr = errors.New("")

	iter.EXPECT().Close().Return(closeErr)

	var e = br.Close()
	assert.Equal(t, e, closeErr, "close did not proxy the iterator close error")
	// After closing, all read calls should return io.EOF.
	_, e = br.Read(b)
	assert.Equal(t, e, io.EOF)
}

func TestPrefetchFileManagerGet(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var getErr = errors.New("")
	var reader = NewMockReader(ctrl)
	var fm = &PrefetchFileManager{
		Ready: make(chan io.Reader, 1),
		errs:  make(chan error, 1),
	}

	// If only an error is enqueued then the error should be returned.
	fm.errs <- getErr
	var r, e = fm.Get()
	assert.Equal(t, e, getErr, "file manager did not return the enqueued error")
	assert.Nil(t, r, "file manager returned file when it should be only an error")

	// If only a file is enqueued then the file should be returned.
	fm.Ready <- reader
	r, e = fm.Get()
	assert.Nil(t, e, "file manager returned an error when it should have been nil")
	assert.Equal(t, r, reader)

	// If both are enqueued then the error is sent first and the file on a subsequent
	// call.
	fm.Ready <- reader
	fm.errs <- getErr
	r, e = fm.Get()
	assert.Equal(t, e, getErr, "file manager did not return the enqueued error")
	assert.Nil(t, r, "file manager returned file when it should be only an error")
	r, e = fm.Get()
	assert.Nil(t, e, "file manager returned an error when it should have been nil")
	assert.Equal(t, r, reader)

	// If neither are enqueued then the first to arrive is returned.
	// Note: the behavior of select is randomized when there are multiple
	// channels ready for read. Because of this we won't test the case of
	// racing an error and a reader and accept that case as non-deterministic.
	go func() {
		time.Sleep(time.Millisecond)
		fm.errs <- getErr
	}()
	r, e = fm.Get()
	assert.Equal(t, e, getErr, "file manager did not return the enqueued error")
	assert.Nil(t, r, "file manager returned file when it should be only an error")
	go func() {
		time.Sleep(time.Millisecond)
		fm.Ready <- reader
	}()
	r, e = fm.Get()
	assert.Nil(t, e, "file manager returned an error when it should have been nil")
	assert.Equal(t, r, reader)
}

func TestPrefetchFileManagerPrefetch(t *testing.T) {
	var ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	var iter = NewMockBucketIterator(ctrl)
	var queue = NewMockS3API(ctrl)
	var locker = NewMockLocker(ctrl)
	// gzipBody is a gzip'd version of 1000 empty bytes. we need any valid gzip
	// content so empty bytes are as good as other content.
	var gzipBody = []byte{
		31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 98, 24, 5, 163, 96, 20, 12, 123, 0,
		8, 0, 0, 255, 255, 128, 23, 11, 6, 232, 3, 0, 0,
	}
	var maxBytes = int64(10 * len(gzipBody))
	var lf = LogFile{Size: int64(len(gzipBody)), Key: "file", Bucket: "bucket"}
	var ready = make(chan io.Reader, 100)
	var fm = &PrefetchFileManager{
		Queue:          queue,
		BucketIterator: iter,
		MaxBytes:       maxBytes,
		Ready:          ready,
		Lock:           locker,
	}
	fm.init()

	iter.EXPECT().Iterate().Return(true).Times(11)
	iter.EXPECT().Current().Return(lf).Times(11)
	iter.EXPECT().Iterate().Return(false).Times(1)
	iter.EXPECT().Close().Return(nil).Times(1)
	locker.EXPECT().Lock().Times(11)
	locker.EXPECT().Unlock().Times(11)
	for x := 0; x < 11; x = x + 1 {
		queue.EXPECT().GetObjectWithContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.GetObjectOutput{
			Body:            ioutil.NopCloser(bytes.NewBuffer(gzipBody)),
			ContentLength:   aws.Int64(int64(len(gzipBody))),
			ContentEncoding: aws.String("gzip"),
		}, nil)
	}

	var ticker = time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var after = time.After(100 * time.Millisecond)
	go fm.Prefetch()

WAITLOOP:
	for {
		select {
		case <-ticker.C:
			if len(ready) == 10 {
				break WAITLOOP
			}
		case <-after:
			assert.FailNow(t, "not enough files were prefetched", "%d", len(ready))
		case e := <-fm.errs:
			assert.FailNow(t, e.Error())
		}
	}
	for x := 0; x < 10; x = x + 1 {
		fm.Put(<-ready)
	}

	// Now that we've read some from the buffer, check that the next element made it in.
	after = time.After(100 * time.Millisecond)
	select {
	case r := <-ready:
		fm.Put(r)
	case <-after:
		assert.FailNow(t, "prefetch did not continue after buffer drained")
	case e := <-fm.errs:
		assert.FailNow(t, e.Error())
	}
	var done = make(chan interface{})
	go func() {
		for range ready {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		assert.FailNow(t, "prefetch never completed and exited")
	}
}
