package vpcflow

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Semaphore implements the sync.Locker interface
// to help with concurrency control for fetching files.
// The buffer size of C defines the max concurrent callers
// of Lock.
type Semaphore struct {
	C chan interface{}
}

// Lock attempts to acquire the semaphore. If the limit has not
// yet been reached then the call returns immediately. If the
// limit is reached then this call blocks until the number of
// concurrent lock holders crosses back under the limit.
func (c Semaphore) Lock() {
	c.C <- nil
}

// Unlock indicates that the caller no longer needs space in
// the semaphore. This must be called at the end of a critical
// section just like any other Locker implementation.
func (c Semaphore) Unlock() {
	<-c.C
}

// FileManager is the binding between an S3 file
// download strategy and the BucketIteratorReader.
// It will be called by the BucketIteratorReader to
// fetch a new S3 Object reader as well as to return
// consumed readers for cleanup.
type FileManager interface {
	// Get a consumable reader for the next object from the manager.
	Get() (io.Reader, error)
	// Put a consumed reader back in the manager for cleanup.
	Put(io.Reader)
}

// PrefetchFileManager implements the FileManager interface by
// eagerly fetching content in the background to maximize the
// chances of having a reader ready whenever a consumer asks for
// one. Typically, this is created with the NewPrefetchPolicy
// method and used as the FetchPolicy for the BucketIteratorReader.
type PrefetchFileManager struct {
	// Queue is any implemenation of the S3API and will be used
	// to download the individual object contents.
	Queue s3iface.S3API
	// BucketIterator is the source from which the manager will
	// pull when deciding what to prefetch.
	BucketIterator BucketIterator
	// Lock is used to control concurrency of downloads.
	// Using a sync.Lock will result in sequential downloads
	// while using the Semaphore will allow up to N
	// number of concurrent downloads in the background.
	Lock sync.Locker
	// MaxBytes is used to control the amount of data fetched
	// into memory from S3. While the Lock attributes controls
	// the maximum concurrent downloads, this attribute controls
	// how much data is actually pre-fetched. Ideally, this value
	// is larger than some number of individual objects in order
	// to allow for actual prefetching of data. In the event that
	// prefetching the next object would put the buffer over the
	// limit the prefetching will stop until the buffer is drained
	// enough to contain the next file.
	//
	// One exception to this is when the buffer is empty and the next
	// file is still larger, on its own, than the max bytes. In this
	// case, the prefetcher will still download the file but will then
	// wait for the buffer to drain before downloading the next file.
	// This means that if the MaxBytes are set to less than the average
	// file size then the prefetcher may degenerate into sequential
	// downloads.
	MaxBytes int64
	// Ready is the channel/buffer on which downloaded files are placed
	// while awaiting consumption. This channel may be given a buffer
	// size or be blocking.
	Ready chan io.Reader

	wg          sync.WaitGroup
	prefetched  int64
	errs        chan error
	sizes       sync.Map
	downloader  *s3manager.Downloader
	initialized int32
}

// Get a prefetched file. If prefetch is lagging behind then
// this call will block until a file is available. If any
// error was encountered since the last call to Get then it
// is returned.
func (f *PrefetchFileManager) Get() (io.Reader, error) {
	select {
	case e := <-f.errs:
		return nil, e
	default:
	}
	select {
	case e := <-f.errs:
		return nil, e
	case r := <-f.Ready:
		return r, nil
	}
}

// Put returns a file to the manager for cleanup.
func (f *PrefetchFileManager) Put(r io.Reader) {
	var size, _ = f.sizes.Load(r)
	f.sizes.Delete(r)
	_ = atomic.AddInt64(&f.prefetched, -size.(int64))
}

func (f *PrefetchFileManager) prefetchFile(lf LogFile) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	defer f.wg.Done()

	var fileBuff = make([]byte, 0, int(lf.Size))
	var awsBuff = aws.NewWriteAtBuffer(fileBuff)
	var _, e = f.downloader.Download(awsBuff, &s3.GetObjectInput{
		Key:    aws.String(lf.Key),
		Bucket: aws.String(lf.Bucket),
	})
	if e != nil {
		f.errs <- e
		return
	}

	var result io.Reader
	var finalBuff = bytes.NewBuffer(awsBuff.Bytes())
	result, e = gzip.NewReader(finalBuff)
	if e != nil {
		f.errs <- e
		return
	}
	f.sizes.Store(result, lf.Size)
	f.Ready <- result
}

func (f *PrefetchFileManager) init() {
	f.downloader = s3manager.NewDownloaderWithClient(f.Queue, func(d *s3manager.Downloader) {
		d.Concurrency = 1
	})
	f.errs = make(chan error, len(f.Ready))
	atomic.AddInt32(&f.initialized, 1)
}

// Prefetch starts a loop that consumes from the attached BucketIterator
// and attempts to load that content before it is needed.
func (f *PrefetchFileManager) Prefetch() {
	if atomic.LoadInt32(&f.initialized) < 1 {
		f.init()
	}
	for f.BucketIterator.Iterate() {
		var curr = f.BucketIterator.Current()
		// Files of zero size don't have content to download so we drop
		// them here if they made it. This is commonly due to directories
		// showing up as objects.
		if curr.Size == 0 {
			continue
		}
		var prefetched = atomic.LoadInt64(&f.prefetched)
		for curr.Size+prefetched > f.MaxBytes && prefetched != 0 {
			time.Sleep(250 * time.Microsecond) // TODO: better waiting pattern.
			prefetched = atomic.LoadInt64(&f.prefetched)
		}
		_ = atomic.AddInt64(&f.prefetched, curr.Size)
		f.wg.Add(1)
		go f.prefetchFile(curr)
	}
	var e = f.BucketIterator.Close()
	if e != nil {
		f.errs <- e
	}
	f.wg.Wait()
	close(f.Ready)
}

// NewPrefetchPolicy implements the signature required for
// the BucketIteratorReader.FetchPolicy by producing a FileManager
// that will pre-fetch content before it is requested. This can
// dramatically speed up reading but must be tuned to the correct
// concurrency and memory limits of a system.
func NewPrefetchPolicy(q s3iface.S3API, maxBytes int64, maxConcurrent int) func(BucketIterator) FileManager {
	return func(iter BucketIterator) FileManager {
		var sem = &Semaphore{C: make(chan interface{}, maxConcurrent)}
		var fm = &PrefetchFileManager{
			Lock:           sem,
			MaxBytes:       maxBytes,
			Queue:          q,
			Ready:          make(chan io.Reader, 1024),
			BucketIterator: iter,
		}
		go fm.Prefetch()
		return fm
	}
}

// BucketIteratorReader converts implementations of the
// BucketIterator interfaces into an io.ReaderCloser that acts
// as a continuous stream of data from all the files returned
// by the interator.
type BucketIteratorReader struct {
	BucketIterator BucketIterator
	FetchPolicy    func(BucketIterator) FileManager

	initialized bool
	policy      FileManager
	current     io.Reader
	exhausted   bool
}

// Read from files produced by the iterator as though they are
// one, continuous file.
func (r *BucketIteratorReader) Read(b []byte) (int, error) {
	if !r.initialized {
		r.policy = r.FetchPolicy(r.BucketIterator)
		r.initialized = true
	}
	for {
		// Once empty, this reader cannot be read from anymore.
		if r.exhausted {
			return 0, io.EOF
		}
		if r.current == nil {
			var current, err = r.policy.Get()
			if err != nil {
				return 0, err
			}
			r.current = current
		}
		// Reading nil from the channel is our best signal that
		// the channel is closed so we exit with io.EOF to indicate
		// that all content have been consumed. This _does_ place
		// a requirement on producers to the ready channel that
		// they must never write nil.
		if r.current == nil {
			r.exhausted = true
			return 0, io.EOF
		}
		var n, e = r.current.Read(b)
		if e == io.EOF {
			r.policy.Put(r.current)
			r.current = nil
			// There are some cases where .Read() can return the EOF
			// marker but _also_ data read from the stream. To account
			// for this, we need to check if any bytes were read before
			// exiting. If we get both EOF and bytes read then we need
			// to return those bytes to the user with a nil error to
			// indicate the caller should call again to get more bytes
			// even if they got less than they asked for this time.
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, e
	}
}

// Close the reader and the underlying BucketIterator.
// The reader may not be used again after calling Close().
func (r *BucketIteratorReader) Close() error {
	// Set exhausted to ensure future reads return
	// io.EOF.
	r.exhausted = true
	return r.BucketIterator.Close()
}
