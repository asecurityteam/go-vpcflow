package vpcflow

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	idxVersion     = 0
	idxAccountID   = 1
	idxInterfaceID = 2
	idxSrcAddr     = 3
	idxDstAddr     = 4
	idxSrcPort     = 5
	idxDstPort     = 6
	idxProtocol    = 7
	idxPackets     = 8
	idxBytes       = 9
	idxStart       = 10
	idxEnd         = 11
	idxAction      = 12
	idxLogStatus   = 13
)

var keyFields = map[int]bool{
	idxVersion:     true,
	idxAccountID:   true,
	idxInterfaceID: true,
	idxSrcAddr:     true,
	idxDstAddr:     true,
	idxDstPort:     true,
	idxProtocol:    true,
	idxAction:      true,
	idxLogStatus:   true,
}

type variableData struct {
	bytes   int64
	packets int64
}

// Digest is responsible for compacting multiple VPC flow log lines into fewer, summarized lines.
type Digest struct {
	reader   *bufio.Reader
	digested map[string]variableData
	start    time.Time
	end      time.Time
}

// NewDigest creates and returns a new Digest of the given io.Reader contents
func NewDigest(r io.Reader) (*Digest, error) {
	d := Digest{
		reader:   bufio.NewReader(r),
		digested: make(map[string]variableData),
	}
	err := d.digest()
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *Digest) digest() error {
	for {
		line, err := d.reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		attrs := strings.Split(line, " ")
		logStatus := strings.ToLower(attrs[idxLogStatus])
		if attrs[idxVersion] != "2" || logStatus != "ok" {
			continue
		}
		dstPort, err := strconv.Atoi(attrs[idxDstPort])
		if err != nil {
			return err
		}
		srcPort, err := strconv.Atoi(attrs[idxSrcPort])
		if err != nil {
			return err
		}
		bytes, err := strconv.ParseInt(attrs[idxBytes], 10, 64)
		if err != nil {
			return err
		}
		packets, err := strconv.ParseInt(attrs[idxPackets], 10, 64)
		if err != nil {
			return err
		}

		// We don't care about the ephemeral port; we only care about the meaningful port.
		// Here the "meaningful" port is the port which carries some sort of conventional
		// meaning to it (e.g. 22, 80, 443, etc.).  We will use a less than heuristic to
		// extract this value, assuming that all "meaningful" ports are less than the
		// ephemeral port used.
		// Going forward, we will store this "meaningful" port in the dstPort field, even if
		// it's technically not the dstPort. We don't care if it's src/dst, we just want to know
		// which port data was communicated over.
		if srcPort < dstPort {
			attrs[idxDstPort] = fmt.Sprintf("%d", srcPort)
		}

		key := keyFromAttrs(attrs)
		if _, ok := d.digested[key]; !ok {
			d.digested[key] = variableData{}
		}
		vd := d.digested[key]
		vd.bytes = vd.bytes + bytes
		vd.packets = vd.packets + packets
		d.digested[key] = vd

		if err := d.updateTimeBounds(attrs); err != nil {
			return err
		}
	}
	return nil
}

// for a given digest, we want to find the earliest time and the latest time, and set those
// as our start and end respectively
func (d *Digest) updateTimeBounds(attrs []string) error {
	startString := attrs[idxStart]
	endString := attrs[idxEnd]
	startSec, err := strconv.ParseInt(startString, 10, 64)
	if err != nil {
		return err
	}
	endSec, err := strconv.ParseInt(endString, 10, 64)
	if err != nil {
		return err
	}
	start := time.Unix(startSec, 0)
	end := time.Unix(endSec, 0)

	if start.Before(d.start) {
		d.start = start
	}
	if end.After(d.end) {
		d.end = end
	}

	return nil
}

// key gets generated from stable values which are not likely to change as much
func keyFromAttrs(attrs []string) string {
	var key, prefix string
	for idx, attr := range attrs {
		val := attr
		if keyFields[idx] {
			val = "-"
		}
		key = key + prefix + val
		prefix = " "
	}
	return key
}

// DigestReader converts the digested VPC flow logs into a io.ReaderCloser
type DigestReader struct {
	Digest      Digest
	buff        bytes.Buffer
	initialized bool
}

func (r *DigestReader) init() error {
	for key, vd := range r.Digest.digested {
		attrs := strings.Split(key, " ")
		var line, prefix string
		for idx, attr := range attrs {
			var val string
			switch idx {
			case idxBytes:
				val = fmt.Sprintf("%d", vd.bytes)
			case idxPackets:
				val = fmt.Sprintf("%d", vd.packets)
			case idxStart:
				val = fmt.Sprintf("%d", r.Digest.start.Unix())
			case idxEnd:
				val = fmt.Sprintf("%d", r.Digest.end.Unix())
			default:
				val = attr
			}
			line = line + prefix + val
			prefix = " "
		}
		line = line + "\n"
		_, err := r.buff.WriteString(line)
		if err != nil {
			return err
		}
	}
	return nil
}

// Read reads from the digested VPC flow logs
func (r *DigestReader) Read(b []byte) (int, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return 0, err
		}
		r.initialized = true
	}

	return r.buff.Read(b)
}
