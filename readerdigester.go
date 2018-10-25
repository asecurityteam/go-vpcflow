package vpcflow

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

// Each log line is space delimited. When tokenized, the attributes can be accessed by the below
// index values
const (
	idxVersion = iota
	idxAccountID
	idxInterfaceID
	idxSrcAddr
	idxDstAddr
	idxSrcPort
	idxDstPort
	idxProtocol
	idxPackets
	idxBytes
	idxStart
	idxEnd
	idxAction
	idxLogStatus
)

// set of keyed fields
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

// Digester interface digests input data, and outputs an io.ReaderCloser
// from which the compacted data can be read
type Digester interface {
	Digest() (io.ReadCloser, error)
}

// ReaderDigester is responsible for compacting multiple VPC flow log lines into fewer, summarized lines.
type ReaderDigester struct {
	Reader io.ReadCloser
}

// Digest reads from the given io.Reader, and compacts multiple VPC flow log lines, producing a digest
// of the material made available via the resulting io.ReadCloser.  A digest is created by squashing
// "stable" values together, and aggregating more volatile values. Stable values would be the srcaddr,
// dstaddr, dstport, protocol, and action values. These are not as likely to change with great frequency
// as the more volatile values such as srcport, start, end, log-status, bytes, and packets. For the most
// part, these volatile values will change with every entry even when the stable values are exactly the
// same.
func (d *ReaderDigester) Digest() (io.ReadCloser, error) {
	defer d.Reader.Close()
	reader := bufio.NewReader(d.Reader)
	digest := make(map[string]variableData)
	var start, end time.Time
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF && len(line) < 1 {
			break
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		attrs := strings.Split(line, " ")
		logStatus := strings.ToLower(strings.TrimSpace(attrs[idxLogStatus]))
		if attrs[idxVersion] != "2" || logStatus != "ok" {
			continue
		}
		dstPort, err := strconv.Atoi(attrs[idxDstPort])
		if err != nil {
			return nil, err
		}
		srcPort, err := strconv.Atoi(attrs[idxSrcPort])
		if err != nil {
			return nil, err
		}
		bytes, err := strconv.ParseInt(attrs[idxBytes], 10, 64)
		if err != nil {
			return nil, err
		}
		packets, err := strconv.ParseInt(attrs[idxPackets], 10, 64)
		if err != nil {
			return nil, err
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
		if _, ok := digest[key]; !ok {
			digest[key] = variableData{}
		}
		vd := digest[key]
		vd.bytes = vd.bytes + bytes
		vd.packets = vd.packets + packets
		digest[key] = vd

		s, e, err := timeBoundsFromAttrs(attrs)
		if err != nil {
			return nil, err
		}
		if s.Before(start) || start.IsZero() {
			start = s
		}
		if e.After(end) || end.IsZero() {
			end = e
		}
	}
	return readerFromDigest(digest, start, end)
}

// for a given log line, extract the unix time stamp, and return start, end respectively
func timeBoundsFromAttrs(attrs []string) (time.Time, time.Time, error) {
	startString := attrs[idxStart]
	endString := attrs[idxEnd]
	startSec, err := strconv.ParseInt(startString, 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	endSec, err := strconv.ParseInt(endString, 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return time.Unix(startSec, 0), time.Unix(endSec, 0), nil
}

// key gets generated from stable values which are not likely to change as much
func keyFromAttrs(attrs []string) string {
	var key, prefix string
	for idx, attr := range attrs {
		val := strings.TrimSpace(attr)
		if !keyFields[idx] {
			val = "-"
		}
		key = key + prefix + val
		prefix = " "
	}
	return key
}

func readerFromDigest(digest map[string]variableData, start, end time.Time) (io.ReadCloser, error) {
	var buff bytes.Buffer
	for key, vd := range digest {
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
				val = fmt.Sprintf("%d", start.Unix())
			case idxEnd:
				val = fmt.Sprintf("%d", end.Unix())
			case idxSrcPort:
				val = "0" // always set src port to 0. see earlier comment
			default:
				val = attr
			}
			line = line + prefix + val
			prefix = " "
		}
		line = line + "\n"
		_, err := buff.WriteString(line)
		if err != nil {
			return nil, err
		}
	}
	return ioutil.NopCloser(&buff), nil
}
