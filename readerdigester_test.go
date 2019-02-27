package vpcflow

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type expectedDigest map[string]bool

func TestDigestSuccess(t *testing.T) {
	tc := []struct {
		Name     string
		Input    []byte
		Expected expectedDigest
	}{
		{
			Name: "happy-path",
			Input: []byte(`version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20641 80 6 20 1000 1418530010 1418530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20541 80 6 20 1000 1518530010 1518530070 ACCEPT OK
2 123456789010 eni-1a2b3c4d - - - - - - - 1431280876 1431280934 - NODATA
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20441 80 6 20 1000 1618530010 1618530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20341 80 6 20 1000 1718530010 1718530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20241 80 6 20 1000 1818530010 1818530070 ACCEPT OK`),
			Expected: expectedDigest{
				"2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 1000 1418530010 1818530070 REJECT OK": true,
				"2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 80 4000 1418530010 1818530070 ACCEPT OK": true,
			},
		},
		{
			Name: "bidirectional-case",
			Input: []byte(`version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20641 80 6 20 1000 1418530010 1418530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.21 172.31.16.139 80 20641 6 20 1000 1518530010 1518530070 ACCEPT OK
2 123456789010 eni-1a2b3c4d - - - - - - - 1431280876 1431280934 - NODATA
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20441 80 6 20 1000 1618530010 1618530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20341 80 6 20 1000 1718530010 1718530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.21 172.31.16.139 80 20341 6 20 1000 1818530010 1818530070 ACCEPT OK`),
			Expected: expectedDigest{
				"2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 1000 1418530010 1818530070 REJECT OK": true,
				"2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 40 2000 1418530010 1818530070 ACCEPT OK": true,
				"2 123456789010 eni-abc123de 172.31.16.21 172.31.16.139 80 0 6 40 2000 1418530010 1818530070 ACCEPT OK": true,
			},
		},
	}

	for _, tt := range tc {
		t.Run(tt.Name, func(t *testing.T) {
			input := ioutil.NopCloser(bytes.NewBuffer(tt.Input))
			rd := ReaderDigester{Reader: input}
			output, err := rd.Digest()
			assert.Nil(t, err)

			reader := bufio.NewReader(output)
			var numLines int
			for {
				line, err := reader.ReadString('\n')
				if err == io.EOF && len(line) < 1 {
					break
				}
				numLines++
				line = strings.TrimSpace(line)
				_, found := tt.Expected[line]
				assert.True(t, found, fmt.Sprintf("Did not expect line: %s", line))
				tt.Expected[line] = false // we should only encouter each line in the digest once
			}
			assert.Equal(t, len(tt.Expected), numLines)
		})
	}
}

func TestDigestBadData(t *testing.T) {
	tc := []struct {
		Name  string
		Input []byte
	}{
		{
			Name:  "bad-src-port",
			Input: []byte("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 NaN 80 6 20 1000 1418530010 1418530070 ACCEPT OK"),
		},
		{
			Name:  "bad-dst-port",
			Input: []byte("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 80 NaN 6 20 1000 1418530010 1418530070 ACCEPT OK"),
		},
		{
			Name:  "bad-bytes",
			Input: []byte("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 65543 80 6 20 NaN 1418530010 1418530070 ACCEPT OK"),
		},
		{
			Name:  "bad-packets",
			Input: []byte("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 65543 80 6 NaN 1000 1418530010 1418530070 ACCEPT OK"),
		},
		{
			Name:  "bad-start",
			Input: []byte("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 65543 80 6 20 1000 - 1418530070 ACCEPT OK"),
		},
		{
			Name:  "bad-end",
			Input: []byte("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 65543 80 6 20 1000 1418530010 - ACCEPT OK"),
		},
	}

	for _, tt := range tc {
		t.Run(tt.Name, func(t *testing.T) {
			input := ioutil.NopCloser(bytes.NewBuffer(tt.Input))
			rd := ReaderDigester{Reader: input}
			_, err := rd.Digest()
			assert.NotNil(t, err)
		})
	}
}

func TestTimeBoundsFromAttrs(t *testing.T) {
	tc := []struct {
		Name          string
		Start         string
		End           string
		ExpectedError bool
	}{
		{
			Name:  "success",
			Start: "1418530010",
			End:   "1418530080",
		},
		{
			Name:          "bad-start",
			Start:         "not valid unix ts",
			End:           "1418530080",
			ExpectedError: true,
		},
		{
			Name:          "bad-end",
			Start:         "1418530010",
			End:           "no valid unix ts",
			ExpectedError: true,
		},
	}

	for _, tt := range tc {
		t.Run(tt.Name, func(t *testing.T) {
			fakeAttrs := make([]string, 14)
			fakeAttrs[idxStart] = tt.Start
			fakeAttrs[idxEnd] = tt.End
			start, end, err := timeBoundsFromAttrs(fakeAttrs)
			if tt.ExpectedError {
				assert.NotNil(t, err)
			} else {
				assert.Equal(t, tt.Start, fmt.Sprintf("%d", start.Unix()))
				assert.Equal(t, tt.End, fmt.Sprintf("%d", end.Unix()))
			}
		})
	}
}

func TestKeyFromAttrs(t *testing.T) {
	logLine := "2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 1000 1418530010 1418530070 ACCEPT OK"
	expectedKey := "2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 - - - - ACCEPT OK"

	assert.Equal(t, expectedKey, keyFromAttrs(strings.Split(logLine, " ")))
}

func TestReaderFromDigest(t *testing.T) {
	digest := map[string]variableData{
		"2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 - - - - ACCEPT OK": {bytes: 100, packets: 20},
	}
	start := time.Now().Add(-10 * time.Second)
	end := time.Now()
	expectedDigestLine := fmt.Sprintf("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 100 %d %d ACCEPT OK\n", start.Unix(), end.Unix())

	r, _ := readerFromDigest(digest, start, end)
	line, _ := bufio.NewReader(r).ReadString('\n')
	assert.Equal(t, expectedDigestLine, line)
}

var benchKeyFromAttrs string

func BenchmarkKeyFromAttrs(b *testing.B) {
	attrs := strings.Split("2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 NaN 80 6 20 1000 1418530010 1418530070 ACCEPT OK", "")
	var key string
	b.ResetTimer()
	for n := 0; n < b.N; n = n + 1 {
		key = keyFromAttrs(attrs)
	}
	benchKeyFromAttrs = key
}
