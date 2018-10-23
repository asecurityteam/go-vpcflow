package vpcflow

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDigest(t *testing.T) {
	data := []byte(`2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20641 80 6 20 1000 1418530010 1418530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20541 80 6 20 1000 1518530010 1518530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20441 80 6 20 1000 1618530010 1618530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20341 80 6 20 1000 1718530010 1718530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20241 80 6 20 1000 1818530010 1818530070 ACCEPT OK`)
	input := ioutil.NopCloser(bytes.NewBuffer(data))
	rd := ReaderDigester{Reader: input}
	output, err := rd.Digest()
	assert.Nil(t, err)

	reader := bufio.NewReader(output)
	var numLines int
	expectedRejectLine := "2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 1000 1418530010 1818530070 REJECT OK"
	expectedOkLine := "2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 80 4000 1418530010 1818530070 ACCEPT OK"
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF && len(line) < 1 {
			break
		}
		numLines++
		line = strings.TrimSpace(line)
		if strings.Contains(line, "ACCEPT") {
			assert.Equal(t, expectedOkLine, line)
			continue
		}
		assert.Equal(t, expectedRejectLine, line)
	}
	assert.Equal(t, 2, numLines)
}
