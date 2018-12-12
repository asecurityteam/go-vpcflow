package vpcflow

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvert(t *testing.T) {
	tc := []struct {
		Name     string
		Input    []byte
		Expected expectedDigest
	}{
		{
			Name: "digested_input",
			Input: []byte(`2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 20 1000 1418530010 1818530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 0 80 6 40 2000 1418530010 1818530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.21 172.31.16.139 80 0 6 40 2000 1418530010 1818530070 ACCEPT OK`),
			Expected: expectedDigest{
				`digraph {`: true,
				`n1723116139 -> n17231162180 [label="protocol=6\npackets=20\nbytes=1000" color=red]`:   true,
				`n1723116139 -> n17231162180 [label="protocol=6\npackets=40\nbytes=2000" color=green]`: true,
				`n17231162180 -> n1723116139 [label="protocol=6\npackets=40\nbytes=2000" color=green]`: true,
				`n1723116139 [label="172.31.16.139"]`:                                                  true,
				`n17231162180 [label="172.31.16.21:80"]`:                                               true,
				`}`:                                                                                    true,
			},
		},
		{
			Name: "undigested_input",
			Input: []byte(`version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20641 80 6 20 1000 1418530010 1418530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20541 80 6 20 1000 1518530010 1518530070 ACCEPT OK
2 123456789010 eni-1a2b3c4d - - - - - - - 1431280876 1431280934 - NODATA
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20441 80 6 20 1000 1618530010 1618530070 ACCEPT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20341 80 6 20 1000 1718530010 1718530070 REJECT OK
2 123456789010 eni-abc123de 172.31.16.139 172.31.16.21 20241 80 6 20 1000 1818530010 1818530070 ACCEPT OK`),
			Expected: expectedDigest{
				`digraph {`: true,
				`n172311613920641 -> n17231162180 [label="protocol=6\npackets=20\nbytes=1000" color=green]`: true,
				`n172311613920541 -> n17231162180 [label="protocol=6\npackets=20\nbytes=1000" color=green]`: true,
				`n172311613920441 -> n17231162180 [label="protocol=6\npackets=20\nbytes=1000" color=green]`: true,
				`n172311613920341 -> n17231162180 [label="protocol=6\npackets=20\nbytes=1000" color=red]`:   true,
				`n172311613920241 -> n17231162180 [label="protocol=6\npackets=20\nbytes=1000" color=green]`: true,
				`n172311613920441 [label="172.31.16.139:20441"]`:                                            true,
				`n172311613920341 [label="172.31.16.139:20341"]`:                                            true,
				`n172311613920241 [label="172.31.16.139:20241"]`:                                            true,
				`n172311613920641 [label="172.31.16.139:20641"]`:                                            true,
				`n17231162180 [label="172.31.16.21:80"]`:                                                    true,
				`n172311613920541 [label="172.31.16.139:20541"]`:                                            true,
				`}`: true,
			},
		},
		{
			Name:  "no_data",
			Input: []byte(``),
			Expected: expectedDigest{
				`digraph {`: true,
				`}`:         true,
			},
		},
	}
	for _, tt := range tc {
		t.Run(tt.Name, func(t *testing.T) {
			input := ioutil.NopCloser(bytes.NewReader(tt.Input))
			output, err := DOTConverter(input)
			assert.Nil(t, err)
			defer output.Close()
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

type trapReader struct{}

func (tr *trapReader) Read(_ []byte) (int, error) {
	return 0, errors.New("oops")
}

func TestReaderError(t *testing.T) {
	_, err := DOTConverter(ioutil.NopCloser(&trapReader{}))
	assert.NotNil(t, err)
}
