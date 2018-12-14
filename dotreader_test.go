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
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="0" govpc_dstPort="80" govpc_protocol="6" govpc_packets="20" govpc_bytes="1000" govpc_start="1418530010" govpc_end="1818530070" color=red label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=0\ndstPort=80\nprotocol=6\npackets=20\nbytes=1000\nstart=1418530010\nend=1818530070"]`:   true,
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="0" govpc_dstPort="80" govpc_protocol="6" govpc_packets="40" govpc_bytes="2000" govpc_start="1418530010" govpc_end="1818530070" color=green label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=0\ndstPort=80\nprotocol=6\npackets=40\nbytes=2000\nstart=1418530010\nend=1818530070"]`: true,
				`n172311621 -> n1723116139 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="80" govpc_dstPort="0" govpc_protocol="6" govpc_packets="40" govpc_bytes="2000" govpc_start="1418530010" govpc_end="1818530070" color=green label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=80\ndstPort=0\nprotocol=6\npackets=40\nbytes=2000\nstart=1418530010\nend=1818530070"]`: true,
				`n1723116139 [label="172.31.16.139"]`: true,
				`n172311621 [label="172.31.16.21"]`:   true,
				`}`:                                   true,
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
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="20641" govpc_dstPort="80" govpc_protocol="6" govpc_packets="20" govpc_bytes="1000" govpc_start="1418530010" govpc_end="1418530070" color=green label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=20641\ndstPort=80\nprotocol=6\npackets=20\nbytes=1000\nstart=1418530010\nend=1418530070"]`: true,
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="20541" govpc_dstPort="80" govpc_protocol="6" govpc_packets="20" govpc_bytes="1000" govpc_start="1518530010" govpc_end="1518530070" color=green label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=20541\ndstPort=80\nprotocol=6\npackets=20\nbytes=1000\nstart=1518530010\nend=1518530070"]`: true,
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="20441" govpc_dstPort="80" govpc_protocol="6" govpc_packets="20" govpc_bytes="1000" govpc_start="1618530010" govpc_end="1618530070" color=green label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=20441\ndstPort=80\nprotocol=6\npackets=20\nbytes=1000\nstart=1618530010\nend=1618530070"]`: true,
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="20341" govpc_dstPort="80" govpc_protocol="6" govpc_packets="20" govpc_bytes="1000" govpc_start="1718530010" govpc_end="1718530070" color=red label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=20341\ndstPort=80\nprotocol=6\npackets=20\nbytes=1000\nstart=1718530010\nend=1718530070"]`:   true,
				`n1723116139 -> n172311621 [govpc_accountID="123456789010" govpc_eniID="eni-abc123de" govpc_srcPort="20241" govpc_dstPort="80" govpc_protocol="6" govpc_packets="20" govpc_bytes="1000" govpc_start="1818530010" govpc_end="1818530070" color=green label="accountID=123456789010\neniID=eni-abc123de\nsrcPort=20241\ndstPort=80\nprotocol=6\npackets=20\nbytes=1000\nstart=1818530010\nend=1818530070"]`: true,
				`n1723116139 [label="172.31.16.139"]`: true,
				`n172311621 [label="172.31.16.21"]`:   true,
				`}`:                                   true,
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
