package vpcflow

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"gonum.org/v1/gonum/graph/formats/dot/ast"
)

var edgeLabels = map[int]string{
	idxProtocol: "protocol",
	idxPackets:  "packets",
	idxBytes:    "bytes",
}

// Converter provides an interface for converting the input data into a different format, made available in the output io.ReadCloser
type Converter func(io.ReadCloser) (io.ReadCloser, error)

// DOTConverter takes in as input a sinle AWS VPC Flow Log file, or a digest of  VPC Flow Logs, and converts the data into a DOT graph.DOTConverter.
// The input ReadCloser will be closed after conversion, the caller should close the output ReadCloser when done reading.
func DOTConverter(r io.ReadCloser) (io.ReadCloser, error) {
	reader := bufio.NewReader(r)
	defer r.Close()
	g := &ast.Graph{Directed: true}
	nodeStmts := make(map[string]ast.Stmt) // dedupe node statements
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

		src := createNode(attrs[idxSrcAddr], attrs[idxSrcPort], nodeStmts)
		dst := createNode(attrs[idxDstAddr], attrs[idxDstPort], nodeStmts)
		e := &ast.Edge{Directed: true, Vertex: dst}
		var prefix, label string
		for idx, attr := range attrs {
			l, ok := edgeLabels[idx]
			if !ok {
				continue
			}
			label = label + prefix + l + "=" + attr
			prefix = "\\n"
		}
		color := "green"
		if strings.ToLower(attrs[idxAction]) == "reject" {
			color = "red"
		}
		g.Stmts = append(g.Stmts, &ast.EdgeStmt{
			From: src,
			To:   e,
			Attrs: []*ast.Attr{
				{
					Key: "label",
					Val: fmt.Sprintf(`"%s"`, label),
				},
				{
					Key: "color",
					Val: color,
				},
			},
		})
	}

	nodes := make([]ast.Stmt, 0, len(nodeStmts))
	for _, v := range nodeStmts {
		nodes = append(nodes, v)
	}
	g.Stmts = append(g.Stmts, nodes...)

	return ioutil.NopCloser(bytes.NewReader([]byte(g.String()))), nil
}

// createNode returns a node, and the corresponding node statement which describes the node
func createNode(addr, port string, nodeStmts map[string]ast.Stmt) *ast.Node {
	var p string
	if port != "0" { // check for normalized ephemeral port
		p = ":" + port
	}
	label := addr + p
	nID := "n" + strings.Replace(label, ".", "", -1)
	nID = strings.Replace(nID, ":", "", -1)
	n := &ast.Node{ID: nID}
	nodeStmts[n.ID] = &ast.NodeStmt{
		Node: n,
		Attrs: []*ast.Attr{
			{
				Key: "label",
				Val: fmt.Sprintf(`"%s"`, label),
			},
		},
	}
	return n
}
