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
	idxAccountID:   "accountID",
	idxInterfaceID: "eniID",
	idxSrcPort:     "srcPort",
	idxDstPort:     "dstPort",
	idxProtocol:    "protocol",
	idxPackets:     "packets",
	idxBytes:       "bytes",
	idxStart:       "start",
	idxEnd:         "end",
}

const namespace = "govpc_"

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

		src := createNode(attrs[idxSrcAddr], nodeStmts)
		dst := createNode(attrs[idxDstAddr], nodeStmts)

		// build up the edge label for rendering, and also add each of the annotations individually
		// so that they may be parsed easily by downstream consumers
		var prefix, label string
		edgeAttrs := make([]*ast.Attr, 0, len(attrs))
		for idx, attr := range attrs {
			l, ok := edgeLabels[idx]
			if !ok {
				continue
			}
			label = label + prefix + l + "=" + attr
			prefix = "\\n"

			edgeAttrs = append(edgeAttrs, &ast.Attr{
				Key: namespace + l,
				Val: fmt.Sprintf(`"%s"`, attr),
			})
		}
		color := &ast.Attr{
			Key: "color",
			Val: "green",
		}
		if strings.ToLower(attrs[idxAction]) == "reject" {
			color.Val = "red"
		}
		edgeAttrs = append(edgeAttrs, color, &ast.Attr{
			Key: "label",
			Val: fmt.Sprintf(`"%s"`, label),
		})
		g.Stmts = append(g.Stmts, &ast.EdgeStmt{
			From:  src,
			To:    &ast.Edge{Directed: true, Vertex: dst},
			Attrs: edgeAttrs,
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
func createNode(addr string, nodeStmts map[string]ast.Stmt) *ast.Node {
	nID := "n" + strings.Replace(addr, ".", "", -1)
	nID = strings.Replace(nID, ":", "", -1)
	n := &ast.Node{ID: nID}
	nodeStmts[n.ID] = &ast.NodeStmt{
		Node: n,
		Attrs: []*ast.Attr{
			{
				Key: "label",
				Val: fmt.Sprintf(`"%s"`, addr),
			},
		},
	}
	return n
}
