package graph

import (
	"encoding/binary"
	"io"
	"os"
	"unicode/utf8"
)

// BinaryGraphReaderWriter is an implementation of the ReaderWriter interface for Graphs
// that reads and writes graph data to and from binary files.
type BinaryGraphReaderWriter struct{}

const maxNodeIDLength = 2147483647
const maxNodeHandleLength = 2147483647

// WriteGraph writes the graph data to a binary file with the given filename.
func (rw BinaryGraphReaderWriter) WriteGraph(g Graph, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	nodeCount := int32(g.GetNodeCount())
	edgeCount := int32(g.GetEdgeCount())
	if err := binary.Write(file, binary.LittleEndian, nodeCount); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, edgeCount); err != nil {
		return err
	}

	nodeIndex := make(map[NodeID]int32)
	var index int32
	for _, node := range g.Nodes {
		nodeIndex[node.DID] = index
		index++

		idLength := int32(utf8.RuneCountInString(string(node.DID)))
		handleLength := int32(utf8.RuneCountInString(node.Handle))
		if err := binary.Write(file, binary.LittleEndian, idLength); err != nil {
			return err
		}
		if _, err := file.WriteString(string(node.DID)); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, handleLength); err != nil {
			return err
		}
		if _, err := file.WriteString(node.Handle); err != nil {
			return err
		}
	}

	for from, edges := range g.Edges {
		for to, weight := range edges {
			if err := binary.Write(file, binary.LittleEndian, nodeIndex[from]); err != nil {
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, nodeIndex[to]); err != nil {
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, int32(weight)); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReadGraph reads the graph data from a binary file with the given filename.
func (rw BinaryGraphReaderWriter) ReadGraph(filename string) (Graph, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Graph{}, err
	}
	defer file.Close()

	var nodeCount, edgeCount int32
	if err := binary.Read(file, binary.LittleEndian, &nodeCount); err != nil {
		return Graph{}, err
	}
	if err := binary.Read(file, binary.LittleEndian, &edgeCount); err != nil {
		return Graph{}, err
	}

	g := NewGraph()
	nodes := make([]Node, nodeCount)

	for i := int32(0); i < nodeCount; i++ {
		var idLength, handleLength int32
		if err := binary.Read(file, binary.LittleEndian, &idLength); err != nil {
			return Graph{}, err
		}

		buf := make([]byte, idLength)
		if _, err := io.ReadFull(file, buf); err != nil {
			return Graph{}, err
		}

		if err := binary.Read(file, binary.LittleEndian, &handleLength); err != nil {
			return Graph{}, err
		}

		handleBuf := make([]byte, handleLength)
		if _, err := io.ReadFull(file, handleBuf); err != nil {
			return Graph{}, err
		}

		nodes[i] = Node{
			DID:    NodeID(string(buf)),
			Handle: string(handleBuf),
		}
		g.AddNode(nodes[i])
	}

	for i := int32(0); i < edgeCount; i++ {
		var fromIndex, toIndex, weight int32

		if err := binary.Read(file, binary.LittleEndian, &fromIndex); err != nil {
			return Graph{}, err
		}
		if err := binary.Read(file, binary.LittleEndian, &toIndex); err != nil {
			return Graph{}, err
		}
		if err := binary.Read(file, binary.LittleEndian, &weight); err != nil {
			return Graph{}, err
		}

		g.AddEdge(nodes[fromIndex], nodes[toIndex], int(weight))
	}

	return g, nil
}
