package goraph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	yaml "gopkg.in/yaml.v2"
)

// ID is unique identifier.
type ID interface {
	// String returns the string ID.
	String() string
}

// StringID is a new type based on string
type StringID string

func (s StringID) String() string {
	return string(s)
}

// Node represents a vertex. The ID must be unique within the graph.
type Node interface {
	// ID returns the node's ID.
	ID() ID

	// String returns the string representation of the node which is the node ID.
	String() string

	// Props returns properties associated to a node
	Props() map[string]string
}

// Node is an internal type that implements the Node interface.
type node struct {
	id    string
	props map[string]string
}

func (n *node) ID() ID {
	return StringID(n.id)
}

func (n *node) String() string {
	return n.id
}

func (n *node) Props() map[string]string {
	return n.props
}

// NewNode creates a new Node type
func NewNode(id string, props map[string]string) Node {
	// TODO : Check if id is unique in the graph
	return &node{
		id:    id,
		props: props,
	}
}

var nodeCnt uint64

// Edge connects between two Nodes.
type Edge interface {
	Source() Node
	Target() Node
	Weight() float64
	String() string
}

// edge is an Edge type that represents a weighted connection from a
// source Node to a target Node.
type edge struct {
	src Node
	tgt Node
	wgt float64
}

func (e *edge) Source() Node {
	return e.src
}

func (e *edge) Target() Node {
	return e.tgt
}

func (e *edge) Weight() float64 {
	return e.wgt
}

func (e *edge) String() string {
	return fmt.Sprintf("%s -- %.3f -→ %s\n", e.src, e.wgt, e.tgt)
}

// NewEdge creates an Edge between a source Node and a target Node with a
// weight of 1.
func NewEdge(src, tgt Node, wgt float64) Edge {
	return &edge{
		src: src,
		tgt: tgt,
		wgt: wgt,
	}
}

// NewUnweightedEdge create a weighted Edge between a source Node and a
// target Node.
func NewUnweightedEdge(src, tgt Node) Edge {
	return &edge{
		src: src,
		tgt: tgt,
		wgt: float64(1),
	}
}

// EdgeSlice is a slice of Edge types
type EdgeSlice []Edge

func (e EdgeSlice) Len() int {
	return len(e)
}
func (e EdgeSlice) Less(i, j int) bool {
	return e[i].Weight() < e[j].Weight()
}
func (e EdgeSlice) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Graph describes the methods of graph operations.
// It assumes that the identifier of a Node is unique.
// And weight values is float64.
type Graph interface {
	// Init initializes a Graph.
	Init()

	// ID returns the node's ID.
	ID() ID

	// NodeCount returns the total number of nodes.
	NodeCount() int

	// Node finds the Node.
	Node(id ID) (Node, error)

	// Nodes returns a map from node ID to
	// empty struct value. Graph does not allow duplicate
	// node ID or name.
	Nodes() map[ID]Node

	// AddNode adds a node to a graph, and returns false
	// if the node already existed in the graph.
	AddNode(nd Node) bool

	// DeleteNode deletes a node from a graph.
	// It returns true if it got deleted.
	// And false if it didn't get deleted.
	DeleteNode(id ID) bool

	// AddEdge adds an edge from nd1 to nd2 with the weight.
	// It returns error if a node does not exist.
	AddEdge(id1, id2 ID, weight float64) error

	// ReplaceEdge replaces an edge from id1 to id2 with the weight.
	ReplaceEdge(id1, id2 ID, weight float64) error

	// DeleteEdge deletes an edge from id1 to id2.
	DeleteEdge(id1, id2 ID) error

	// EdgeWeight returns the weight from id1 to id2.
	EdgeWeight(id1, id2 ID) (float64, error)

	// ParentNodes returns the map of parent Nodes.
	// (Nodes that come towards the argument vertex.)
	ParentNodes(id ID) (map[ID]Node, error)

	// ChildNodes returns the map of child Nodes.
	// (Nodes that go out of the argument vertex.)
	ChildNodes(id ID) (map[ID]Node, error)

	// ExportToJSON serializes the graph into a JSON file and
	// saves to disk.
	ExportToJSON(path string) map[string]map[string]map[string]float64

	// String describes the Graph.
	String() string
}

// graph is an internal default graph type that
// implements all methods in Graph interface.
type graph struct {
	mu sync.RWMutex // guards the following

	// id is a unique graph identifier
	id string

	// nodes stores all nodes.
	nodes map[ID]Node

	// nodeParents maps a Node identifer to sources(parents)
	// with edge weights.
	nodeParents map[ID]map[ID]float64

	// nodeChildren maps a Node identifer to targets(children)
	// with edge weights.
	nodeChildren map[ID]map[ID]float64
}

func (g *graph) Init() {
	// (X) g = newGraph()
	// this only updates the pointer
	//
	//
	// (X) *g = *newGraph()
	// assignment copies lock value

	g.nodes = make(map[ID]Node)
	g.nodeParents = make(map[ID]map[ID]float64)
	g.nodeChildren = make(map[ID]map[ID]float64)
}

func (g *graph) NodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.nodes)
}

func (g *graph) ID() ID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return StringID(g.id)
}

func (g *graph) Node(id ID) (Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id) {
		return nil, fmt.Errorf("%s does not exist in the graph", id)
	}

	return g.nodes[id], nil
}

func (g *graph) Nodes() map[ID]Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.nodes
}

func (g *graph) unsafeExistID(id ID) bool {
	_, ok := g.nodes[id]
	return ok
}

func (g *graph) AddNode(nd Node) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.unsafeExistID(nd.ID()) {
		return false
	}

	id := nd.ID()
	g.nodes[id] = nd
	return true
}

func (g *graph) DeleteNode(id ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id) {
		return false
	}

	delete(g.nodes, id)

	delete(g.nodeChildren, id)
	for _, smap := range g.nodeChildren {
		delete(smap, id)
	}

	delete(g.nodeParents, id)
	for _, smap := range g.nodeParents {
		delete(smap, id)
	}

	return true
}

func (g *graph) AddEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph", id2)
	}

	if _, ok := g.nodeChildren[id1]; ok {
		if v, ok2 := g.nodeChildren[id1][id2]; ok2 {
			g.nodeChildren[id1][id2] = v + weight
		} else {
			g.nodeChildren[id1][id2] = weight
		}
	} else {
		tmap := make(map[ID]float64)
		tmap[id2] = weight
		g.nodeChildren[id1] = tmap
	}
	if _, ok := g.nodeParents[id2]; ok {
		if v, ok2 := g.nodeParents[id2][id1]; ok2 {
			g.nodeParents[id2][id1] = v + weight
		} else {
			g.nodeParents[id2][id1] = weight
		}
	} else {
		tmap := make(map[ID]float64)
		tmap[id1] = weight
		g.nodeParents[id2] = tmap
	}

	return nil
}

func (g *graph) ReplaceEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph", id2)
	}

	if _, ok := g.nodeChildren[id1]; ok {
		g.nodeChildren[id1][id2] = weight
	} else {
		tmap := make(map[ID]float64)
		tmap[id2] = weight
		g.nodeChildren[id1] = tmap
	}
	if _, ok := g.nodeParents[id2]; ok {
		g.nodeParents[id2][id1] = weight
	} else {
		tmap := make(map[ID]float64)
		tmap[id1] = weight
		g.nodeParents[id2] = tmap
	}
	return nil
}

func (g *graph) DeleteEdge(id1, id2 ID) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph", id2)
	}

	if _, ok := g.nodeChildren[id1]; ok {
		if _, ok := g.nodeChildren[id1][id2]; ok {
			delete(g.nodeChildren[id1], id2)
		}
	}
	if _, ok := g.nodeParents[id2]; ok {
		if _, ok := g.nodeParents[id2][id1]; ok {
			delete(g.nodeParents[id2], id1)
		}
	}
	return nil
}

func (g *graph) EdgeWeight(id1, id2 ID) (float64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id1) {
		return 0, fmt.Errorf("%s does not exist in the graph", id1)
	}
	if !g.unsafeExistID(id2) {
		return 0, fmt.Errorf("%s does not exist in the graph", id2)
	}

	if _, ok := g.nodeChildren[id1]; ok {
		if v, ok := g.nodeChildren[id1][id2]; ok {
			return v, nil
		}
	}
	return 0.0, fmt.Errorf("there is no edge from %s to %s", id1, id2)
}

func (g *graph) ParentNodes(id ID) (map[ID]Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id) {
		return nil, fmt.Errorf("%s does not exist in the graph", id)
	}

	rs := make(map[ID]Node)
	if _, ok := g.nodeParents[id]; ok {
		for n := range g.nodeParents[id] {
			rs[n] = g.nodes[n]
		}
	}
	return rs, nil
}

func (g *graph) ChildNodes(id ID) (map[ID]Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id) {
		return nil, fmt.Errorf("%s does not exist in the graph", id)
	}

	rs := make(map[ID]Node)
	if _, ok := g.nodeChildren[id]; ok {
		for n := range g.nodeChildren[id] {
			rs[n] = g.nodes[n]
		}
	}
	return rs, nil
}

func (g *graph) ExportToJSON(path string) map[string]map[string]map[string]float64 {
	panic("Not implemented")
}

func (g *graph) String() string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	buf := new(bytes.Buffer)
	for id1, nd1 := range g.nodes {
		nmap, _ := g.ChildNodes(id1)
		for id2, nd2 := range nmap {
			weight, _ := g.EdgeWeight(id1, id2)
			fmt.Fprintf(buf, "%s -- %.3f -→ %s\n", nd1, weight, nd2)
		}
	}
	return buf.String()
}

// newGraph returns a new graph.
func newGraph() *graph {
	return &graph{
		nodes:        make(map[ID]Node),
		nodeParents:  make(map[ID]map[ID]float64),
		nodeChildren: make(map[ID]map[ID]float64),
		//
		// without this
		// panic: assignment to entry in nil map
	}
}

// NewGraph returns a new graph.
func NewGraph() Graph {
	return newGraph()
}

// NewGraphFromJSON returns a new Graph from a JSON file.
// Here's the sample JSON data:
//
//	{
//	    "graph_00": {
//	        "S": {
//	            "A": 100,
//	            "B": 14,
//	            "C": 200
//	        },
//	        "A": {
//	            "S": 15,
//	            "B": 5,
//	            "D": 20,
//	            "T": 44
//	        },
//	        "B": {
//	            "S": 14,
//	            "A": 5,
//	            "D": 30,
//	            "E": 18
//	        },
//	        "C": {
//	            "S": 9,
//	            "E": 24
//	        },
//	        "D": {
//	            "A": 20,
//	            "B": 30,
//	            "E": 2,
//	            "F": 11,
//	            "T": 16
//	        },
//	        "E": {
//	            "B": 18,
//	            "C": 24,
//	            "D": 2,
//	            "F": 6,
//	            "T": 19
//	        },
//	        "F": {
//	            "D": 11,
//	            "E": 6,
//	            "T": 6
//	        },
//	        "T": {
//	            "A": 44,
//	            "D": 16,
//	            "F": 6,
//	            "E": 19
//	        }
//	    },
//	}
//
func NewGraphFromJSON(rd io.Reader, graphID string) (Graph, error) {
	js := make(map[string]map[string]map[string]float64)
	dec := json.NewDecoder(rd)
	for {
		if err := dec.Decode(&js); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
	}
	if _, ok := js[graphID]; !ok {
		return nil, fmt.Errorf("%s does not exist", graphID)
	}
	gmap := js[graphID]

	g := newGraph()
	for id1, mm := range gmap {
		nd1, err := g.Node(StringID(id1))
		if err != nil {
			nd1 = NewNode(id1, make(map[string]string))
			g.AddNode(nd1)
		}
		for id2, weight := range mm {
			nd2, err := g.Node(StringID(id2))
			if err != nil {
				nd2 = NewNode(id2, make(map[string]string))
				g.AddNode(nd2)
			}
			g.ReplaceEdge(nd1.ID(), nd2.ID(), weight)
		}
	}

	return g, nil
}

// NewGraphFromYAML returns a new Graph from a YAML file.
// Here's the sample YAML data:
//
// graph_00:
//   S:
//     A: 100
//     B: 14
//     C: 200
//   A:
//     S: 15
//     B: 5
//     D: 20
//     T: 44
//   B:
//     S: 14
//     A: 5
//     D: 30
//     E: 18
//   C:
//     S: 9
//     E: 24
//   D:
//     A: 20
//     B: 30
//     E: 2
//     F: 11
//     T: 16
//   E:
//     B: 18
//     C: 24
//     D: 2
//     F: 6
//     T: 19
//   F:
//     D: 11
//     E: 6
//     T: 6
//   T:
//     A: 44
//     D: 16
//     F: 6
//     E: 19
//
func NewGraphFromYAML(rd io.Reader, graphID string) (Graph, error) {
	js := make(map[string]map[string]map[string]float64)
	var data []byte
	d := make([]byte, 1024)
	for {
		n, err := rd.Read(d)
		if err == io.EOF {
			break
		}

		data = append(data, d[0:n]...)
	}

	err := yaml.Unmarshal(data, &js)
	for {
		if err != nil {
			return nil, err
		} else {
			break
		}
	}
	if _, ok := js[graphID]; !ok {
		return nil, fmt.Errorf("%s does not exist", graphID)
	}
	gmap := js[graphID]

	g := newGraph()
	for id1, mm := range gmap {
		nd1, err := g.Node(StringID(id1))
		if err != nil {
			nd1 = NewNode(id1, make(map[string]string))
			g.AddNode(nd1)
		}
		for id2, weight := range mm {
			nd2, err := g.Node(StringID(id2))
			if err != nil {
				nd2 = NewNode(id2, make(map[string]string))
				g.AddNode(nd2)
			}
			g.ReplaceEdge(nd1.ID(), nd2.ID(), weight)
		}
	}

	return g, nil
}
