package graph

import (
	"container/heap"
	"math"
)

// distanceInfo is a struct representing the distance value, hop count, and node ID.
type distanceInfo struct {
	value float64
	hops  int
	node  NodeID
}

// distanceQueue implements the container/heap interface for distanceInfo.
type distanceQueue []distanceInfo

func (pq distanceQueue) Len() int { return len(pq) }

func (pq distanceQueue) Less(i, j int) bool {
	return pq[i].value > pq[j].value
}

func (pq distanceQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *distanceQueue) Push(x interface{}) {
	item := x.(distanceInfo)
	*pq = append(*pq, item)
}

func (pq *distanceQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// FindSocialDistance finds the highest value "distance" between two nodes and returns the path with distance contributions.
func (g *Graph) FindSocialDistance(src, dest NodeID) (float64, []NodeID, []float64) {
	visited := make(map[NodeID]bool)
	distances := make(map[NodeID]float64)
	path := make(map[NodeID]NodeID)
	distanceContrib := make(map[NodeID]float64)
	distances[src] = 0

	pq := &distanceQueue{}
	heap.Init(pq)
	heap.Push(pq, distanceInfo{value: 0, hops: 0, node: src})

	for pq.Len() > 0 {
		curr := heap.Pop(pq).(distanceInfo)
		currNode := curr.node
		currHops := curr.hops

		if visited[currNode] {
			continue
		}

		visited[currNode] = true
		if currNode == dest {
			// Reconstruct the path and distance contributions
			reversePath := []NodeID{dest}
			reverseContributions := []float64{}
			for currNode != src {
				contrib := distanceContrib[currNode]
				reverseContributions = append(reverseContributions, contrib)
				currNode = path[currNode]
				reversePath = append(reversePath, currNode)
			}
			// Reverse the path and contributions to get them in the correct order
			path := make([]NodeID, len(reversePath))
			contributions := make([]float64, len(reverseContributions))
			for i, node := range reversePath {
				path[len(reversePath)-1-i] = node
			}
			for i, contrib := range reverseContributions {
				contributions[len(reverseContributions)-1-i] = contrib
			}
			return -1 * curr.value, path, contributions
		}

		if currHops >= 6 {
			continue
		}

		for neighbor, weight := range g.Edges[currNode] {
			combinedWeight := weight
			reverseWeight := 0
			if reverseEdgeWeight, ok := g.Edges[neighbor][currNode]; ok {
				combinedWeight += reverseEdgeWeight
				reverseWeight = reverseEdgeWeight
			}
			mutualityFactor := 1 - math.Abs(float64(weight-reverseWeight))/float64(combinedWeight)
			adjustedWeight := float64(combinedWeight) * mutualityFactor

			if adjustedWeight == 0 {
				continue
			}

			distance := adjustedWeight + float64((currHops*20)+1)*math.Log(adjustedWeight)
			newDistance := curr.value - distance

			if prevDist, ok := distances[neighbor]; !ok || newDistance > prevDist {
				distances[neighbor] = newDistance
				path[neighbor] = currNode
				distanceContrib[neighbor] = distance
				heap.Push(pq, distanceInfo{value: newDistance, hops: currHops + 1, node: neighbor})
			}
		}
	}

	return math.Inf(1), nil, nil
}
