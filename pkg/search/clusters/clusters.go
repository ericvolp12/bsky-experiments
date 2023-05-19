package clusters

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type Cluster struct {
	ClusterID   string
	ClusterName string
}

type HandleClusterMapEntry struct {
	UserHandle string
	ClusterID  string
}

type DIDClusterMapEntry struct {
	UserDID   string
	ClusterID string
}

type ClusterManager struct {
	GraphJSONUrl     string
	Clusters         map[string]*Cluster
	HandleClusterMap map[string]*HandleClusterMapEntry
	DIDClusterMap    map[string]*DIDClusterMapEntry
}

type GraphData struct {
	Options    map[string]interface{} `json:"options"`
	Attributes struct {
		Clusters map[string]struct {
			Label string `json:"label"`
		} `json:"clusters"`
	} `json:"attributes"`
	Nodes []struct {
		Key        string `json:"key"`
		Attributes struct {
			Label     string `json:"label"`
			DID       string `json:"did"`
			Community int    `json:"community"`
		} `json:"attributes"`
	} `json:"nodes"`
}

func NewClusterManager(graphJSONUrl string) (*ClusterManager, error) {
	cm := &ClusterManager{
		GraphJSONUrl:     graphJSONUrl,
		Clusters:         make(map[string]*Cluster),
		HandleClusterMap: make(map[string]*HandleClusterMapEntry),
		DIDClusterMap:    make(map[string]*DIDClusterMapEntry),
	}

	log.Printf("getting graph data from %s", graphJSONUrl)

	resp, err := http.Get(graphJSONUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to get graph data from (%s): %w", graphJSONUrl, err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read graph data: %w", err)
	}

	var graphData GraphData
	err = json.Unmarshal(body, &graphData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal graph data: %w", err)
	}

	log.Printf("found %d clusters and %d users", len(graphData.Attributes.Clusters), len(graphData.Nodes))

	for id, cluster := range graphData.Attributes.Clusters {
		cm.Clusters[id] = &Cluster{
			ClusterID:   id,
			ClusterName: cluster.Label,
		}
	}

	for _, node := range graphData.Nodes {
		nodeCommunity := fmt.Sprintf("%d", node.Attributes.Community)
		if cluster, exists := cm.Clusters[nodeCommunity]; exists {
			cm.HandleClusterMap[node.Attributes.Label] = &HandleClusterMapEntry{
				UserHandle: node.Attributes.Label,
				ClusterID:  cluster.ClusterID,
			}
			cm.DIDClusterMap[node.Attributes.DID] = &DIDClusterMapEntry{
				UserDID:   node.Attributes.DID,
				ClusterID: cluster.ClusterID,
			}
		}
	}

	log.Printf("found %d users in clusters", len(cm.HandleClusterMap))

	return cm, nil
}

func (cm *ClusterManager) GetClusterForHandle(ctx context.Context, userHandle string) (*Cluster, error) {
	mapEntry, exists := cm.HandleClusterMap[userHandle]
	if !exists {
		return nil, nil
	}

	return cm.Clusters[mapEntry.ClusterID], nil
}

func (cm *ClusterManager) GetClusterForDID(ctx context.Context, userDID string) (*Cluster, error) {
	mapEntry, exists := cm.DIDClusterMap[userDID]
	if !exists {
		return nil, nil
	}

	return cm.Clusters[mapEntry.ClusterID], nil
}
