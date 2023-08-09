package consumer

import (
	"fmt"
	"strings"
)

type URI struct {
	Did        string
	RKey       string
	Collection string
}

// URI: at://{did}/{namespace}/{rkey}
func GetURI(uri string) (*URI, error) {
	trimmed := strings.TrimPrefix(uri, "at://")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid uri: %s", uri)
	}
	return &URI{
		Did:        parts[0],
		Collection: parts[1],
		RKey:       parts[2],
	}, nil
}
