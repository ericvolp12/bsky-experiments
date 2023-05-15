package search

import (
	"context"
	"github.com/ericvolp12/bsky-experiments/pkg/search/search_queries"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"net/url"
)

func newNeo4jRegistry(connectionString string) (PostRegistry, error) {
	uri, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}
	// TODO: re-construct URI without user/password
	password, _ := uri.User.Password()
	driver, err := neo4j.NewDriverWithContext(connectionString, neo4j.BasicAuth(uri.User.Username(), password, ""))
	if err != nil {
		return nil, err
	}
	err = driver.VerifyConnectivity(context.Background())
	if err != nil {
		return nil, err
	}
	registry := &Neo4jRegistry{driver: driver}
	if err = registry.initializeDB(); err != nil {
		return nil, err
	}
	return registry, nil
}

type Neo4jRegistry struct {
	driver neo4j.DriverWithContext
}

// TODO: add context.Context to interface
func (n *Neo4jRegistry) initializeDB() error {
	ctx := context.Background()
	_, err := neo4j.ExecuteQuery(ctx, n.driver,
		"CREATE CONSTRAINT author_did IF NOT EXISTS FOR (a:Author) REQUIRE (a.did) IS UNIQUE", nil, neo4j.EagerResultTransformer)
	if err != nil {
		return err
	}
	_, err = neo4j.ExecuteQuery(ctx, n.driver,
		"CREATE CONSTRAINT post_id IF NOT EXISTS FOR (p:Post) REQUIRE (p.id) IS UNIQUE", nil, neo4j.EagerResultTransformer)
	return err
}

func (n *Neo4jRegistry) AddPost(ctx context.Context, post *Post) error {
	_, err := neo4j.ExecuteQuery(ctx, n.driver,
		`
MERGE (a:Author {did: $author_did}) 
MERGE (p:Post {id: $id}) ON CREATE SET p.text = $text
MERGE (a)-[:POSTED]->(p)
WITH * WHERE $parentPostId IS NOT NULL
MERGE (parent: Post {id: $parentPostId})
MERGE (p)-[:PARENT]->(parent)
`,
		map[string]any{
			"author_did":   post.AuthorDID,
			"id":           post.ID,
			"text":         post.Text,
			"parentPostId": post.ParentPostID,
		},
		neo4j.EagerResultTransformer)
	return err
}

func (n *Neo4jRegistry) AddAuthor(ctx context.Context, author *Author) error {
	_, err := neo4j.ExecuteQuery(ctx, n.driver,
		"MERGE (a:Author {did: $did}) ON CREATE SET a.handle = $handle",
		map[string]any{
			"did":    author.DID,
			"handle": author.Handle,
		},
		neo4j.EagerResultTransformer)
	return err
}

func (n *Neo4jRegistry) GetPost(ctx context.Context, postID string) (*Post, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) GetAuthorStats(ctx context.Context) (*AuthorStats, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) GetTopPosters(ctx context.Context, count int32) ([]search_queries.GetTopPostersRow, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) GetAuthor(ctx context.Context, did string) (*Author, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) GetAuthorsByHandle(ctx context.Context, handle string) ([]*Author, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) GetThreadView(ctx context.Context, postID, authorID string) ([]PostView, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) GetOldestPresentParent(ctx context.Context, postID string) (*Post, error) {
	//TODO implement me
	panic("implement me")
}

func (n *Neo4jRegistry) Close() error {
	return n.driver.Close(context.Background())
}
