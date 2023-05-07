import express from "express";
import { json } from "body-parser";
import { MultiDirectedGraph } from "graphology";

import forceAtlas2 from "graphology-layout-forceatlas2";
import circular from "graphology-layout/circular";

interface Edge {
  source: string;
  target: string;
}

interface SearchParams {
  author_did?: string;
  author_handle?: string;
  post?: string;
  selectdPost?: string;
  selectedAuthor?: string;
}

interface Post {
  id: string;
  text: string;
  parent_post_id: string | null;
  root_post_id: string | null;
  author_did: string;
  created_at: string;
  has_embedded_media: boolean;
}

interface ThreadItem {
  key: string;
  author_handle: string;
  post: Post;
  depth: number;
}

interface LayoutItem {
  key: string;
  author_handle: string;
  post: Post;
  depth: number;
  x: number;
  y: number;
}

export interface SelectedNode {
  id: string;
  text: string;
  created_at: string;
  author_handle: string;
  author_did: string;
  has_media: boolean;
  x: number;
  y: number;
}

function assignGraphLayout(responseJSON: ThreadItem[]): MultiDirectedGraph {
  const nodesMap: Map<string, ThreadItem> = new Map();
  const edges: Edge[] = [];

  responseJSON.forEach((post: ThreadItem) => {
    if (!nodesMap.has(post.post.id)) {
      nodesMap.set(post.post.id, {
        ...post,
        key: post.post.id,
      });
    }

    let source = post.post.parent_post_id;
    if (source != null && nodesMap.get(source) === undefined) {
      return;
    }

    edges.push({
      source: post.post.parent_post_id || "root",
      target: post.post.id,
    });
  });

  // Sort the nodes by did so that the order is consistent
  const nodes: ThreadItem[] = Array.from(nodesMap.values()).sort((a, b) => {
    if (a.post.id < b.post.id) {
      return -1;
    } else if (a.post.id > b.post.id) {
      return 1;
    } else {
      return 0;
    }
  });

  const graph = new MultiDirectedGraph();
  const totalEdges = edges.length;
  const totalNodes = nodes.length;

  const maxDepth = Math.max(...nodes.map((node) => node.depth));
  const maxSize = 6;
  const minSize = 1;

  for (let i = 0; i < totalNodes; i++) {
    const node = nodes[i];
    // Split node text on a newline every 30 characters
    let size =
      minSize + (maxSize - minSize) * ((maxDepth - node.depth + 1) / maxDepth);
    if (node.depth === 0) {
      size = maxSize + 3;
    }
    graph.addNode(node.post.id, {
      ...node,
      size,
      label: node.post.text.substring(0, 15) + "...",
    });
  }

  for (let i = 0; i < totalEdges; i++) {
    const edge = edges[i];
    if (edge.source === "root") {
      continue;
    }
    graph.addEdge(edge.source, edge.target);
  }

  circular.assign(graph);
  const settings = forceAtlas2.inferSettings(graph);
  const iterationCount = 600;
  console.log(`Running ${iterationCount} Force Atlas simulations...`);
  forceAtlas2.assign(graph, { settings, iterations: iterationCount });
  console.log("Done running Force Atlas");
  graph.setAttribute("lastUpdated", new Date().toISOString());

  return graph;
}

const app = express();
const port = process.env.PORT || 8087;

app.use(json({ limit: "20mb" }));

app.post("/fa2_layout", (req, res) => {
  const inputThreadItems: ThreadItem[] = req.body;

  if (!inputThreadItems) {
    res.status(400).json({ error: "Missing threadItems in request body" });
    return;
  }

  try {
    const graph = assignGraphLayout(inputThreadItems);
    const layoutItems = graph.nodes().map((nodeId) => {
      const node = graph.getNodeAttributes(nodeId);
      return {
        key: node.key,
        author_handle: node.author_handle,
        post: node.post,
        depth: node.depth,
        // Round the x and y values to 2 decimal places
        x: Math.round(node.x * 100) / 100,
        y: Math.round(node.y * 100) / 100,
      };
    });
    res.status(200).json(layoutItems);
  } catch (error) {
    console.log(`Error processing layout: ${error}`);
    res.status(500).json({ error: `Error processing layout: ${error}` });
  }
});

app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
