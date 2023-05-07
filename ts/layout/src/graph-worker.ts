import { parentPort } from "worker_threads";
import { MultiDirectedGraph } from "graphology";
import forceAtlas2 from "graphology-layout-forceatlas2";
import circular from "graphology-layout/circular";
import { Edge, ThreadItem } from "./models";

if (!parentPort) {
  throw new Error("Parent port not available in worker thread");
}

// Listen for messages from the main thread
parentPort.on("message", (responseJSON: ThreadItem[]) => {
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
  // Send the graph back to the main thread
  parentPort?.postMessage(graph.toJSON());
});
