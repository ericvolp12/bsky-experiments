import { json } from "body-parser";
import express from "express";
import client from "prom-client";
import promMid from "express-prometheus-middleware";
import morgan from "morgan";

import { assignGraphLayout } from "./graph";
import { ThreadItem } from "./models";
import { MultiDirectedGraph } from "graphology";

const app = express();
const port = process.env.PORT || 8087;

app.use(json({ limit: "20mb" }));

app.use(
  promMid({
    metricsPath: "/metrics",
    collectDefaultMetrics: true,
    requestDurationBuckets: [
      0.001, 0.05, 0.5, 1, 3, 5, 10, 20, 40, 60, 80, 100,
    ],
    requestLengthBuckets: client.exponentialBuckets(100, 10, 10),
    responseLengthBuckets: client.exponentialBuckets(100, 10, 10),
  })
);

// Add morgan logging middleware
app.use(morgan("combined"));

app.post("/fa2_layout", async (req, res) => {
  const inputThreadItems: ThreadItem[] = req.body;

  if (!inputThreadItems) {
    res.status(400).json({ error: "Missing threadItems in request body" });
    return;
  }

  try {
    const serializedGraph = await assignGraphLayout(inputThreadItems);
    const graph = MultiDirectedGraph.from(serializedGraph);
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
