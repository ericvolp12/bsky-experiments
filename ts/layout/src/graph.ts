import { Worker } from "worker_threads";
import { join } from "path";
import { MultiDirectedGraph } from "graphology";
import { ThreadItem } from "./models";

export async function assignGraphLayout(
  responseJSON: ThreadItem[]
): Promise<MultiDirectedGraph> {
  const workerPath = join(__dirname, "graph-worker.js");

  return new Promise((resolve, reject) => {
    const worker = new Worker(workerPath);

    worker.on("message", (graph: MultiDirectedGraph) => {
      resolve(graph);
    });

    worker.on("error", (error: Error) => {
      reject(error);
    });

    worker.on("exit", (code: number) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });

    worker.postMessage(responseJSON);
  });
}
