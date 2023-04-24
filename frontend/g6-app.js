// Fetch the graph data from the external file
fetch('graph_data.txt')
    .then(response => response.text())
    .then(graphData => {
        const lines = graphData.split('\n').filter(line => line.trim() !== '');
        const nodes = [];
        const edges = [];

        lines.forEach(line => {
            const [source, target, weight] = line.split(' ');
            if (!nodes.find(node => node.id === source)) {
                nodes.push({ id: source });
            }
            if (!nodes.find(node => node.id === target)) {
                nodes.push({ id: target });
            }
            edges.push({ source, target, weight: +weight });
        });

        const graph = new G6.Graph({
            container: 'container',
            width: window.innerWidth,
            height: window.innerHeight,
            modes: {
                default: ['drag-canvas', 'zoom-canvas', 'drag-node']
            },
            defaultNode: {
                size: 15,
                style: {
                    fill: '#69b3a2',
                    stroke: '#333'
                },
                labelCfg: {
                    style: {
                        fill: '#333',
                        fontSize: 12
                    },
                    position: 'bottom'
                }
            },
            defaultEdge: {
                style: {
                    stroke: '#999',
                    lineWidth: (edge) => Math.log(edge.weight) + 1,
                    endArrow: {
                        path: 'M 0,0 L 8,4 L 8,-4 Z',
                        fill: '#999'
                    }
                }
            },
            layout: {
                type: 'force',
                preventOverlap: true,
                linkDistance: 100
            },
            animate: true
        });

        graph.data({ nodes, edges });
        graph.render();
    })
    .catch(error => console.error('Error fetching graph data:', error));
