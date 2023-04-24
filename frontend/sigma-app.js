// Fetch the graph data from the external file
fetch('graph_data.txt')
    .then(response => response.text())
    .then(graphData => {
        const lines = graphData.split('\n').filter(line => line.trim() !== '');
        const nodes = [];
        const edges = [];

        lines.forEach((line, index) => {
            const [source, target, weight] = line.split(' ');
            if (!nodes.find(node => node.id === source)) {
                nodes.push({ id: source, label: source, x: Math.random(), y: Math.random(), size: 1, color: '#69b3a2' });
            }
            if (!nodes.find(node => node.id === target)) {
                nodes.push({ id: target, label: target, x: Math.random(), y: Math.random(), size: 1, color: '#69b3a2' });
            }
            edges.push({ id: `e${index}`, source, target, size: Math.log(weight) + 1, color: '#999', type: 'arrow' });
        });

        const sigmaInstance = new sigma({
            graph: { nodes, edges },
            container: 'container',
            settings: {
                edgeLabelSize: 'proportional',
                minArrowSize: 8
            }
        });

        // Configure ForceAtlas2 layout
        const forceAtlas2 = new sigma.layouts.forceAtlas2(sigmaInstance, {
            worker: true,
            barnesHutOptimize: true,
            strongGravityMode: true,
            gravity: 1,
            scalingRatio: 50,
            slowDown: 10
        });

        // Start the layout
        forceAtlas2.start();

        // Stop the layout after a certain time
        setTimeout(() => {
            forceAtlas2.stop();
        }, 5000);
    })
    .catch(error => console.error('Error fetching graph data:', error));
