// Fetch the graph data from the external file
fetch('graph_data.txt')
    .then(response => response.text())
    .then(graphData => {
        const lines = graphData.split('\n').filter(line => line.trim() !== '');
        const elements = lines.flatMap(line => {
            const [source, target, weight] = line.split(' ');
            return [
                { data: { id: source } },
                { data: { id: target } },
                { data: { id: `${source}-${target}`, source, target, weight: +weight } }
            ];
        });

        const cy = cytoscape({
            container: document.getElementById('cy'),
            elements: elements,
            style: [
                {
                    selector: 'node',
                    style: {
                        'background-color': '#69b3a2',
                        'label': 'data(id)'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 'mapData(weight, 0, 10, 1, 5)',
                        'line-color': '#999',
                        'target-arrow-color': '#999',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier'
                    }
                }
            ],
            layout: {
                name: 'cose',
                idealEdgeLength: 100,
                nodeOverlap: 20,
                refresh: 20,
                fit: true,
                padding: 30,
                randomize: true,
                componentSpacing: 100,
                nodeRepulsion: 400000,
                edgeElasticity: 100,
                nestingFactor: 5,
                gravity: 80,
                numIter: 1000,
                initialTemp: 200,
                coolingFactor: 0.95,
                minTemp: 1.0
            }
        });
    })
    .catch(error => console.error('Error fetching graph data:', error));
