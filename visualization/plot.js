data = fetch("http://localhost:8000/logs/")
    .then(response => response.json())
    .then(json => {
        colors = [
            '#1f77b4',  // muted blue
            '#ff7f0e',  // safety orange
            '#2ca02c',  // cooked asparagus green
            '#d62728',  // brick red
            '#9467bd',  // muted purple
            '#8c564b',  // chestnut brown
            '#e377c2',  // raspberry yogurt pink
            '#7f7f7f',  // middle gray
            '#bcbd22',  // curry yellow-green
            '#17becf'   // blue-teal
        ]
        
        client_colors = new Map()

        messages = {};

        json.forEach(entry => {
            if (messages[entry.message_id] === undefined) {
                messages[entry.message_id] = [entry];
            } else {
                messages[entry.message_id].push(entry);
            }
        })

        data = []

        for (var message_id in messages) {
            let sender = messages[message_id][0]
            let receiver = messages[message_id][1]

            data.push({
                source: sender.sender,
                destination: receiver.receiver,
                send_timestamp: sender.timestamp,
                recv_timestamp: receiver.timestamp,
                action: [sender.action, receiver.action]
            })
        }

        // Extract unique sources and destinations
        let nodes = new Set();
        let clients = new Set();
        data.forEach(entry => {
            if (entry.source.startsWith('node')) {
                nodes.add(entry.source);
            } else {
                clients.add(entry.source);
            }
            if (entry.destination.startsWith('node')) {
                nodes.add(entry.destination);
            } else {
                clients.add(entry.destination);
            }
        });

        // Positions for plotting
        let positions = {};
        Array.from(nodes).concat(Array.from(clients)).forEach((item, index) => {
            positions[item] = index + 1;
        });

        // Determine the min and max timestamps
        let times = data.flatMap(entry => [new Date(entry.send_timestamp), new Date(entry.recv_timestamp)]);
        let minTime = new Date(Math.min(...times));
        let maxTime = new Date(Math.max(...times));
        // let timeExtension = (maxTime - minTime) * 0.05; // 15% of the time span

        // minTime = new Date(minTime.getTime() - timeExtension);
        // maxTime = new Date(maxTime.getTime() + timeExtension);

        // Prepare traces for the plot
        let traces = [];

        data.forEach((entry, _) => {
            
            let startTime = new Date(entry.send_timestamp);
            let endTime = new Date(entry.recv_timestamp);
            let startY = positions[entry.source];
            let endY = positions[entry.destination];

            let client = entry.source
            let color = client_colors.get(client)

            if (color === undefined) {
                idx = client_colors.size
                color = colors[idx % colors.length]
                client_colors.set(client, color)
            }

            var line
            if (entry.source.startsWith("node") && entry.destination.startsWith("node")) {
                line = { width: 1, color: color, dash: 'dash' }
            } else {
                line = { width: 2, color: color }
            }

            traces.push({
                x: [new Date(startTime.getTime() - minTime), new Date(endTime.getTime() - minTime)], // Start and end time for the arrow
                y: [startY, endY], // Source and destination positions
                mode: 'lines+markers',
                type: 'scattergl',  // Use scattergl for better performance and line hover
                marker: { size: 4, symbol: 'arrow-wide' },
                line: line, // Changed to make the line dashed
                hoverinfo: 'text', // Shows custom hover text
                hovertext: entry.action, // Custom text for hover
                hoveron: 'points+fills' // Allows hover effects on points and line fills
            });            
        })

        let duration = new Date(maxTime - minTime);
        // Layout configuration
        let layout = {
            title: 'Event Timing Diagram',
            xaxis: {
                title: 'Time',
                tickformat: '%S.%f',
                range: [0, duration]
            },
            yaxis: {
                title: 'Nodes',
                tickvals: Object.values(positions),
                ticktext: Object.keys(positions),
            },
            showlegend: false,
            hovermode: 'closest', // Only the closest data point's hover info is displayed
        };

        // Render the plot
        Plotly.newPlot('plotDiv', traces, layout, {responsive: true});

    }
)
