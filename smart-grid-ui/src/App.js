// App.js
// This is the main React component for the Smart Grid UI.
// It handles the WebSocket connection, state management, user interactions, and rendering of all UI elements.

// --- Imports ---
import React, { useState, useEffect, useMemo } from 'react';
import CytoscapeComponent from 'react-cytoscapejs';
import useWebSocket, { ReadyState } from 'react-use-websocket';
import './App.css';

function App() {
  // --- State Management ---
  const [elements, setElements] = useState({ nodes: [], edges: [] }); // Holds nodes and edges for the Cytoscape graph
  const [events, setEvents] = useState([]); // A log of all events from the backend
  const [toast, setToast] = useState(''); // For temporary warning pop-ups
  const [tieLineSource, setTieLineSource] = useState(''); // Source node for creating a new tie-line
  const [tieLineTarget, setTieLineTarget] = useState(''); // Target node for creating a new tie-line
  const [dominoStartNode, setDominoStartNode] = useState(''); // Node to start a domino effect
  const [dominoAddedLoad, setDominoAddedLoad] = useState(50); // Load to add for domino effect
  const [isSmartModeEnabled, setIsSmartModeEnabled] = useState(false); // Tracks if smart mode is on
  const [analytics, setAnalytics] = useState({ // Calculated metrics for the dashboard
    nodesOffline: 0,
    homesWithoutPower: 0,
    gridHealth: 100
  });
  const graphElements = useMemo(() => [...elements.nodes, ...elements.edges], [elements]);
  const topologySignature = useMemo(() => {
    const nodeIds = elements.nodes
      .map((n) => n.data.id)
      .filter(Boolean)
      .sort()
      .join('|');

    const edgeIds = elements.edges
      .map((e) => {
        const id = e.data.id || '';
        const source = e.data.source || '';
        const target = e.data.target || '';
        return `${id}:${source}->${target}`;
      })
      .sort()
      .join('|');

    return `${nodeIds}__${edgeIds}`;
  }, [elements.nodes, elements.edges]);

  // --- WebSocket Connection ---
  const socketUrl = 'ws://127.0.0.1:8000/ws/grid-state';
  const { sendMessage, lastMessage, readyState } = useWebSocket(socketUrl, {
    onOpen: () => {
      console.log('WebSocket connected, requesting initial grid state.');
      sendMessage('get_grid');
    },
    shouldReconnect: (closeEvent) => true, // Automatically reconnect
  });

  // --- Data Processing ---
  useEffect(() => {
    if (lastMessage !== null) {
      const outerData = JSON.parse(lastMessage.data);
      const data = typeof outerData === 'string' ? JSON.parse(outerData) : outerData;

      if (data.elements) {
        setElements({
          nodes: data.elements.nodes || [],
          edges: data.elements.edges || []
        });
      }

      if (data.events_reset) {
        setEvents([]);
      }
      
      if (data.events) {
        setEvents(prevEvents => [...data.events, ...prevEvents]);
        const warning = data.events.find(e => e.level === 'warning');
        if (warning) {
          setToast(warning.message);
          setTimeout(() => setToast(''), 5000);
        }
      }
    }
  }, [lastMessage]);

  // --- Initial Data Fetching ---
  useEffect(() => {
    // Fetch smart mode and an initial grid snapshot. WebSocket updates continue in real-time.
    const fetchSmartModeStatus = async () => {
      try {
        const response = await fetch('http://127.0.0.1:8000/api/smart-mode');
        const data = await response.json();
        setIsSmartModeEnabled(data.enabled);
      } catch (error) {
        console.error("Failed to fetch smart mode status:", error);
      }
    };

    const fetchInitialGridState = async () => {
      try {
        const response = await fetch('http://127.0.0.1:8000/api/grid-state');
        const data = await response.json();
        if (data.elements) {
          setElements({
            nodes: data.elements.nodes || [],
            edges: data.elements.edges || []
          });
        }
      } catch (error) {
        console.error('Failed to fetch initial grid state:', error);
      }
    };
    
    fetchSmartModeStatus();
    fetchInitialGridState();
  }, []); // Empty dependency array ensures this runs only once on mount

  // --- Control Panel Interaction Handlers ---
  // Resets the grid to its default state by calling the backend endpoint.
  const handleResetGrid = async () => {
    await fetch('http://127.0.0.1:8000/api/seed-grid', { method: 'POST' });
  };

  // Triggers a simulated high-load event on a specific transformer.
  const handleTriggerSpike = async () => {
    const payload = {
      data: [{ transformer_id: "TX-A", load_kw: 250.0 }]
    };
    await fetch('http://127.0.0.1:8000/api/telemetry', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
  };

  // Triggers a cascading failure with a specified additional load.
  const handleTriggerDomino = async () => {
    if (!dominoStartNode) {
      alert('Please select a start node for the domino effect.');
      return;
    }
    const payload = { start_node_id: dominoStartNode, added_load: parseFloat(dominoAddedLoad) };
    await fetch('http://127.0.0.1:8000/api/trigger-domino', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
  };

  // Toggles the "Smart Mode" for automatic load shedding.
  const handleToggleSmartMode = async () => {
    const newStatus = !isSmartModeEnabled;
    await fetch('http://127.0.0.1:8000/api/smart-mode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: newStatus })
    });
    setIsSmartModeEnabled(newStatus); // Update state after successful API call
  };

  // Creates a new tie-line between two selected nodes.
  const handleCreateTieLine = async () => {
    if (!tieLineSource || !tieLineTarget || tieLineSource === tieLineTarget) {
      alert('Please select two different nodes to create a tie-line.');
      return;
    }
    const payload = { source: tieLineSource, target: tieLineTarget };
    await fetch('http://127.0.0.1:8000/api/create-tie-line', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
  };

  // --- Analytics Calculation ---

  // useEffect hook to recalculate analytics whenever the grid elements change.
  useEffect(() => {
    const nodes = elements.nodes;
    const offlineNodes = nodes.filter(node => node.data.status === 'OFFLINE');
    const onlineNodes = nodes.filter(node => node.data.status === 'ONLINE');
    
    const nodesOfflineCount = offlineNodes.length;
    const homesWithoutPower = nodesOfflineCount * 50; // Assuming 50 homes per transformer
    const gridHealth = nodes.length > 0 ? (onlineNodes.length / nodes.length) * 100 : 100;

    setAnalytics({
      nodesOffline: nodesOfflineCount,
      homesWithoutPower,
      gridHealth: Math.round(gridHealth)
    });
  }, [elements]); // Dependency array ensures this runs when `elements` state changes

  // --- Cytoscape Graph Styling ---
  const stylesheet = [
    {
      selector: 'node',
      style: {
        'label': (ele) => ele.data('id') + '\n' + ele.data('load') + 'kW',
        'text-wrap': 'wrap',
        'color': '#ffffff',
        'text-valign': 'center',
        'font-size': '12px',
        'font-weight': 'bold',
        'width': 65,
        'height': 65,
        'background-color': (ele) => ele.data('status') === 'ONLINE' ? '#10b981' : '#ef4444',
        'border-width': 4,
        'border-color': '#1e293b'
      }
    },
    {
      selector: 'edge',
      style: {
        'width': 4,
        'line-color': '#64748b',
        'target-arrow-color': '#64748b',
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier'
      }
    }
  ];

  const connectionStatus = {
    [ReadyState.CONNECTING]: 'Connecting',
    [ReadyState.OPEN]: 'Open',
    [ReadyState.CLOSING]: 'Closing',
    [ReadyState.CLOSED]: 'Closed',
    [ReadyState.UNINSTANTIATED]: 'Uninstantiated',
  }[readyState];

  const graphLayout = {
    name: 'cose',
    fit: true,
    padding: 70,
    animate: false,
    idealEdgeLength: 140,
    nodeRepulsion: 5000
  };

  // --- Render Method ---
  return (
    <div className="app">
      {/* Toast notification for warnings */}
      {toast && <div className="toast">{toast}</div>}
      
      {/* Header Section */}
      <header className="app-header">
        <h1>⚡ Smart Infrastructure Digital Twin</h1>
        <p>Live Transformer Status & Cascading Failure Monitor</p>
        <p>WebSocket Status: {connectionStatus}</p>
      </header>
      
      {/* Control Panel for user interactions */}
      <div className="control-panel">
        <div className="control-group">
          <h4>Grid Actions</h4>
          <button onClick={handleResetGrid} className="btn btn-primary">
            🔄 Reset Grid
          </button>
          <button onClick={handleTriggerSpike} className="btn btn-danger">
            ⚡ Trigger EV Spike
          </button>
        </div>

        <div className="control-group">
          <h4>Smart Mode</h4>
          <label className="switch">
            <input type="checkbox" checked={isSmartModeEnabled} onChange={handleToggleSmartMode} />
            <span className="slider round"></span>
          </label>
          <span className="switch-label">{isSmartModeEnabled ? 'Auto-Shedding ON' : 'Auto-Shedding OFF'}</span>
        </div>

        <div className="control-group">
          <h4>Cascading Failure Test</h4>
          <select onChange={(e) => setDominoStartNode(e.target.value)} value={dominoStartNode}>
            <option value="">Select Start Node</option>
            {elements.nodes.map(el => <option key={el.data.id} value={el.data.id}>{el.data.id}</option>)}
          </select>
          <input 
            type="number" 
            value={dominoAddedLoad} 
            onChange={(e) => setDominoAddedLoad(e.target.value)} 
            placeholder="Added Load (kW)"
          />
          <button onClick={handleTriggerDomino} className="btn btn-warning">
            Trigger Domino
          </button>
        </div>

        <div className="control-group">
          <h4>Dynamic Tie-Line</h4>
          <select onChange={(e) => setTieLineSource(e.target.value)} value={tieLineSource}>
            <option value="">Select Source</option>
            {elements.nodes.map(el => <option key={el.data.id} value={el.data.id}>{el.data.id}</option>)}
          </select>
          <select onChange={(e) => setTieLineTarget(e.target.value)} value={tieLineTarget}>
            <option value="">Select Target</option>
            {elements.nodes.map(el => <option key={el.data.id} value={el.data.id}>{el.data.id}</option>)}
          </select>
          <button onClick={handleCreateTieLine} className="btn btn-success">
            Create Tie-Line
          </button>
        </div>
      </div>

      {/* Analytics Dashboard */}
      <div className="analytics-dashboard">
        <div className="metric">
          <h3>Total Nodes Offline</h3>
          <p className={analytics.nodesOffline > 0 ? 'text-danger' : 'text-success'}>{analytics.nodesOffline}</p>
        </div>
        <div className="metric">
          <h3>Estimated Homes without Power</h3>
          <p className={analytics.homesWithoutPower > 0 ? 'text-danger' : ''}>{analytics.homesWithoutPower}</p>
        </div>
        <div className="metric">
          <h3>Grid Health</h3>
          <p className={analytics.gridHealth < 100 ? 'text-warning' : 'text-success'}>{analytics.gridHealth}%</p>
        </div>
      </div>

      {/* Main Content Area (Graph and Event Log) */}
      <main className="main-content">
        <div className="graph-container">
          {elements.nodes.length > 0 ? (
            <CytoscapeComponent 
              key={topologySignature}
              elements={graphElements}
              style={{ width: '100%', height: '100%' }} 
              stylesheet={stylesheet}
              layout={graphLayout}
            />
          ) : (
            <div className="loading">
              <h3>Loading Grid Data...</h3>
            </div>
          )}
        </div>
        <div className="event-log">
          <h3>Event Log</h3>
          <div className="events-container">
            {events.map(event => (
              <div key={event.id} className={`event event-${event.level}`}>
                <p className="event-source">{event.source.toUpperCase()}</p>
                <p className="event-message">{event.message}</p>
                <p className="event-timestamp">{new Date(event.timestamp).toLocaleTimeString()}</p>
              </div>
            ))}
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;