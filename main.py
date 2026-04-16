# main.py
# Main FastAPI application for the Smart Grid Digital Twin.
# This file defines the API endpoints, WebSocket communication, and database interactions.

# --- Imports ---
import os
import asyncio
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo4j import AsyncGraphDatabase, exceptions
from pydantic import BaseModel
import json
from datetime import datetime, timezone

# --- Initial Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO)

# --- FastAPI App Initialization ---
app = FastAPI()

# CORS (Cross-Origin Resource Sharing) configuration to allow the React frontend to connect.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# --- Neo4j Database Connection ---
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
driver = None  # Global driver instance

# --- Pydantic Models for API requests ---
class TelemetryData(BaseModel):
    transformer_id: str
    load_kw: float

class TelemetryPayload(BaseModel):
    data: list[TelemetryData]

class TieLineRequest(BaseModel):
    source: str
    target: str

class SmartModeRequest(BaseModel):
    enabled: bool

class DominoRequest(BaseModel):
    start_node_id: str
    added_load: float

# --- Global State ---
smart_mode_enabled = False
simulation_generation = 0
domino_lock = asyncio.Lock()

# --- WebSocket Connection Manager ---
class ConnectionManager:
    """Manages active WebSocket connections."""
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- Database and Application Lifecycle Events ---
@app.on_event("startup")
async def startup_event():
    """Connect to Neo4j on application startup."""
    global driver
    try:
        driver = AsyncGraphDatabase.driver(URI, auth=AUTH)
        await driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
    except exceptions.ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j: {e}")
        # In a real application, you might want to exit or have a retry mechanism.
        driver = None

@app.on_event("shutdown")
async def shutdown_event():
    """Close the Neo4j connection on application shutdown."""
    if driver:
        await driver.close()
        logging.info("Neo4j connection closed.")

# --- Helper Functions ---
async def get_grid_state_from_db():
    """
    Queries the database for the current state of all nodes and edges
    and formats it for the frontend.
    """
    if not driver:
        return {"elements": {"nodes": [], "edges": []}}

    async with driver.session() as session:
        result = await session.run("""
            MATCH (n)
            OPTIONAL MATCH (n)-[r]->(m)
            RETURN n, r, m
        """)
        
        nodes = []
        edges = []
        node_ids = set()

        async for record in result:
            if record["n"] and record["n"].element_id not in node_ids:
                node_data = dict(record["n"])
                nodes.append({
                    "data": {
                        "id": node_data.get("id"),
                        "label": node_data.get("id"),
                        "status": node_data.get("status", "UNKNOWN"),
                        "load": node_data.get("load_kw", 0),
                        **node_data
                    }
                })
                node_ids.add(record["n"].element_id)

            if record["r"]:
                edge_data = dict(record["r"])
                edges.append({
                    "data": {
                        "id": record["r"].element_id,
                        "source": record["n"]["id"],
                        "target": record["m"]["id"],
                        **edge_data
                    }
                })
        
        return {"elements": {"nodes": nodes, "edges": edges}}

async def add_event(source: str, message: str, level: str = "info"):
    """
    Logs an event and broadcasts it to all connected clients.
    Levels: 'info', 'warning', 'error'
    """
    event = {
        "id": datetime.now(timezone.utc).isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "message": message,
        "level": level
    }
    logging.info(f"EVENT [{level.upper()}]: {source} - {message}")
    await manager.broadcast(json.dumps({"events": [event]}))
    return event

async def broadcast_grid_state():
    """Fetches the current grid state and broadcasts it to all clients."""
    grid_state = await get_grid_state_from_db()
    await manager.broadcast(json.dumps(grid_state))

def bump_simulation_generation():
    """Invalidates in-flight long-running simulations (e.g., domino runs)."""
    global simulation_generation
    simulation_generation += 1
    return simulation_generation

def is_simulation_generation_active(run_generation: int) -> bool:
    return run_generation == simulation_generation

# --- WebSocket Endpoint ---
@app.websocket("/ws/grid-state")
async def websocket_endpoint(websocket: WebSocket):
    """
    Handles incoming WebSocket connections and messages.
    This provides a stable, long-lived connection for each client.
    """
    await manager.connect(websocket)
    try:
        while True:
            # The server will listen for a specific message from the client
            # to trigger an action, like sending the initial grid state.
            data = await websocket.receive_text()
            if data == 'get_grid':
                logging.info("Received 'get_grid' request. Broadcasting state.")
                await broadcast_grid_state()
            else:
                logging.warning(f"Received unknown message from client: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logging.info(f"Client disconnected. Total connections: {len(manager.active_connections)}")

# --- API Endpoints ---
@app.get("/api/grid-state")
async def get_grid_state():
    """HTTP endpoint to get the initial grid state."""
    return await get_grid_state_from_db()

@app.post("/api/seed-grid")
async def seed_grid():
    """
    Clears the database and seeds it with a default grid topology.
    This is useful for resetting the simulation.
    """
    if not driver:
        raise HTTPException(status_code=503, detail="Database not connected")

    # Invalidate any in-flight domino run so reset becomes authoritative.
    bump_simulation_generation()

    async with driver.session() as session:
        # Clear the database
        await session.run("MATCH (n) DETACH DELETE n")
        
        # Use the test.cypher file to seed the grid
        try:
            seed_file_path = os.path.join(os.path.dirname(__file__), "test.cypher")
            with open(seed_file_path, "r", encoding="utf-8") as f:
                cypher_query = f.read()
                await session.run(cypher_query)
            logging.info("Database cleared and seeded successfully.")
            # Tell clients to clear historical logs so reset is complete from UI perspective.
            await manager.broadcast(json.dumps({"events_reset": True}))
            await add_event("System", "Grid has been reset to default state.")
        except FileNotFoundError:
            raise HTTPException(status_code=500, detail="test.cypher file not found.")
    
    await broadcast_grid_state()
    return {"message": "Grid seeded successfully"}

@app.post("/api/control-power")
async def control_power(transformer_id: str, action: str):
    """
    Allows manual control to turn a transformer ON or OFF.
    This simulates manual intervention by a grid operator.
    """
    if not driver:
        raise HTTPException(status_code=503, detail="Database not connected")

    new_status = "ONLINE" if action.upper() == "ON" else "OFFLINE"
    async with driver.session() as session:
        await session.run(
            "MATCH (t:Transformer {id: $id}) SET t.status = $status",
            id=transformer_id, status=new_status
        )
    
    await add_event("Operator", f"Manually set {transformer_id} to {new_status}.")
    await broadcast_grid_state()
    return {"message": f"{transformer_id} turned {action}"}

@app.post("/api/trigger-domino")
async def trigger_domino(request: DominoRequest):
    """
    Triggers a cascading failure simulation starting from a specific node
    after applying an additional load.
    """
    if not driver:
        raise HTTPException(status_code=503, detail="Database not connected")

    async with domino_lock:
        run_generation = simulation_generation

        def ensure_active():
            if not is_simulation_generation_active(run_generation):
                raise asyncio.CancelledError("Simulation cancelled by reset or new control action")

        try:
            ensure_active()
            async with driver.session() as session:
                # First, apply the added load to the start node
                await session.run(
                    "MATCH (n {id: $id}) SET n.load_kw = n.load_kw + $load",
                    id=request.start_node_id, load=request.added_load
                )

            await add_event(
                "Simulation",
                f"Applying {request.added_load}kW additional load to {request.start_node_id} to trigger domino effect."
            )
            await broadcast_grid_state()
            await asyncio.sleep(1)
            ensure_active()

            async def run_failure_simulation(tx, start_node):
                ensure_active()

                # Set the current node to OFFLINE if it is still online
                result = await tx.run(
                    """
                    MATCH (n {id: $id})
                    WHERE n.status = 'ONLINE'
                    SET n.status = 'OFFLINE'
                    RETURN n.id AS id
                    """,
                    id=start_node
                )
                row = await result.single()
                if row is None:
                    return

                await add_event("Fault", f"Failure at {start_node}. Node is now OFFLINE.", "error")
                await broadcast_grid_state()
                await asyncio.sleep(1)
                ensure_active()

                # Find and process online neighbors
                result = await tx.run(
                    """
                    MATCH (start_node {id: $id})-[:CONNECTED_TO]-(neighbor)
                    WHERE neighbor.status = 'ONLINE'
                    RETURN neighbor.id AS id
                    """,
                    id=start_node
                )

                neighbors = [record["id"] async for record in result]
                for neighbor_id in neighbors:
                    ensure_active()
                    await add_event(
                        "Cascade",
                        f"Overload detected at {neighbor_id} due to failure at {start_node}.",
                        "warning"
                    )
                    await broadcast_grid_state()
                    await asyncio.sleep(1)
                    ensure_active()
                    await run_failure_simulation(tx, neighbor_id)

            async with driver.session() as session:
                await session.execute_write(run_failure_simulation, request.start_node_id)

            ensure_active()
            await add_event("System", "Cascading failure simulation complete.")
            await broadcast_grid_state()
            return {"message": "Domino effect triggered"}
        except asyncio.CancelledError:
            logging.info("Domino simulation cancelled due to reset/newer generation.")
            return {"message": "Domino effect cancelled due to grid reset."}

@app.post("/api/telemetry")
async def process_telemetry(payload: TelemetryPayload):
    """
    Receives telemetry data (e.g., load changes) and updates the grid state.
    Includes logic for "Smart Load Shedding" to automatically throttle high loads if Smart Mode is enabled.
    """
    if not driver:
        raise HTTPException(status_code=503, detail="Database not connected")

    async with driver.session() as session:
        for item in payload.data:
            # --- Smart Load Shedding Logic ---
            if smart_mode_enabled and item.load_kw >= 150 * 0.9:
                original_load = item.load_kw
                item.load_kw = 150 * 0.9  # Throttle the load
                await add_event(
                    "Auto-Shedding",
                    f"High load ({original_load}kW) on {item.transformer_id} detected. Throttling to {item.load_kw:.2f}kW.",
                    "warning"
                )

            # Update the database
            await session.run(
                "MATCH (t:Transformer {id: $id}) SET t.load_kw = $load",
                id=item.transformer_id, load=item.load_kw
            )
            logging.info(f"Updated {item.transformer_id} load to {item.load_kw} kW")

            if item.load_kw >= 150:
                await add_event(
                    "Telemetry",
                    f"{item.transformer_id} is overloaded at {item.load_kw:.2f}kW.",
                    "warning"
                )
            else:
                await add_event(
                    "Telemetry",
                    f"{item.transformer_id} load updated to {item.load_kw:.2f}kW.",
                    "info"
                )

    await broadcast_grid_state()
    return {"message": "Telemetry processed"}

@app.get("/api/smart-mode")
async def get_smart_mode():
    """Returns the current status of Smart Mode."""
    return {"enabled": smart_mode_enabled}

@app.post("/api/smart-mode")
async def set_smart_mode(request: SmartModeRequest):
    """Enables or disables Smart Mode."""
    global smart_mode_enabled
    smart_mode_enabled = request.enabled
    status = "enabled" if smart_mode_enabled else "disabled"
    await add_event("Operator", f"Smart Mode has been {status}.")
    return {"message": f"Smart Mode {status}"}

@app.post("/api/create-tie-line")
async def create_tie_line(request: TieLineRequest):
    """
    Creates a new connection (tie-line) between two nodes in the grid.
    This simulates dynamic grid reconfiguration.
    """
    if not driver:
        raise HTTPException(status_code=503, detail="Database not connected")

    async with driver.session() as session:
        # Using MERGE to avoid creating duplicate relationships
        await session.run("""
            MATCH (a {id: $source}), (b {id: $target})
            MERGE (a)-[r:CONNECTED_TO]-(b)
            RETURN r
        """, source=request.source, target=request.target)
    
    await add_event("Operator", f"New tie-line created between {request.source} and {request.target}.")
    await broadcast_grid_state()
    return {"message": f"Tie-line created between {request.source} and {request.target}"}