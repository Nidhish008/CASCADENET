import os
import sys
import time
import uuid
import asyncio
import logging
import subprocess
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase, exceptions
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Dict, List, Optional, Set

# Set up logging for production-ready observability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

print("--- DEBUG INFO ---")
print(f"Loaded URI: {os.getenv('NEO4J_URI')}")
print(f"Loaded User: {os.getenv('NEO4J_USERNAME')}")
print("------------------")

# ==========================================
# Database Architecture & Connection Manager
# ==========================================
class Neo4jConnectionManager:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = None

    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            self.driver.verify_connectivity()
            logger.info("Successfully connected to the Neo4j database.")
            return True
        except Exception as e:
            self.driver = None
            logger.error(f"Failed to connect to Neo4j: {e}")
            return False

    def is_connected(self):
        if self.driver is None:
            return False
        try:
            self.driver.verify_connectivity()
            return True
        except Exception:
            return False

    def ensure_connected(self):
        if self.is_connected():
            return True
        self.close()
        return self.connect()

    def close(self):
        if self.driver is not None:
            self.driver.close()
            logger.info("Neo4j connection closed.")

    def execute_transaction(self, queries_with_params: list):
        if not self.driver:
            raise Exception("Driver not initialized.")
        with self.driver.session() as session:
            try:
                def _tx_logic(tx):
                    results = []
                    for query_dict in queries_with_params:
                        query = query_dict.get("query")
                        params = query_dict.get("params", {})
                        result = tx.run(query, params)
                        results.append(result.consume()) 
                    return results
                return session.execute_write(_tx_logic)
            except exceptions.Neo4jError as e:
                logger.error(f"Transaction failed: {e}")
                raise e

db_manager = Neo4jConnectionManager()


def ensure_db_or_503():
    if db_manager.ensure_connected():
        return

    raise HTTPException(
        status_code=503,
        detail=(
            "Neo4j is unavailable. Check NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD in .env "
            "and ensure the database is reachable."
        )
    )

# ==========================================
# FastAPI Application & Lifecycle Setup
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global smart_mode_task
    try:
        db_manager.connect()
        smart_mode_task = asyncio.create_task(smart_mode_monitor_loop())
        add_event("Backend service started.", level="success", source="system")
    except Exception as e:
        logger.critical(f"Could not establish database connection on startup: {e}")
        add_event(f"Startup database connection failed: {e}", level="error", source="system")
    yield 
    if smart_mode_task is not None:
        smart_mode_task.cancel()
        try:
            await smart_mode_task
        except asyncio.CancelledError:
            pass
    db_manager.close()

app = FastAPI(
    title="Smart Infrastructure Asset Relationship Analytics Platform",
    description="API for managing and analyzing cascading power grid failures.",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# Data Models
# ==========================================
class SpikeRequest(BaseModel):
    target_name: str
    added_load: float

class TelemetryData(BaseModel):
    transformer_id: str
    load_kw: float

class TelemetryPayload(BaseModel):
    data: List[TelemetryData]


class SmartModeRequest(BaseModel):
    enabled: bool


BASE_DIR = Path(__file__).resolve().parent
SIMULATOR_SCRIPT = BASE_DIR / "simulator.py"
simulator_process = None
smart_mode_enabled = False
smart_mode_task: Optional[asyncio.Task] = None

SHED_THRESHOLD_RATIO = 0.90
SHED_REDUCTION_RATIO = 0.85
EMERGENCY_SHED_REDUCTION_RATIO = 0.70
MAX_SMART_INTERVENTION_ROUNDS = 3
SMART_MONITOR_INTERVAL_SECONDS = 2
SMART_SHED_COOLDOWN_SECONDS = 10

recent_events: deque = deque(maxlen=250)
last_shed_times: Dict[str, float] = {}


def add_event(message: str, level: str = "info", source: str = "system"):
    now = datetime.now(timezone.utc)
    recent_events.appendleft({
        "id": str(uuid.uuid4()),
        "timestamp": now.isoformat(),
        "epoch_ms": int(now.timestamp() * 1000),
        "level": level,
        "source": source,
        "message": message
    })


def get_online_transformers(session):
    return session.run("""
        MATCH (t:Transformer)
        WHERE t.status = 'ONLINE'
        RETURN t.name AS id, t.current_load AS load, t.max_capacity AS max
    """).data()


def get_overloaded_nodes(session, fail_at_capacity: bool = False):
    comparator = ">=" if fail_at_capacity else ">"
    query = f"""
        MATCH (t:Transformer)
        WHERE t.status = 'ONLINE' AND t.current_load {comparator} t.max_capacity
        RETURN t.name AS id, t.current_load AS load, t.max_capacity AS max
    """
    return session.run(query).data()


def run_smart_intervention(
    session,
    include_overloaded: bool = False,
    trigger_source: str = "smart-mode",
    only_ids: Optional[Set[str]] = None,
):
    now_monotonic = time.monotonic()
    interventions = 0

    for tx in get_online_transformers(session):
        tx_id = tx["id"]
        if only_ids and tx_id not in only_ids:
            continue

        current_load = float(tx["load"])
        max_capacity = float(tx["max"])
        if max_capacity <= 0:
            continue

        utilization = current_load / max_capacity
        reduction_ratio = None
        reason = None

        if SHED_THRESHOLD_RATIO <= utilization < 1.0:
            last_shed = last_shed_times.get(tx_id, 0.0)
            if (now_monotonic - last_shed) < SMART_SHED_COOLDOWN_SECONDS:
                continue
            reduction_ratio = SHED_REDUCTION_RATIO
            reason = "preventive"
        elif include_overloaded and utilization >= 1.0:
            reduction_ratio = EMERGENCY_SHED_REDUCTION_RATIO
            reason = "emergency"

        if reduction_ratio is None:
            continue

        reduced_load = round(current_load * reduction_ratio, 2)
        session.run("""
            MATCH (t:Transformer {name: $id})
            SET t.current_load = $new_load
        """, id=tx_id, new_load=reduced_load)

        last_shed_times[tx_id] = now_monotonic
        interventions += 1
        add_event(
            f"Smart Mode {reason} shedding on {tx_id}: {current_load:.2f} -> {reduced_load:.2f} kW",
            level="success",
            source=trigger_source,
        )

    return interventions


def run_cascade_algorithm(
    session,
    smart_mode_guard: bool = False,
    trigger_source: str = "cascade",
    fail_at_capacity: bool = False,
    aggressive_domino: bool = False,
):
    """Executes cascading failure logic until no overloaded ONLINE transformers remain."""
    while True:
        overloaded_nodes = get_overloaded_nodes(session, fail_at_capacity=fail_at_capacity)
        if not overloaded_nodes:
            break

        for node in overloaded_nodes:
            failing_id = node["id"]

            if smart_mode_guard:
                rescued = run_smart_intervention(
                    session,
                    include_overloaded=True,
                    trigger_source=trigger_source,
                    only_ids={failing_id},
                )
                if rescued > 0:
                    rescue_comparator = ">=" if fail_at_capacity else ">"
                    still_overloaded = session.run("""
                        MATCH (t:Transformer {name: $id})
                        RETURN t.current_load {} t.max_capacity AS overloaded
                    """.format(rescue_comparator), id=failing_id).single()
                    if still_overloaded and not still_overloaded["overloaded"]:
                        add_event(
                            f"Smart Mode prevented shutdown of {failing_id}.",
                            level="success",
                            source="smart-mode",
                        )
                        continue

            failed_row = session.run("""
                MATCH (t:Transformer {name: $id})
                SET t.status = 'OFFLINE'
                RETURN t.current_load AS load
            """, id=failing_id).single()

            if not failed_row:
                continue

            orphaned_load = float(failed_row["load"])

            neighbors = session.run("""
                MATCH (t:Transformer {name: $id})-[:CONNECTED_TO]-(neighbor:Transformer)
                WHERE neighbor.status = 'ONLINE'
                RETURN neighbor.name AS id, neighbor.current_load AS load, neighbor.max_capacity AS max
            """, id=failing_id).data()

            if not neighbors:
                add_event(
                    f"{orphaned_load:.2f} kW curtailed after {failing_id} failed (no online neighbors).",
                    level="info",
                    source="cascade",
                )
                continue

            if aggressive_domino:
                split_load = orphaned_load / len(neighbors)
                for neighbor in neighbors:
                    session.run("""
                        MATCH (n:Transformer {name: $id})
                        SET n.current_load = n.current_load + $extra_load
                    """, id=neighbor["id"], extra_load=split_load)
                continue

            headrooms = []
            for neighbor in neighbors:
                headroom = max(0.0, float(neighbor["max"]) - float(neighbor["load"]))
                headrooms.append((neighbor["id"], headroom))

            total_headroom = sum(headroom for _, headroom in headrooms)
            if total_headroom <= 0:
                add_event(
                    f"{orphaned_load:.2f} kW curtailed after {failing_id} failed (neighbors full).",
                    level="info",
                    source="cascade",
                )
                continue

            distributed = 0.0
            for neighbor_id, headroom in headrooms:
                if headroom <= 0:
                    continue
                proposed = orphaned_load * (headroom / total_headroom)
                alloc = round(min(proposed, headroom), 2)
                if alloc <= 0:
                    continue

                session.run("""
                    MATCH (n:Transformer {name: $id})
                    SET n.current_load = n.current_load + $extra_load
                """, id=neighbor_id, extra_load=alloc)
                distributed += alloc

            curtailed = round(max(0.0, orphaned_load - distributed), 2)
            if curtailed > 0:
                add_event(
                    f"{curtailed:.2f} kW curtailed during redistribution from {failing_id}.",
                    level="info",
                    source="cascade",
                )


async def smart_mode_monitor_loop():
    while True:
        try:
            if smart_mode_enabled and db_manager.driver is not None:
                with db_manager.driver.session() as session:
                    changed = run_smart_intervention(
                        session,
                        include_overloaded=False,
                        trigger_source="smart-mode",
                    )
                    if changed > 0:
                        run_cascade_algorithm(
                            session,
                            smart_mode_guard=False,
                            trigger_source="smart-mode",
                        )
        except Exception as exc:
            logger.error(f"Smart Mode monitor loop error: {exc}")
            add_event(f"Smart Mode monitor error: {exc}", level="error", source="smart-mode")
        await asyncio.sleep(SMART_MONITOR_INTERVAL_SECONDS)


# ==========================================
# Core Endpoints
# ==========================================
@app.post("/api/seed-grid", status_code=status.HTTP_201_CREATED)
async def seed_grid():
    """Wipes the database and seeds a new 5-node transformer ring topology."""
    ensure_db_or_503()
    clear_query = "MATCH (n) DETACH DELETE n"
    seed_query = """
    CREATE
            (a:Transformer {name: 'TX-A', max_capacity: 100.0, current_load: 40.0, status: 'ONLINE'}),
      (b:Transformer {name: 'TX-B', max_capacity: 250.0, current_load: 40.0, status: 'ONLINE'}),
            (c:Transformer {name: 'TX-C', max_capacity: 100.0, current_load: 40.0, status: 'ONLINE'}),
            (d:Transformer {name: 'TX-D', max_capacity: 100.0, current_load: 40.0, status: 'ONLINE'}),
            (e:Transformer {name: 'TX-E', max_capacity: 100.0, current_load: 40.0, status: 'ONLINE'}),
      (a)-[:CONNECTED_TO]->(b),
      (b)-[:CONNECTED_TO]->(c),
      (c)-[:CONNECTED_TO]->(d),
      (d)-[:CONNECTED_TO]->(e),
      (e)-[:CONNECTED_TO]->(a)
    """
    try:
        db_manager.execute_transaction([{"query": clear_query}, {"query": seed_query}])
        add_event("Grid topology seeded (5-node ring).", level="success", source="manual")
        return {"message": "Grid seeded successfully. 5 Transformers created in a ring topology."}
    except Exception as e:
        add_event(f"Grid seeding failed: {e}", level="error", source="manual")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/grid-state")
def get_grid_state():
    """Fetches the current state of the grid for the React/Cytoscape frontend."""
    ensure_db_or_503()
    with db_manager.driver.session() as session:
        # Get all transformers (Mapped to 'name' property based on your seed query)
        node_result = session.run("""
            MATCH (n:Transformer) 
            RETURN n.name AS id, n.status AS status, n.current_load AS load, n.max_capacity AS max
        """)
        nodes = [
            {"data": {"id": record["id"], "label": record["id"], "status": record["status"], "load": round(record["load"], 2), "max": record["max"]}} 
            for record in node_result
        ]

        # Get all connections
        edge_result = session.run("""
            MATCH (a:Transformer)-[:CONNECTED_TO]->(b:Transformer) 
            RETURN a.name AS source, b.name AS target
        """)
        edges = [
            {"data": {"source": record["source"], "target": record["target"]}} 
            for record in edge_result
        ]

    return {"elements": {"nodes": nodes, "edges": edges}}


@app.post("/api/control-power")
def control_power(payload: TelemetryPayload):
    """Sets transformer loads directly from the control panel and runs cascade checks."""
    ensure_db_or_503()
    with db_manager.driver.session() as session:
        for item in payload.data:
            session.run("""
                MATCH (t:Transformer {name: $id})
                SET t.current_load = $load
                SET t.status = CASE
                    WHEN $load <= t.max_capacity THEN 'ONLINE'
                    ELSE t.status
                END
            """, id=item.transformer_id, load=item.load_kw)

        intervention_count = 0
        if smart_mode_enabled:
            for _ in range(MAX_SMART_INTERVENTION_ROUNDS):
                changed = run_smart_intervention(
                    session,
                    include_overloaded=True,
                    trigger_source="manual",
                )
                intervention_count += changed
                if changed == 0:
                    break

        run_cascade_algorithm(
            session,
            smart_mode_guard=smart_mode_enabled,
            trigger_source="manual",
        )

    add_event("Manual power controls applied.", level="success", source="manual")

    return {
        "message": "Power controls applied and cascade logic evaluated.",
        "smart_mode_applied": smart_mode_enabled,
        "interventions": intervention_count,
    }


@app.post("/api/trigger-domino")
def trigger_domino(spike: SpikeRequest):
    """Injects a load spike for a specific transformer and executes cascade logic."""
    ensure_db_or_503()
    with db_manager.driver.session() as session:
        existing = session.run(
            "MATCH (t:Transformer {name: $id}) RETURN t.name AS id",
            id=spike.target_name
        ).single()

        if not existing:
            raise HTTPException(status_code=404, detail=f"Transformer '{spike.target_name}' not found")

        session.run("""
            MATCH (t:Transformer {name: $id})
            SET t.current_load = t.current_load + $added_load
        """, id=spike.target_name, added_load=spike.added_load)

        if smart_mode_enabled:
            for _ in range(MAX_SMART_INTERVENTION_ROUNDS):
                changed = run_smart_intervention(
                    session,
                    include_overloaded=True,
                    trigger_source="manual",
                )
                if changed == 0:
                    break

        run_cascade_algorithm(
            session,
            smart_mode_guard=smart_mode_enabled,
            trigger_source="manual",
            fail_at_capacity=True,
            aggressive_domino=True,
        )

    add_event(
        f"Domino event triggered on {spike.target_name} (+{spike.added_load} kW).",
        level="success",
        source="manual"
    )

    return {
        "message": f"Domino event triggered on {spike.target_name} (+{spike.added_load} kW)."
    }

@app.post("/api/telemetry")
def process_telemetry(payload: TelemetryPayload):
    """The Cascade Engine: Receives EV spikes, updates loads, and recursively calculates cascading failures."""
    ensure_db_or_503()
    with db_manager.driver.session() as session:
        # STEP 1: Apply incoming loads
        for item in payload.data:
            session.run("""
                MATCH (t:Transformer {name: $id}) 
                SET t.current_load = $load
            """, id=item.transformer_id, load=item.load_kw)

        if smart_mode_enabled:
            for _ in range(MAX_SMART_INTERVENTION_ROUNDS):
                changed = run_smart_intervention(
                    session,
                    include_overloaded=True,
                    trigger_source="smart-mode",
                )
                if changed == 0:
                    break

        # STEP 2: Domino-effect cascade evaluation
        run_cascade_algorithm(
            session,
            smart_mode_guard=smart_mode_enabled,
            trigger_source="smart-mode",
        )

    return {"message": "Telemetry processed. Cascade algorithms executed."}


@app.get("/api/simulator/status")
def get_simulator_status():
    global simulator_process
    running = simulator_process is not None and simulator_process.poll() is None
    return {
        "running": running,
        "pid": simulator_process.pid if running else None
    }


@app.post("/api/simulator/start")
def start_simulator():
    global simulator_process

    if not SIMULATOR_SCRIPT.exists():
        raise HTTPException(status_code=404, detail="simulator.py not found")

    if simulator_process is not None and simulator_process.poll() is None:
        add_event("Simulator start requested but it is already running.", level="info", source="simulator")
        return {
            "message": "Simulator is already running.",
            "pid": simulator_process.pid
        }

    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    try:
        simulator_process = subprocess.Popen(
            [sys.executable, str(SIMULATOR_SCRIPT)],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags
        )
    except Exception as exc:
        add_event(f"Simulator failed to start: {exc}", level="error", source="simulator")
        raise HTTPException(status_code=500, detail=f"Failed to start simulator: {exc}")

    add_event("Simulator started.", level="success", source="simulator")

    return {
        "message": "Simulator started successfully.",
        "pid": simulator_process.pid
    }


@app.post("/api/simulator/stop")
def stop_simulator():
    global simulator_process

    if simulator_process is None or simulator_process.poll() is not None:
        simulator_process = None
        add_event("Simulator stop requested but it was not running.", level="info", source="simulator")
        return {"message": "Simulator is not running."}

    try:
        simulator_process.terminate()
        simulator_process.wait(timeout=5)
    except Exception:
        simulator_process.kill()
    finally:
        simulator_process = None

    add_event("Simulator stopped.", level="success", source="simulator")

    return {"message": "Simulator stopped."}


@app.get("/api/smart-mode")
def get_smart_mode_status():
    return {
        "enabled": smart_mode_enabled,
        "threshold_ratio": SHED_THRESHOLD_RATIO,
        "reduction_percent": int((1 - SHED_REDUCTION_RATIO) * 100),
        "cooldown_seconds": SMART_SHED_COOLDOWN_SECONDS
    }


@app.post("/api/smart-mode")
def set_smart_mode(payload: SmartModeRequest):
    global smart_mode_enabled

    smart_mode_enabled = payload.enabled
    add_event(
        f"Smart Mode turned {'ON' if smart_mode_enabled else 'OFF'}.",
        level="success",
        source="manual"
    )

    return {
        "enabled": smart_mode_enabled,
        "message": f"Smart Mode {'enabled' if smart_mode_enabled else 'disabled'}."
    }


@app.get("/api/events")
def get_events(limit: int = Query(default=100, ge=1, le=250)):
    events = list(recent_events)[:limit]
    return {"events": events}


@app.post("/api/events/clear")
def clear_events():
    recent_events.clear()
    add_event("Event log cleared from dashboard.", level="info", source="manual")
    return {"message": "Event log cleared."}

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Smart Infrastructure API",
        "neo4j_connected": db_manager.is_connected()
    }