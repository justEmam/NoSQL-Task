"""
Distributed Data Store Service with Replication and Leader Election.
Supports leader/follower replication with automatic failover.
"""
import os
import sys
import json
import random
import threading
import time
import requests
from flask import Flask, request, jsonify
from datetime import datetime
from collections import defaultdict

webapp = Flask(__name__)


class TransactionLog:
    """Transaction log for data safety."""
    
    def __init__(self, log_path="txlog.dat"):
        self.log_path = log_path
        self.mutex = threading.Lock()
    
    def record(self, action, payload):
        """Record action to log synchronously."""
        with self.mutex:
            entry = {
                "ts": datetime.now().isoformat(),
                "action": action,
                "payload": payload
            }
            with open(self.log_path, "a") as handle:
                handle.write(json.dumps(entry) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return entry
    
    def replay(self):
        """Replay all records from log."""
        records = []
        if os.path.exists(self.log_path):
            with open(self.log_path, "r") as handle:
                for row in handle:
                    row = row.strip()
                    if row:
                        records.append(json.loads(row))
        return records
    
    def get_position(self):
        """Get current log position."""
        records = self.replay()
        return len(records)
    
    def truncate(self):
        """Clear log after snapshot."""
        with self.mutex:
            if os.path.exists(self.log_path):
                os.remove(self.log_path)


class VectorModel:
    """Simple vector model for semantic matching (demo)."""
    
    def __init__(self, dimensions=50):
        self.dimensions = dimensions
        self.term_vectors = {}
    
    def get_term_vector(self, term):
        """Get or create vector for a term."""
        term = term.lower()
        if term not in self.term_vectors:
            random.seed(hash(term))
            self.term_vectors[term] = [random.gauss(0, 1) for _ in range(self.dimensions)]
        return self.term_vectors[term]
    
    def get_content_vector(self, content):
        """Get vector for content (average of term vectors)."""
        terms = content.lower().split()
        if not terms:
            return [0.0] * self.dimensions
        
        vecs = [self.get_term_vector(t) for t in terms]
        avg = [sum(v[i] for v in vecs) / len(vecs) for i in range(self.dimensions)]
        return avg
    
    def similarity(self, vec_a, vec_b):
        """Compute cosine similarity."""
        dot_prod = sum(a * b for a, b in zip(vec_a, vec_b))
        mag_a = sum(a * a for a in vec_a) ** 0.5
        mag_b = sum(b * b for b in vec_b) ** 0.5
        if mag_a == 0 or mag_b == 0:
            return 0
        return dot_prod / (mag_a * mag_b)


class DistributedDataStore:
    """Data Store with clustering, replication, and search."""
    
    def __init__(self, snapshot_path="snapshot.json", log_path="txlog.dat", instance_id=None):
        self.snapshot_path = snapshot_path
        self.txlog = TransactionLog(log_path)
        self.records = {}
        self.mutex = threading.Lock()
        self.instance_id = instance_id or "primary"
        
        # Cluster settings
        self.is_leader = False
        self.leader_addr = None
        self.followers = []  # List of (instance_id, addr) tuples
        self.distributed = False
        
        # Election state
        self.epoch = 0
        self.voted_for = None
        self.last_ping = time.time()
        self.election_wait = random.uniform(1.5, 3.0)
        self.ping_rate = 0.5
        
        # Search structures
        self.term_map = {}  # term -> set of record ids
        self.vec_store = {}  # record id -> vector
        self.vec_model = VectorModel()
        
        # Multi-leader mode
        self.multi_leader = False
        self.logical_clock = defaultdict(int)
        
        self._restore()
    
    def _restore(self):
        """Restore state from snapshot and log."""
        if os.path.exists(self.snapshot_path):
            try:
                with open(self.snapshot_path, "r") as handle:
                    self.records = json.load(handle)
            except:
                self.records = {}
        
        for entry in self.txlog.replay():
            act = entry["action"]
            payload = entry["payload"]
            if act == "put":
                self.records[payload["id"]] = payload["content"]
            elif act == "remove":
                self.records.pop(payload["id"], None)
            elif act == "batch_put":
                for rid, content in payload["entries"]:
                    self.records[rid] = content
        
        self._rebuild_search()
        self._snapshot()
    
    def _rebuild_search(self):
        """Rebuild search structures from records."""
        self.term_map = {}
        self.vec_store = {}
        for rid, content in self.records.items():
            self._index_record(rid, content)
    
    def _index_record(self, rid, content):
        """Add record to search structures."""
        if isinstance(content, str):
            # Term map
            terms = content.lower().split()
            for term in terms:
                if term not in self.term_map:
                    self.term_map[term] = set()
                self.term_map[term].add(rid)
            
            # Vector
            self.vec_store[rid] = self.vec_model.get_content_vector(content)
    
    def _unindex_record(self, rid):
        """Remove record from search structures."""
        for term in list(self.term_map.keys()):
            self.term_map[term].discard(rid)
            if not self.term_map[term]:
                del self.term_map[term]
        self.vec_store.pop(rid, None)
    
    def _snapshot(self):
        """Save current state and clear log."""
        with self.mutex:
            with open(self.snapshot_path, "w") as handle:
                json.dump(self.records, handle)
                handle.flush()
                os.fsync(handle.fileno())
            self.txlog.truncate()
    
    def _persist(self, simulate_issue=False):
        """Persist records to disk."""
        if simulate_issue:
            if random.random() < 0.01:
                return False
        with open(self.snapshot_path, "w") as handle:
            json.dump(self.records, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return True
    
    def _sync_to_followers(self, action, payload):
        """Sync action to follower instances."""
        if not self.distributed or self.multi_leader:
            return
        
        for inst_id, addr in self.followers:
            try:
                requests.post(
                    f"{addr}/sync",
                    json={"action": action, "payload": payload, "epoch": self.epoch},
                    timeout=1
                )
            except:
                pass
    
    def apply_sync(self, action, payload):
        """Apply a synced action."""
        with self.mutex:
            if action == "put":
                self.records[payload["id"]] = payload["content"]
                self._index_record(payload["id"], payload["content"])
            elif action == "remove":
                self._unindex_record(payload["id"])
                self.records.pop(payload["id"], None)
            elif action == "batch_put":
                for rid, content in payload["entries"]:
                    self.records[rid] = content
                    self._index_record(rid, content)
            self._persist()
    
    def fetch(self, rid):
        """Fetch content by record id."""
        with self.mutex:
            return self.records.get(rid)
    
    def put(self, rid, content, simulate_issue=False):
        """Put record with id and content."""
        if self.distributed and not self.is_leader and not self.multi_leader:
            return False, "Not leader"
        
        with self.mutex:
            self.txlog.record("put", {"id": rid, "content": content})
            
            existing = self.records.get(rid)
            self.records[rid] = content
            
            if existing is not None:
                self._unindex_record(rid)
            self._index_record(rid, content)
            
            self._persist(simulate_issue)
            
            if self.multi_leader:
                self.logical_clock[self.instance_id] += 1
            
        self._sync_to_followers("put", {"id": rid, "content": content})
        return True, "OK"
    
    def remove(self, rid, simulate_issue=False):
        """Remove record by id."""
        if self.distributed and not self.is_leader and not self.multi_leader:
            return False, "Not leader"
        
        with self.mutex:
            if rid in self.records:
                self.txlog.record("remove", {"id": rid})
                self._unindex_record(rid)
                del self.records[rid]
                self._persist(simulate_issue)
                
                if self.multi_leader:
                    self.logical_clock[self.instance_id] += 1
                
                self._sync_to_followers("remove", {"id": rid})
                return True, "OK"
            return False, "Record not found"
    
    def batch_put(self, entries, simulate_issue=False):
        """Batch put multiple records atomically."""
        if self.distributed and not self.is_leader and not self.multi_leader:
            return False, "Not leader"
        
        with self.mutex:
            self.txlog.record("batch_put", {"entries": entries})
            
            for rid, content in entries:
                existing = self.records.get(rid)
                if existing is not None:
                    self._unindex_record(rid)
                self.records[rid] = content
                self._index_record(rid, content)
            
            self._persist(simulate_issue)
            
            if self.multi_leader:
                self.logical_clock[self.instance_id] += 1
        
        self._sync_to_followers("batch_put", {"entries": entries})
        return True, "OK"
    
    def query(self, terms):
        """Query records by terms (AND search)."""
        term_list = terms.lower().split()
        if not term_list:
            return []
        
        matching = None
        for term in term_list:
            rids = self.term_map.get(term, set())
            if matching is None:
                matching = rids.copy()
            else:
                matching &= rids
        
        if matching is None:
            return []
        
        return [(rid, self.records[rid]) for rid in matching if rid in self.records]
    
    def semantic_query(self, terms, limit=5):
        """Semantic query using vector similarity."""
        query_vec = self.vec_model.get_content_vector(terms)
        
        scores = []
        for rid, vec in self.vec_store.items():
            score = self.vec_model.similarity(query_vec, vec)
            scores.append((rid, score, self.records.get(rid)))
        
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:limit]
    
    def list_ids(self):
        """List all record ids."""
        with self.mutex:
            return list(self.records.keys())
    
    # Leader Election Methods
    def begin_election(self):
        """Start leader election."""
        self.epoch += 1
        self.voted_for = self.instance_id
        ballot_count = 1
        
        for inst_id, addr in self.followers:
            try:
                resp = requests.post(
                    f"{addr}/ballot",
                    json={"epoch": self.epoch, "nominee": self.instance_id},
                    timeout=1
                )
                if resp.status_code == 200 and resp.json().get("accepted"):
                    ballot_count += 1
            except:
                pass
        
        total = len(self.followers) + 1
        if ballot_count > total // 2:
            self.is_leader = True
            print(f"[{self.instance_id}] Became leader with {ballot_count}/{total} ballots")
            return True
        return False
    
    def handle_ballot(self, epoch, nominee):
        """Handle ballot request from another instance."""
        if epoch > self.epoch:
            self.epoch = epoch
            self.voted_for = None
            self.is_leader = False
        
        if epoch >= self.epoch and (self.voted_for is None or self.voted_for == nominee):
            self.voted_for = nominee
            self.last_ping = time.time()
            return True
        return False
    
    def handle_ping(self, epoch, leader):
        """Handle ping from leader."""
        if epoch >= self.epoch:
            self.epoch = epoch
            self.is_leader = False
            self.leader_addr = leader
            self.last_ping = time.time()
            self.voted_for = None
    
    def send_pings(self):
        """Send pings to followers."""
        if not self.is_leader:
            return
        
        for inst_id, addr in self.followers:
            try:
                requests.post(
                    f"{addr}/ping",
                    json={"epoch": self.epoch, "leader": self.instance_id},
                    timeout=0.5
                )
            except:
                pass
    
    def check_election_needed(self):
        """Check if election is needed."""
        if self.is_leader:
            return False
        return time.time() - self.last_ping > self.election_wait


# Global instance
storage = None


def get_storage():
    global storage
    if storage is None:
        data_folder = os.environ.get("DATASTORE_FOLDER", ".")
        instance_id = os.environ.get("DATASTORE_INSTANCE", "primary")
        snapshot_path = os.path.join(data_folder, f"snapshot_{instance_id}.json")
        log_path = os.path.join(data_folder, f"txlog_{instance_id}.dat")
        storage = DistributedDataStore(snapshot_path, log_path, instance_id)
    return storage


# Background thread for election
def election_daemon():
    """Background thread for leader election and pings."""
    while True:
        try:
            store = get_storage()
            if store.distributed and not store.multi_leader:
                if store.is_leader:
                    store.send_pings()
                elif store.check_election_needed():
                    print(f"[{store.instance_id}] Election timeout, starting election")
                    store.begin_election()
            time.sleep(0.3)
        except:
            time.sleep(1)


# Start election thread
election_worker = threading.Thread(target=election_daemon, daemon=True)


@webapp.route("/fetch/<rid>", methods=["GET"])
def api_fetch(rid):
    """Fetch content by record id."""
    content = get_storage().fetch(rid)
    if content is None:
        return jsonify({"err": "Record not found"}), 404
    return jsonify({"id": rid, "content": content})


@webapp.route("/put", methods=["POST"])
def api_put():
    """Put record."""
    body = request.get_json()
    rid = body.get("id")
    content = body.get("content")
    simulate_issue = body.get("simulate_issue", False)
    
    if rid is None or content is None:
        return jsonify({"err": "Missing id or content"}), 400
    
    ok, msg = get_storage().put(rid, content, simulate_issue)
    if not ok:
        return jsonify({"err": msg}), 400
    return jsonify({"ok": True, "id": rid})


@webapp.route("/remove/<rid>", methods=["DELETE"])
def api_remove(rid):
    """Remove record."""
    simulate_issue = request.args.get("simulate_issue", "false").lower() == "true"
    ok, msg = get_storage().remove(rid, simulate_issue)
    if not ok:
        return jsonify({"err": msg}), 404
    return jsonify({"ok": True, "id": rid})


@webapp.route("/batch_put", methods=["POST"])
def api_batch_put():
    """Batch put multiple records."""
    body = request.get_json()
    entries = body.get("entries", [])
    simulate_issue = body.get("simulate_issue", False)
    
    if not entries:
        return jsonify({"err": "No entries provided"}), 400
    
    ok, msg = get_storage().batch_put(entries, simulate_issue)
    if not ok:
        return jsonify({"err": msg}), 400
    return jsonify({"ok": True, "count": len(entries)})


@webapp.route("/query", methods=["GET"])
def api_query():
    """Query records by terms."""
    terms = request.args.get("terms", "")
    matches = get_storage().query(terms)
    return jsonify({"matches": matches})


@webapp.route("/semantic", methods=["GET"])
def api_semantic():
    """Semantic query using vectors."""
    terms = request.args.get("terms", "")
    limit = int(request.args.get("limit", 5))
    matches = get_storage().semantic_query(terms, limit)
    return jsonify({"matches": matches})


@webapp.route("/ids", methods=["GET"])
def api_ids():
    """List all record ids."""
    ids = get_storage().list_ids()
    return jsonify({"ids": ids})


@webapp.route("/status", methods=["GET"])
def api_status():
    """Status check endpoint."""
    store = get_storage()
    return jsonify({
        "status": "running",
        "instance": store.instance_id,
        "is_leader": store.is_leader,
        "epoch": store.epoch
    })


@webapp.route("/cluster/info", methods=["GET"])
def cluster_info():
    """Get cluster information."""
    store = get_storage()
    return jsonify({
        "instance": store.instance_id,
        "is_leader": store.is_leader,
        "epoch": store.epoch,
        "distributed": store.distributed,
        "multi_leader": store.multi_leader,
        "followers": store.followers
    })


@webapp.route("/cluster/register", methods=["POST"])
def cluster_register():
    """Register to a cluster."""
    body = request.get_json()
    followers = body.get("followers", [])
    multi_leader = body.get("multi_leader", False)
    
    store = get_storage()
    store.followers = [(f["instance"], f["addr"]) for f in followers]
    store.distributed = True
    store.multi_leader = multi_leader
    
    if not multi_leader and not store.is_leader:
        store.last_ping = time.time()
    
    return jsonify({"ok": True})


@webapp.route("/sync", methods=["POST"])
def api_sync():
    """Receive synced action from leader."""
    body = request.get_json()
    action = body.get("action")
    payload = body.get("payload")
    epoch = body.get("epoch", 0)
    
    store = get_storage()
    if epoch >= store.epoch:
        store.apply_sync(action, payload)
    
    return jsonify({"ok": True})


@webapp.route("/ballot", methods=["POST"])
def api_ballot():
    """Handle ballot request."""
    body = request.get_json()
    epoch = body.get("epoch")
    nominee = body.get("nominee")
    
    accepted = get_storage().handle_ballot(epoch, nominee)
    return jsonify({"accepted": accepted})


@webapp.route("/ping", methods=["POST"])
def api_ping():
    """Handle ping from leader."""
    body = request.get_json()
    epoch = body.get("epoch")
    leader = body.get("leader")
    
    get_storage().handle_ping(epoch, leader)
    return jsonify({"ok": True})


@webapp.route("/promote", methods=["POST"])
def api_promote():
    """Force this instance to become leader (for testing)."""
    store = get_storage()
    store.is_leader = True
    store.epoch += 1
    return jsonify({"ok": True, "epoch": store.epoch})


if __name__ == "__main__":
    election_worker.start()
    
    port_num = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    webapp.run(host="0.0.0.0", port=port_num, threaded=True)
