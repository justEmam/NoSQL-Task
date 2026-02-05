"""
Persistent Data Store Service with Transaction Log.
Supports Put, Fetch, Remove, BatchPut operations.
"""
import os
import json
import random
import threading
import time
from flask import Flask, request, jsonify
from datetime import datetime

webapp = Flask(__name__)

class TransactionLog:
    """Transaction log for data safety."""
    
    def __init__(self, log_path="txlog.dat"):
        self.log_path = log_path
        self.mutex = threading.Lock()
    
    def record(self, action, payload):
        """Record action to log synchronously."""
        with self.mutex:
            record = {
                "ts": datetime.now().isoformat(),
                "action": action,
                "payload": payload
            }
            with open(self.log_path, "a") as handle:
                handle.write(json.dumps(record) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
    
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
    
    def truncate(self):
        """Clear log after snapshot."""
        with self.mutex:
            if os.path.exists(self.log_path):
                os.remove(self.log_path)


class DataStore:
    """Persistent Data Store with Transaction Log."""
    
    def __init__(self, snapshot_path="snapshot.json", log_path="txlog.dat", instance_id=None):
        self.snapshot_path = snapshot_path
        self.txlog = TransactionLog(log_path)
        self.records = {}
        self.mutex = threading.Lock()
        self.instance_id = instance_id
        
        # Cluster settings
        self.is_leader = True
        self.leader_addr = None
        self.replicas = []
        self.distributed = False
        
        # Search structures
        self.word_map = {}  # term -> set of record ids
        self.vectors = {}  # record id -> vector
        
        self._restore()
    
    def _restore(self):
        """Restore state from snapshot and log."""
        # Load snapshot
        if os.path.exists(self.snapshot_path):
            try:
                with open(self.snapshot_path, "r") as handle:
                    self.records = json.load(handle)
            except:
                self.records = {}
        
        # Replay log
        for record in self.txlog.replay():
            act = record["action"]
            payload = record["payload"]
            if act == "put":
                self.records[payload["id"]] = payload["content"]
            elif act == "remove":
                self.records.pop(payload["id"], None)
            elif act == "batch_put":
                for rid, content in payload["entries"]:
                    self.records[rid] = content
        
        # Rebuild search structures
        self._rebuild_search()
        
        # Create snapshot
        self._create_snapshot()
    
    def _rebuild_search(self):
        """Rebuild search structures from records."""
        self.word_map = {}
        for rid, content in self.records.items():
            self._add_to_search(rid, content)
    
    def _add_to_search(self, rid, content):
        """Add content to search structures."""
        if isinstance(content, str):
            terms = content.lower().split()
            for term in terms:
                if term not in self.word_map:
                    self.word_map[term] = set()
                self.word_map[term].add(rid)
    
    def _drop_from_search(self, rid):
        """Remove record from search structures."""
        for term in list(self.word_map.keys()):
            self.word_map[term].discard(rid)
            if not self.word_map[term]:
                del self.word_map[term]
    
    def _create_snapshot(self):
        """Save current state and clear log."""
        with self.mutex:
            with open(self.snapshot_path, "w") as handle:
                json.dump(self.records, handle)
                handle.flush()
                os.fsync(handle.fileno())
            self.txlog.truncate()
    
    def _persist(self, simulate_issue=False):
        """Persist records to disk with optional simulated failure."""
        if simulate_issue:
            if random.random() < 0.01:
                return False
        with open(self.snapshot_path, "w") as handle:
            json.dump(self.records, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return True
    
    def fetch(self, rid):
        """Fetch content by record id."""
        with self.mutex:
            return self.records.get(rid)
    
    def put(self, rid, content, simulate_issue=False):
        """Put record with id and content."""
        with self.mutex:
            # Log first (always synchronous)
            self.txlog.record("put", {"id": rid, "content": content})
            
            # Update memory
            existing = self.records.get(rid)
            self.records[rid] = content
            
            # Update search
            if existing is not None:
                self._drop_from_search(rid)
            self._add_to_search(rid, content)
            
            # Persist
            self._persist(simulate_issue)
            
            return True
    
    def remove(self, rid, simulate_issue=False):
        """Remove record by id."""
        with self.mutex:
            if rid in self.records:
                # Log first
                self.txlog.record("remove", {"id": rid})
                
                # Update search
                self._drop_from_search(rid)
                
                # Update memory
                del self.records[rid]
                
                # Persist
                self._persist(simulate_issue)
                
                return True
            return False
    
    def batch_put(self, entries, simulate_issue=False):
        """Batch put multiple records atomically."""
        with self.mutex:
            # Log first (atomic operation)
            self.txlog.record("batch_put", {"entries": entries})
            
            # Update memory
            for rid, content in entries:
                existing = self.records.get(rid)
                if existing is not None:
                    self._drop_from_search(rid)
                self.records[rid] = content
                self._add_to_search(rid, content)
            
            # Persist
            self._persist(simulate_issue)
            
            return True
    
    def query(self, terms):
        """Query records by terms (AND search)."""
        term_list = terms.lower().split()
        if not term_list:
            return []
        
        # Find records containing all terms
        matching = None
        for term in term_list:
            rids = self.word_map.get(term, set())
            if matching is None:
                matching = rids.copy()
            else:
                matching &= rids
        
        if matching is None:
            return []
        
        return [(rid, self.records[rid]) for rid in matching if rid in self.records]
    
    def list_ids(self):
        """List all record ids."""
        with self.mutex:
            return list(self.records.keys())


# Global instance
storage = None


def get_storage():
    global storage
    if storage is None:
        data_folder = os.environ.get("DATASTORE_FOLDER", ".")
        instance_id = os.environ.get("DATASTORE_INSTANCE", "primary")
        snapshot_path = os.path.join(data_folder, f"snapshot_{instance_id}.json")
        log_path = os.path.join(data_folder, f"txlog_{instance_id}.dat")
        storage = DataStore(snapshot_path, log_path, instance_id)
    return storage


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
    
    get_storage().put(rid, content, simulate_issue)
    return jsonify({"ok": True, "id": rid})


@webapp.route("/remove/<rid>", methods=["DELETE"])
def api_remove(rid):
    """Remove record."""
    simulate_issue = request.args.get("simulate_issue", "false").lower() == "true"
    ok = get_storage().remove(rid, simulate_issue)
    if not ok:
        return jsonify({"err": "Record not found"}), 404
    return jsonify({"ok": True, "id": rid})


@webapp.route("/batch_put", methods=["POST"])
def api_batch_put():
    """Batch put multiple records."""
    body = request.get_json()
    entries = body.get("entries", [])
    simulate_issue = body.get("simulate_issue", False)
    
    if not entries:
        return jsonify({"err": "No entries provided"}), 400
    
    get_storage().batch_put(entries, simulate_issue)
    return jsonify({"ok": True, "count": len(entries)})


@webapp.route("/query", methods=["GET"])
def api_query():
    """Query records by terms."""
    terms = request.args.get("terms", "")
    matches = get_storage().query(terms)
    return jsonify({"matches": matches})


@webapp.route("/ids", methods=["GET"])
def api_ids():
    """List all record ids."""
    ids = get_storage().list_ids()
    return jsonify({"ids": ids})


@webapp.route("/status", methods=["GET"])
def api_status():
    """Status check endpoint."""
    return jsonify({"status": "running", "instance": get_storage().instance_id})


if __name__ == "__main__":
    import sys
    port_num = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    webapp.run(host="0.0.0.0", port=port_num, threaded=True)
