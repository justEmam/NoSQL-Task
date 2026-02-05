"""
DataStore Connector - Python connector for the Persistent Data Store.
"""
import requests
from typing import List, Tuple, Optional, Any


class DataStoreConnector:
    """Connector for the Persistent Data Store."""
    
    def __init__(self, host: str = "localhost", port: int = 5000, wait_time: int = 10):
        self.endpoint = f"http://{host}:{port}"
        self.wait_time = wait_time
    
    def fetch(self, rid: str) -> Optional[Any]:
        """Fetch content by record id."""
        try:
            resp = requests.get(
                f"{self.endpoint}/fetch/{rid}",
                timeout=self.wait_time
            )
            if resp.status_code == 200:
                return resp.json().get("content")
            return None
        except requests.RequestException:
            return None
    
    def put(self, rid: str, content: Any, simulate_issue: bool = False) -> bool:
        """Put record with id and content."""
        try:
            resp = requests.post(
                f"{self.endpoint}/put",
                json={"id": rid, "content": content, "simulate_issue": simulate_issue},
                timeout=self.wait_time
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False
    
    def remove(self, rid: str, simulate_issue: bool = False) -> bool:
        """Remove record by id."""
        try:
            resp = requests.delete(
                f"{self.endpoint}/remove/{rid}",
                params={"simulate_issue": str(simulate_issue).lower()},
                timeout=self.wait_time
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False
    
    def batch_put(self, entries: List[Tuple[str, Any]], simulate_issue: bool = False) -> bool:
        """Batch put multiple records."""
        try:
            resp = requests.post(
                f"{self.endpoint}/batch_put",
                json={"entries": entries, "simulate_issue": simulate_issue},
                timeout=self.wait_time
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False
    
    def query(self, terms: str) -> List[Tuple[str, Any]]:
        """Query records by terms."""
        try:
            resp = requests.get(
                f"{self.endpoint}/query",
                params={"terms": terms},
                timeout=self.wait_time
            )
            if resp.status_code == 200:
                return resp.json().get("matches", [])
            return []
        except requests.RequestException:
            return []
    
    def list_ids(self) -> List[str]:
        """List all record ids."""
        try:
            resp = requests.get(
                f"{self.endpoint}/ids",
                timeout=self.wait_time
            )
            if resp.status_code == 200:
                return resp.json().get("ids", [])
            return []
        except requests.RequestException:
            return []
    
    def is_running(self) -> bool:
        """Check if service is running."""
        try:
            resp = requests.get(
                f"{self.endpoint}/status",
                timeout=self.wait_time
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False


class DistributedConnector:
    """Connector for distributed DataStore with automatic failover."""
    
    def __init__(self, instances: List[Tuple[str, int]], wait_time: int = 5):
        self.instances = instances
        self.wait_time = wait_time
        self.connectors = [DataStoreConnector(host, port, wait_time) for host, port in instances]
        self.active_idx = 0
    
    def _find_leader(self) -> Optional[DataStoreConnector]:
        """Find the current leader instance."""
        for idx, conn in enumerate(self.connectors):
            try:
                resp = requests.get(
                    f"{conn.endpoint}/cluster/info",
                    timeout=self.wait_time
                )
                if resp.status_code == 200:
                    info = resp.json()
                    if info.get("is_leader"):
                        self.active_idx = idx
                        return conn
            except:
                continue
        return self.connectors[self.active_idx] if self.connectors else None
    
    def fetch(self, rid: str) -> Optional[Any]:
        """Fetch content (from leader)."""
        conn = self._find_leader()
        return conn.fetch(rid) if conn else None
    
    def put(self, rid: str, content: Any, simulate_issue: bool = False) -> bool:
        """Put record (on leader)."""
        conn = self._find_leader()
        return conn.put(rid, content, simulate_issue) if conn else False
    
    def remove(self, rid: str, simulate_issue: bool = False) -> bool:
        """Remove record (on leader)."""
        conn = self._find_leader()
        return conn.remove(rid, simulate_issue) if conn else False
    
    def batch_put(self, entries: List[Tuple[str, Any]], simulate_issue: bool = False) -> bool:
        """Batch put (on leader)."""
        conn = self._find_leader()
        return conn.batch_put(entries, simulate_issue) if conn else False
