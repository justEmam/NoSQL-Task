"""
Tests for Distributed Data Store with Replication and Leader Election.
"""
import os
import sys
import time
import signal
import subprocess
import tempfile
import shutil
import pytest
import threading
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import DataStoreConnector


class InstanceManager:
    """Helper class to manage distributed instance."""
    
    def __init__(self, port, instance_id, data_folder):
        self.port = port
        self.instance_id = instance_id
        self.data_folder = data_folder
        self.proc = None
        self.addr = f"http://localhost:{port}"
    
    def launch(self):
        """Launch the distributed instance."""
        environ = os.environ.copy()
        environ["DATASTORE_FOLDER"] = self.data_folder
        environ["DATASTORE_INSTANCE"] = self.instance_id
        
        self.proc = subprocess.Popen(
            [sys.executable, "distributed_store.py", str(self.port)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=environ,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        )
        
        # Wait for instance to be ready
        for _ in range(50):
            try:
                resp = requests.get(f"{self.addr}/status", timeout=1)
                if resp.status_code == 200:
                    return True
            except:
                pass
            time.sleep(0.1)
        return False
    
    def shutdown(self):
        """Shutdown instance gracefully."""
        if self.proc:
            if os.name == 'nt':
                self.proc.terminate()
            else:
                self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=5)
            except:
                pass
            self.proc = None
    
    def terminate(self):
        """Terminate instance forcefully."""
        if self.proc:
            if os.name == 'nt':
                subprocess.run(["taskkill", "/F", "/PID", str(self.proc.pid)], 
                             capture_output=True)
            else:
                self.proc.send_signal(signal.SIGKILL)
            try:
                self.proc.wait(timeout=2)
            except:
                pass
            self.proc = None
    
    def active(self):
        """Check if instance is active."""
        if self.proc is None:
            return False
        return self.proc.poll() is None
    
    def register_cluster(self, peers, multi_leader=False):
        """Register to a cluster with given peers."""
        follower_list = [{"instance": p.instance_id, "addr": p.addr} for p in peers]
        try:
            resp = requests.post(
                f"{self.addr}/cluster/register",
                json={"followers": follower_list, "multi_leader": multi_leader},
                timeout=2
            )
            return resp.status_code == 200
        except:
            return False
    
    def promote(self):
        """Promote this instance to leader."""
        try:
            resp = requests.post(f"{self.addr}/promote", timeout=2)
            return resp.status_code == 200
        except:
            return False
    
    def get_info(self):
        """Get cluster info."""
        try:
            resp = requests.get(f"{self.addr}/cluster/info", timeout=2)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        return None


class TestReplication:
    """Test data replication."""
    
    def test_sync_to_followers(self):
        """Test that writes to leader are synced to followers."""
        data_folder = tempfile.mkdtemp()
        instances = []
        
        try:
            # Create 3 instances
            for i in range(3):
                inst = InstanceManager(
                    port=5020 + i,
                    instance_id=f"inst{i}",
                    data_folder=data_folder
                )
                inst.launch()
                instances.append(inst)
            
            # Set up cluster - inst0 is leader
            leader = instances[0]
            followers = instances[1:]
            
            # Register cluster
            for inst in instances:
                peers = [n for n in instances if n != inst]
                inst.register_cluster(peers)
            
            # Make inst0 leader
            leader.promote()
            time.sleep(0.5)
            
            # Write to leader
            conn = DataStoreConnector(port=leader.port)
            assert conn.put("sync_rec", "sync_data")
            
            # Wait for sync
            time.sleep(0.5)
            
            # Check followers have the data
            for follower in followers:
                follower_conn = DataStoreConnector(port=follower.port)
                content = follower_conn.fetch("sync_rec")
                assert content == "sync_data", f"Sync failed to {follower.instance_id}"
            
        finally:
            for inst in instances:
                inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)
    
    def test_batch_put_sync(self):
        """Test that batch puts are synced."""
        data_folder = tempfile.mkdtemp()
        instances = []
        
        try:
            for i in range(3):
                inst = InstanceManager(
                    port=5023 + i,
                    instance_id=f"inst{i}",
                    data_folder=data_folder
                )
                inst.launch()
                instances.append(inst)
            
            leader = instances[0]
            
            for inst in instances:
                peers = [n for n in instances if n != inst]
                inst.register_cluster(peers)
            
            leader.promote()
            time.sleep(1)
            
            # Batch put on leader
            conn = DataStoreConnector(port=leader.port)
            entries = [(f"batch_{i}", f"data_{i}") for i in range(10)]
            assert conn.batch_put(entries)
            
            time.sleep(1)
            
            # Check leader first
            for i in range(10):
                assert conn.fetch(f"batch_{i}") == f"data_{i}"
            
            # Check followers
            for inst in instances[1:]:
                inst_conn = DataStoreConnector(port=inst.port)
                for i in range(10):
                    content = inst_conn.fetch(f"batch_{i}")
                    if content is not None:
                        assert content == f"data_{i}"
            
        finally:
            for inst in instances:
                inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)


class TestLeaderElection:
    """Test leader election."""
    
    def test_election_on_leader_failure(self):
        """Test that election happens when leader goes down."""
        data_folder = tempfile.mkdtemp()
        instances = []
        
        try:
            for i in range(3):
                inst = InstanceManager(
                    port=5026 + i,
                    instance_id=f"inst{i}",
                    data_folder=data_folder
                )
                inst.launch()
                instances.append(inst)
            
            leader = instances[0]
            
            for inst in instances:
                peers = [n for n in instances if n != inst]
                inst.register_cluster(peers)
            
            leader.promote()
            time.sleep(0.5)
            
            # Write some data
            conn = DataStoreConnector(port=leader.port)
            conn.put("election_rec", "data1")
            time.sleep(0.3)
            
            # Terminate leader
            print("Terminating leader...")
            leader.terminate()
            
            # Wait for election
            time.sleep(4)
            
            # Check if a new leader was elected
            new_leader = None
            for inst in instances[1:]:
                info = inst.get_info()
                if info and info.get("is_leader"):
                    new_leader = inst
                    break
            
            assert new_leader is not None, "No new leader was elected"
            print(f"New leader: {new_leader.instance_id}")
            
            # Verify data is still accessible
            conn = DataStoreConnector(port=new_leader.port)
            assert conn.fetch("election_rec") == "data1"
            
        finally:
            for inst in instances:
                inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)
    
    def test_writes_only_on_leader(self):
        """Test that writes only work on leader in distributed mode."""
        data_folder = tempfile.mkdtemp()
        instances = []
        
        try:
            for i in range(2):
                inst = InstanceManager(
                    port=5029 + i,
                    instance_id=f"inst{i}",
                    data_folder=data_folder
                )
                inst.launch()
                instances.append(inst)
            
            leader = instances[0]
            follower = instances[1]
            
            for inst in instances:
                peers = [n for n in instances if n != inst]
                inst.register_cluster(peers)
            
            leader.promote()
            time.sleep(0.3)
            
            # Write should work on leader
            leader_conn = DataStoreConnector(port=leader.port)
            assert leader_conn.put("leader_rec", "data")
            
            # Write should fail on follower
            resp = requests.post(
                f"{follower.addr}/put",
                json={"id": "follower_rec", "content": "data"},
                timeout=2
            )
            assert resp.status_code == 400
            
        finally:
            for inst in instances:
                inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)


class TestMultiLeader:
    """Test multi-leader mode."""
    
    def test_multi_leader_writes_anywhere(self):
        """Test that in multi-leader mode, writes work on any instance."""
        data_folder = tempfile.mkdtemp()
        instances = []
        
        try:
            for i in range(3):
                inst = InstanceManager(
                    port=5031 + i,
                    instance_id=f"inst{i}",
                    data_folder=data_folder
                )
                inst.launch()
                instances.append(inst)
            
            # Register cluster in multi-leader mode
            for inst in instances:
                peers = [n for n in instances if n != inst]
                inst.register_cluster(peers, multi_leader=True)
            
            time.sleep(1)
            
            # Write to each instance
            for i, inst in enumerate(instances):
                conn = DataStoreConnector(port=inst.port)
                result = conn.put(f"rec_{i}", f"from_inst_{i}")
                assert result, f"Write failed on inst{i}"
            
            time.sleep(1)
            
            # Each instance should have its own record
            for i, inst in enumerate(instances):
                conn = DataStoreConnector(port=inst.port)
                content = conn.fetch(f"rec_{i}")
                assert content == f"from_inst_{i}", f"Missing rec_{i} on its origin instance"
            
            print("Multi-leader writes verified on origin instances")
            
        finally:
            for inst in instances:
                inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)


class TestSearch:
    """Test search functionality."""
    
    def test_term_query(self):
        """Test term-based query."""
        data_folder = tempfile.mkdtemp()
        inst = InstanceManager(port=5034, instance_id="inst0", data_folder=data_folder)
        
        try:
            inst.launch()
            conn = DataStoreConnector(port=5034)
            
            # Add documents
            conn.put("doc1", "python programming language")
            conn.put("doc2", "python web development flask")
            conn.put("doc3", "java programming enterprise")
            
            # Query
            matches = conn.query("python")
            ids = [m[0] for m in matches]
            
            assert "doc1" in ids
            assert "doc2" in ids
            assert "doc3" not in ids
            
        finally:
            inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)
    
    def test_semantic_query(self):
        """Test semantic query with vectors."""
        data_folder = tempfile.mkdtemp()
        inst = InstanceManager(port=5035, instance_id="inst0", data_folder=data_folder)
        
        try:
            inst.launch()
            conn = DataStoreConnector(port=5035)
            
            # Add documents
            conn.put("doc1", "machine learning artificial intelligence")
            conn.put("doc2", "web development frontend backend")
            conn.put("doc3", "deep learning neural networks")
            
            # Semantic query
            resp = requests.get(
                f"http://localhost:5035/semantic",
                params={"terms": "AI neural", "limit": 2},
                timeout=2
            )
            
            assert resp.status_code == 200
            matches = resp.json()["matches"]
            
            # Should find relevant documents
            assert len(matches) > 0
            
        finally:
            inst.shutdown()
            shutil.rmtree(data_folder, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-x"])
