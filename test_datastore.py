"""
Tests for the Persistent Data Store.
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
import random

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connector import DataStoreConnector


class ServiceManager:
    """Helper class to manage test service instances."""
    
    def __init__(self, port=5000, data_folder=None):
        self.port = port
        self.data_folder = data_folder or tempfile.mkdtemp()
        self.proc = None
        self.instance_id = f"test_{port}"
    
    def launch(self):
        """Launch the service."""
        environ = os.environ.copy()
        environ["DATASTORE_FOLDER"] = self.data_folder
        environ["DATASTORE_INSTANCE"] = self.instance_id
        
        self.proc = subprocess.Popen(
            [sys.executable, "datastore.py", str(self.port)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env=environ,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        )
        
        # Wait for service to be ready
        conn = DataStoreConnector(port=self.port)
        for _ in range(50):
            if conn.is_running():
                return True
            time.sleep(0.1)
        return False
    
    def shutdown(self):
        """Shutdown the service gracefully."""
        if self.proc:
            if os.name == 'nt':
                self.proc.terminate()
            else:
                self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=5)
            self.proc = None
    
    def terminate(self):
        """Terminate the service forcefully (SIGKILL)."""
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
    
    def purge(self):
        """Purge data folder."""
        if os.path.exists(self.data_folder):
            shutil.rmtree(self.data_folder)


@pytest.fixture
def service():
    """Fixture that provides a running service."""
    mgr = ServiceManager(port=5001)
    mgr.launch()
    yield mgr
    mgr.shutdown()
    mgr.purge()


@pytest.fixture
def connector(service):
    """Fixture that provides a connector to the test service."""
    return DataStoreConnector(port=service.port)


class TestCoreOperations:
    """Test core CRUD operations."""
    
    def test_put_then_fetch(self, connector):
        """Test: Put then Fetch."""
        assert connector.put("rec1", "data1")
        assert connector.fetch("rec1") == "data1"
    
    def test_put_then_remove_then_fetch(self, connector):
        """Test: Put then Remove then Fetch."""
        assert connector.put("rec2", "data2")
        assert connector.fetch("rec2") == "data2"
        assert connector.remove("rec2")
        assert connector.fetch("rec2") is None
    
    def test_fetch_nonexistent(self, connector):
        """Test: Fetch without putting."""
        assert connector.fetch("missing_record") is None
    
    def test_put_overwrite_then_fetch(self, connector):
        """Test: Put then Put (same id) then Fetch."""
        assert connector.put("rec3", "data3")
        assert connector.fetch("rec3") == "data3"
        assert connector.put("rec3", "updated_data3")
        assert connector.fetch("rec3") == "updated_data3"
    
    def test_batch_put(self, connector):
        """Test: Batch put multiple records."""
        entries = [("batch1", "val1"), ("batch2", "val2"), ("batch3", "val3")]
        assert connector.batch_put(entries)
        assert connector.fetch("batch1") == "val1"
        assert connector.fetch("batch2") == "val2"
        assert connector.fetch("batch3") == "val3"
    
    def test_various_content_types(self, connector):
        """Test: Various content types."""
        assert connector.put("num_rec", 42)
        assert connector.fetch("num_rec") == 42
        
        assert connector.put("arr_rec", [1, 2, 3])
        assert connector.fetch("arr_rec") == [1, 2, 3]
        
        assert connector.put("obj_rec", {"inner": "data"})
        assert connector.fetch("obj_rec") == {"inner": "data"}


class TestDataSafety:
    """Test data safety across restarts."""
    
    def test_put_then_restart_then_fetch(self):
        """Test: Put then exit (gracefully) then Fetch."""
        data_folder = tempfile.mkdtemp()
        try:
            # Launch service and put data
            mgr = ServiceManager(port=5002, data_folder=data_folder)
            mgr.launch()
            conn = DataStoreConnector(port=5002)
            
            assert conn.put("safe_rec", "safe_data")
            assert conn.fetch("safe_rec") == "safe_data"
            
            # Shutdown gracefully
            mgr.shutdown()
            time.sleep(0.5)
            
            # Relaunch service
            mgr.launch()
            conn = DataStoreConnector(port=5002)
            
            # Verify data persisted
            assert conn.fetch("safe_rec") == "safe_data"
            
            mgr.shutdown()
        finally:
            shutil.rmtree(data_folder)
    
    def test_batch_put_safety(self):
        """Test: Batch put then restart then Fetch."""
        data_folder = tempfile.mkdtemp()
        try:
            mgr = ServiceManager(port=5003, data_folder=data_folder)
            mgr.launch()
            conn = DataStoreConnector(port=5003)
            
            entries = [("sp1", "v1"), ("sp2", "v2"), ("sp3", "v3")]
            assert conn.batch_put(entries)
            
            mgr.shutdown()
            time.sleep(0.5)
            mgr.launch()
            
            conn = DataStoreConnector(port=5003)
            assert conn.fetch("sp1") == "v1"
            assert conn.fetch("sp2") == "v2"
            assert conn.fetch("sp3") == "v3"
            
            mgr.shutdown()
        finally:
            shutil.rmtree(data_folder)


class TestAtomicity:
    """Test atomic operations."""
    
    def test_concurrent_batch_writes_same_ids(self):
        """Test: Concurrent batch put writes touching the same ids."""
        data_folder = tempfile.mkdtemp()
        try:
            mgr = ServiceManager(port=5016, data_folder=data_folder)
            assert mgr.launch(), "Service failed to launch"
            
            outcomes = []
            issues = []
            
            def writer_task(writer_num):
                conn = DataStoreConnector(port=5016)
                try:
                    entries = [(f"shared_{i}", f"writer{writer_num}_data{i}") 
                             for i in range(10)]
                    ok = conn.batch_put(entries)
                    outcomes.append((writer_num, ok))
                except Exception as ex:
                    issues.append((writer_num, str(ex)))
            
            # Run 5 concurrent writers
            workers = [threading.Thread(target=writer_task, args=(i,)) for i in range(5)]
            for w in workers:
                w.start()
            for w in workers:
                w.join()
            
            # All writes should succeed
            assert len(issues) == 0
            assert all(o[1] for o in outcomes)
            
            # Contents should be consistent
            conn = DataStoreConnector(port=5016)
            contents = [conn.fetch(f"shared_{i}") for i in range(10)]
            
            # Each content should be valid
            for c in contents:
                assert c is not None
                assert "data" in c
            
            mgr.shutdown()
        finally:
            shutil.rmtree(data_folder)
    
    def test_batch_write_recovery_on_crash(self):
        """Test: Batch writes with terminate - Transaction log ensures recovery."""
        data_folder = tempfile.mkdtemp()
        try:
            mgr = ServiceManager(port=5015, data_folder=data_folder)
            assert mgr.launch(), "Service failed to launch"
            conn = DataStoreConnector(port=5015)
            
            # Do a successful batch write first
            entries = [(f"atomic_{i}", f"data_{i}") for i in range(20)]
            assert conn.batch_put(entries)
            
            # Verify all written
            for i in range(20):
                val = conn.fetch(f"atomic_{i}")
                assert val == f"data_{i}", f"Missing atomic_{i}"
            
            # Terminate and relaunch - data should persist
            mgr.terminate()
            time.sleep(0.5)
            assert mgr.launch(), "Service failed to relaunch"
            conn = DataStoreConnector(port=5015)
            
            # Data should be recovered from transaction log
            recovered_count = 0
            for i in range(20):
                val = conn.fetch(f"atomic_{i}")
                if val == f"data_{i}":
                    recovered_count += 1
            
            # All acknowledged writes should survive
            assert recovered_count == 20, f"Only {recovered_count}/20 records recovered"
            
            mgr.shutdown()
        finally:
            shutil.rmtree(data_folder)


class TestTextSearch:
    """Test text search functionality."""
    
    def test_simple_query(self, connector):
        """Test: Simple text query."""
        connector.put("doc1", "hello world python")
        connector.put("doc2", "hello flask web")
        connector.put("doc3", "goodbye world java")
        
        matches = connector.query("hello")
        ids = [m[0] for m in matches]
        assert "doc1" in ids
        assert "doc2" in ids
        assert "doc3" not in ids
    
    def test_multi_term_query(self, connector):
        """Test: Multi-term query (AND)."""
        connector.put("doc1", "hello world python")
        connector.put("doc2", "hello flask web")
        
        matches = connector.query("hello world")
        ids = [m[0] for m in matches]
        assert "doc1" in ids
        assert "doc2" not in ids


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
