"""
Performance Measurements for the Persistent Data Store.
"""
import os
import sys
import time
import signal
import subprocess
import tempfile
import shutil
import threading
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import DataStoreConnector


class ServiceManager:
    """Helper class to manage benchmark service instances."""
    
    def __init__(self, port=5000, data_folder=None):
        self.port = port
        self.data_folder = data_folder or tempfile.mkdtemp()
        self.proc = None
        self.instance_id = f"perf_{port}"
    
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
        
        conn = DataStoreConnector(port=self.port)
        for _ in range(50):
            if conn.is_running():
                return True
            time.sleep(0.1)
        return False
    
    def shutdown(self):
        """Shutdown service gracefully."""
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
        """Terminate service forcefully."""
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
            shutil.rmtree(self.data_folder, ignore_errors=True)


def measure_write_rate():
    """Measure write rate with different data sizes."""
    print("\n" + "="*60)
    print("MEASUREMENT: Write Rate")
    print("="*60)
    
    data_folder = tempfile.mkdtemp()
    results = []
    
    try:
        for prepopulated in [0, 100, 500, 1000]:
            mgr = ServiceManager(port=5010, data_folder=data_folder)
            mgr.launch()
            conn = DataStoreConnector(port=5010)
            
            # Prepopulate data
            if prepopulated > 0:
                entries = [(f"pre_{i}", f"data_{i}" * 10) for i in range(prepopulated)]
                conn.batch_put(entries)
            
            # Measure writes
            num_writes = 100
            start_ts = time.time()
            
            for i in range(num_writes):
                conn.put(f"perf_rec_{i}", f"perf_data_{i}" * 10)
            
            elapsed = time.time() - start_ts
            rate = num_writes / elapsed
            
            results.append({
                "prepopulated": prepopulated,
                "writes": num_writes,
                "elapsed": elapsed,
                "rate": rate
            })
            
            print(f"Prepopulated: {prepopulated:5d} | "
                  f"Writes: {num_writes} | "
                  f"Time: {elapsed:.3f}s | "
                  f"Rate: {rate:.1f} writes/sec")
            
            mgr.shutdown()
            
            # Clear data for next run
            for f in os.listdir(data_folder):
                os.remove(os.path.join(data_folder, f))
    finally:
        shutil.rmtree(data_folder, ignore_errors=True)
    
    return results


def measure_batch_write_rate():
    """Measure batch write rate."""
    print("\n" + "="*60)
    print("MEASUREMENT: Batch Write Rate")
    print("="*60)
    
    data_folder = tempfile.mkdtemp()
    results = []
    
    try:
        for batch_size in [10, 50, 100]:
            mgr = ServiceManager(port=5011, data_folder=data_folder)
            mgr.launch()
            conn = DataStoreConnector(port=5011)
            
            num_batches = 20
            total_writes = num_batches * batch_size
            
            start_ts = time.time()
            
            for batch_num in range(num_batches):
                entries = [(f"batch_{batch_num}_{i}", f"data_{i}" * 10) 
                         for i in range(batch_size)]
                conn.batch_put(entries)
            
            elapsed = time.time() - start_ts
            rate = total_writes / elapsed
            
            results.append({
                "batch_size": batch_size,
                "total_writes": total_writes,
                "elapsed": elapsed,
                "rate": rate
            })
            
            print(f"Batch Size: {batch_size:3d} | "
                  f"Total: {total_writes:5d} | "
                  f"Time: {elapsed:.3f}s | "
                  f"Rate: {rate:.1f} writes/sec")
            
            mgr.shutdown()
            
            for f in os.listdir(data_folder):
                os.remove(os.path.join(data_folder, f))
    finally:
        shutil.rmtree(data_folder, ignore_errors=True)
    
    return results


def measure_read_rate():
    """Measure read rate."""
    print("\n" + "="*60)
    print("MEASUREMENT: Read Rate")
    print("="*60)
    
    data_folder = tempfile.mkdtemp()
    
    try:
        mgr = ServiceManager(port=5012, data_folder=data_folder)
        mgr.launch()
        conn = DataStoreConnector(port=5012)
        
        # Prepopulate
        entries = [(f"read_{i}", f"data_{i}" * 10) for i in range(500)]
        conn.batch_put(entries)
        
        # Measure reads
        num_reads = 500
        start_ts = time.time()
        
        for i in range(num_reads):
            conn.fetch(f"read_{i % 500}")
        
        elapsed = time.time() - start_ts
        rate = num_reads / elapsed
        
        print(f"Reads: {num_reads} | "
              f"Time: {elapsed:.3f}s | "
              f"Rate: {rate:.1f} reads/sec")
        
        mgr.shutdown()
    finally:
        shutil.rmtree(data_folder, ignore_errors=True)


def measure_durability():
    """Measure durability under random crashes."""
    print("\n" + "="*60)
    print("MEASUREMENT: Durability (Crash Recovery)")
    print("="*60)
    
    data_folder = tempfile.mkdtemp()
    
    confirmed_ids = set()
    mutex = threading.Lock()
    halt_flag = threading.Event()
    
    try:
        mgr = ServiceManager(port=5013, data_folder=data_folder)
        mgr.launch()
        
        def writer_task():
            """Task that writes data and tracks confirmed ids."""
            conn = DataStoreConnector(port=5013, wait_time=2)
            counter = 0
            
            while not halt_flag.is_set():
                rid = f"durable_{counter}"
                content = f"data_{counter}"
                
                try:
                    if conn.put(rid, content):
                        with mutex:
                            confirmed_ids.add(rid)
                        counter += 1
                except:
                    time.sleep(0.1)
        
        def terminator_task():
            """Task that randomly terminates the service."""
            nonlocal mgr
            
            for _ in range(3):
                time.sleep(random.uniform(0.3, 0.5))
                
                if halt_flag.is_set():
                    break
                
                print("  [Terminator] Terminating service...")
                mgr.terminate()
                time.sleep(0.2)
                
                print("  [Terminator] Relaunching service...")
                mgr = ServiceManager(port=5013, data_folder=data_folder)
                mgr.launch()
        
        # Start tasks
        writer = threading.Thread(target=writer_task)
        terminator = threading.Thread(target=terminator_task)
        
        writer.start()
        terminator.start()
        
        # Let it run
        terminator.join(timeout=5)
        halt_flag.set()
        writer.join(timeout=2)
        
        # Wait for service to be stable
        time.sleep(0.5)
        if mgr.proc is None or mgr.proc.poll() is not None:
            mgr = ServiceManager(port=5013, data_folder=data_folder)
            mgr.launch()
        
        # Check which confirmed ids survived
        conn = DataStoreConnector(port=5013, wait_time=5)
        
        survived_ids = set()
        lost_ids = set()
        
        with mutex:
            all_confirmed = confirmed_ids.copy()
        
        for rid in all_confirmed:
            content = conn.fetch(rid)
            if content is not None:
                survived_ids.add(rid)
            else:
                lost_ids.add(rid)
        
        total_confirmed = len(all_confirmed)
        total_survived = len(survived_ids)
        total_lost = len(lost_ids)
        
        durability_pct = (total_survived / total_confirmed * 100) if total_confirmed > 0 else 100
        
        print(f"\nDurability Results:")
        print(f"  Confirmed writes: {total_confirmed}")
        print(f"  Survived after crashes: {total_survived}")
        print(f"  Lost records: {total_lost}")
        print(f"  Durability: {durability_pct:.2f}%")
        
        mgr.shutdown()
        
        return {
            "confirmed": total_confirmed,
            "survived": total_survived,
            "lost": total_lost,
            "durability_pct": durability_pct
        }
    finally:
        shutil.rmtree(data_folder, ignore_errors=True)


def measure_concurrent_writes():
    """Measure concurrent write performance."""
    print("\n" + "="*60)
    print("MEASUREMENT: Concurrent Writes")
    print("="*60)
    
    data_folder = tempfile.mkdtemp()
    
    try:
        mgr = ServiceManager(port=5014, data_folder=data_folder)
        mgr.launch()
        
        for num_workers in [1, 2, 4]:
            writes_per_worker = 50
            total_writes = num_workers * writes_per_worker
            
            def worker_task(worker_num):
                conn = DataStoreConnector(port=5014)
                for i in range(writes_per_worker):
                    conn.put(f"concurrent_{worker_num}_{i}", f"data_{i}")
            
            start_ts = time.time()
            
            workers = [threading.Thread(target=worker_task, args=(i,)) 
                      for i in range(num_workers)]
            for w in workers:
                w.start()
            for w in workers:
                w.join()
            
            elapsed = time.time() - start_ts
            rate = total_writes / elapsed
            
            print(f"Workers: {num_workers} | "
                  f"Total Writes: {total_writes} | "
                  f"Time: {elapsed:.3f}s | "
                  f"Rate: {rate:.1f} writes/sec")
        
        mgr.shutdown()
    finally:
        shutil.rmtree(data_folder, ignore_errors=True)


def run_all_measurements():
    """Run all performance measurements."""
    print("\n" + "#"*60)
    print("# Data Store Performance Measurements")
    print("#"*60)
    
    measure_write_rate()
    measure_batch_write_rate()
    measure_read_rate()
    measure_concurrent_writes()
    measure_durability()
    
    print("\n" + "#"*60)
    print("# Measurements Complete")
    print("#"*60)


if __name__ == "__main__":
    run_all_measurements()
