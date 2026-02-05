"""
Main entry point for tests and performance measurements.
"""
import subprocess
import sys
import os


def execute_tests():
    """Execute all tests."""
    print("\n" + "="*60)
    print("Executing Data Store Tests")
    print("="*60)
    
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "test_datastore.py", "-v", "--tb=short", "-x"],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    
    if result.returncode != 0:
        print("Basic tests failed!")
        return False
    
    print("\n" + "="*60)
    print("Executing Distributed Tests")
    print("="*60)
    
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "test_distributed.py", "-v", "--tb=short", "-x"],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    
    if result.returncode != 0:
        print("Distributed tests failed!")
        return False
    
    return True


def execute_performance():
    """Execute performance measurements."""
    print("\n" + "="*60)
    print("Executing Performance Measurements")
    print("="*60)
    
    result = subprocess.run(
        [sys.executable, "performance.py"],
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    
    return result.returncode == 0


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Store Test Executor")
    parser.add_argument("--tests", action="store_true", help="Execute tests")
    parser.add_argument("--performance", action="store_true", help="Execute performance measurements")
    parser.add_argument("--all", action="store_true", help="Execute tests and performance measurements")
    
    args = parser.parse_args()
    
    if not any([args.tests, args.performance, args.all]):
        args.all = True
    
    success = True
    
    if args.tests or args.all:
        success = execute_tests() and success
    
    if args.performance or args.all:
        success = execute_performance() and success
    
    if success:
        print("\n" + "="*60)
        print("All operations completed successfully!")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("Some operations failed!")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
