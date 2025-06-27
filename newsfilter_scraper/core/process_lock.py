# Single instance enforcement to prevent multiple scraper processes

import os
import sys
import time
import logging
import signal
from pathlib import Path
from typing import Optional


class ProcessLockError(Exception):
    """Custom exception for process lock related errors"""
    pass


class ProcessLock:
    """Manages process locking to ensure only one scraper instance runs at a time"""
    
    def __init__(self, lock_file: str = "/tmp/newsfilter_scraper.lock", 
                 timeout: int = 300, check_interval: int = 5):
        """
        Initialize the process lock
        
        Args:
            lock_file: Path to the lock file
            timeout: Maximum time to wait for lock acquisition (seconds)
            check_interval: How often to check for lock availability (seconds)
        """
        self.lock_file = Path(lock_file)
        self.timeout = timeout
        self.check_interval = check_interval
        self.pid = os.getpid()
        
        self.logger = logging.getLogger(__name__)
        self._locked = False
        
        # Ensure lock directory exists
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
    
    def acquire(self, wait: bool = False) -> bool:
        """
        Acquire the process lock
        
        Args:
            wait: Whether to wait for lock if not immediately available
        
        Returns:
            bool: True if lock acquired successfully
        """
        if self._locked:
            self.logger.warning("Lock already acquired by this process")
            return True
        
        start_time = time.time()
        
        while True:
            try:
                # Check if lock file exists
                if self.lock_file.exists():
                    if not self._is_process_running():
                        # Stale lock file - remove it
                        self._remove_stale_lock()
                    else:
                        # Active lock exists
                        if not wait:
                            self.logger.info("Another scraper instance is already running")
                            return False
                        
                        # Check timeout
                        if time.time() - start_time >= self.timeout:
                            self.logger.error(f"Timeout waiting for lock after {self.timeout} seconds")
                            return False
                        
                        self.logger.info(f"Waiting for lock... (PID: {self._get_lock_pid()})")
                        time.sleep(self.check_interval)
                        continue
                
                # Create lock file
                self._create_lock_file()
                self._locked = True
                self.logger.info(f"Process lock acquired (PID: {self.pid})")
                return True
                
            except OSError as e:
                self.logger.error(f"Error acquiring lock: {e}")
                return False
    
    def release(self):
        """Release the process lock"""
        if not self._locked:
            self.logger.warning("Attempting to release lock that wasn't acquired")
            return
        
        try:
            if self.lock_file.exists():
                # Verify this is our lock file
                if self._get_lock_pid() == self.pid:
                    self.lock_file.unlink()
                    self.logger.info(f"Process lock released (PID: {self.pid})")
                else:
                    self.logger.warning("Lock file PID doesn't match current process")
            
            self._locked = False
            
        except OSError as e:
            self.logger.error(f"Error releasing lock: {e}")
    
    def _create_lock_file(self):
        """Create the lock file with current process information"""
        lock_data = {
            'pid': self.pid,
            'timestamp': time.time(),
            'command': ' '.join(sys.argv)
        }
        
        try:
            # Use atomic write operation
            temp_file = self.lock_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                f.write(f"{lock_data['pid']}\n")
                f.write(f"{lock_data['timestamp']}\n")
                f.write(f"{lock_data['command']}\n")
            
            # Atomic rename
            temp_file.rename(self.lock_file)
            
        except OSError as e:
            raise ProcessLockError(f"Could not create lock file: {e}")
    
    def _get_lock_pid(self) -> Optional[int]:
        """Get the PID from the lock file"""
        try:
            if not self.lock_file.exists():
                return None
            
            with open(self.lock_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    return int(lines[0].strip())
            
            return None
            
        except (OSError, ValueError) as e:
            self.logger.warning(f"Could not read PID from lock file: {e}")
            return None
    
    def _is_process_running(self) -> bool:
        """Check if the process that created the lock is still running"""
        lock_pid = self._get_lock_pid()
        
        if lock_pid is None:
            return False
        
        try:
            # Send signal 0 to check if process exists
            os.kill(lock_pid, 0)
            return True
        except OSError:
            # Process doesn't exist
            return False
    
    def _remove_stale_lock(self):
        """Remove a stale lock file"""
        try:
            lock_pid = self._get_lock_pid()
            self.lock_file.unlink()
            self.logger.info(f"Removed stale lock file (PID: {lock_pid})")
        except OSError as e:
            self.logger.warning(f"Could not remove stale lock file: {e}")
    
    def get_lock_info(self) -> Optional[dict]:
        """Get information about the current lock"""
        if not self.lock_file.exists():
            return None
        
        try:
            with open(self.lock_file, 'r') as f:
                lines = f.readlines()
            
            if len(lines) >= 3:
                return {
                    'pid': int(lines[0].strip()),
                    'timestamp': float(lines[1].strip()),
                    'command': lines[2].strip(),
                    'running': self._is_process_running()
                }
            
            return None
            
        except (OSError, ValueError) as e:
            self.logger.warning(f"Could not read lock info: {e}")
            return None
    
    def is_locked(self) -> bool:
        """Check if a valid lock exists"""
        return self.lock_file.exists() and self._is_process_running()
    
    def force_release(self):
        """Force release of the lock (use with extreme caution)"""
        try:
            if self.lock_file.exists():
                lock_info = self.get_lock_info()
                self.lock_file.unlink()
                self.logger.warning(f"Force released lock file (was PID: {lock_info.get('pid') if lock_info else 'unknown'})")
        except OSError as e:
            self.logger.error(f"Could not force release lock: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        if not self.acquire():
            raise ProcessLockError("Could not acquire process lock")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.release()
    
    def __del__(self):
        """Cleanup lock on object destruction"""
        if self._locked:
            self.release()