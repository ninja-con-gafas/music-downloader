import uuid
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import threading
from datetime import datetime, timedelta
from concurrent.futures import as_completed, Future, ThreadPoolExecutor
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

class SessionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class DownloadStatus(Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class DownloadItem:
    id: str
    name: str
    url: str
    status: DownloadStatus = DownloadStatus.QUEUED
    progress: float = 0.0
    error_message: Optional[str] = None
    file_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'url': self.url,
            'status': self.status.value,
            'progress': self.progress,
            'error_message': self.error_message,
            'file_path': self.file_path,
            'metadata': self.metadata,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }

@dataclass
class DownloadSession:
    session_id: str
    name: str
    status: SessionStatus = SessionStatus.PENDING
    downloads: List[DownloadItem] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_items: int = 0
    completed_items: int = 0
    failed_items: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_download(self, item: DownloadItem) -> None:
        self.downloads.append(item)
        self.total_items = len(self.downloads)
    
    def get_progress_summary(self) -> Dict[str, Any]:
        completed = sum(1 for d in self.downloads if d.status == DownloadStatus.COMPLETED)
        failed = sum(1 for d in self.downloads if d.status == DownloadStatus.FAILED)
        downloading = sum(1 for d in self.downloads if d.status == DownloadStatus.DOWNLOADING)
        
        overall_progress = (completed + failed) / self.total_items * 100 if self.total_items > 0 else 0
        
        return {
            'session_id': self.session_id,
            'name': self.name,
            'status': self.status.value,
            'overall_progress': overall_progress,
            'total_items': self.total_items,
            'completed_items': completed,
            'failed_items': failed,
            'downloading_items': downloading,
            'queued_items': self.total_items - completed - failed - downloading,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'session_id': self.session_id,
            'name': self.name,
            'status': self.status.value,
            'downloads': [d.to_dict() for d in self.downloads],
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'total_items': self.total_items,
            'completed_items': self.completed_items,
            'failed_items': self.failed_items,
            'metadata': self.metadata
        }

class SessionManager:    
    def __init__(self, max_concurrent_sessions: int = 5, session_timeout_minutes: int = 60):
        self.sessions: Dict[str, DownloadSession] = {}
        self.active_futures: Dict[str, List[Future]] = {}
        self.session_locks: Dict[str, threading.Lock] = {}
        self.max_concurrent_sessions = max_concurrent_sessions
        self.session_timeout = timedelta(minutes=session_timeout_minutes)
        self._cleanup_lock = threading.Lock()
    
    def generate_session_id(self) -> str:
        return str(uuid.uuid4()).replace('-', '')
    
    def create_session(self, name: str, metadata: Dict[str, Any] = None) -> DownloadSession:
        session_id = self.generate_session_id()
        
        self._cleanup_expired_sessions()
        
        active_sessions = self._get_active_sessions_count()
        
        if active_sessions >= self.max_concurrent_sessions:
            raise ValueError(f"Maximum concurrent sessions ({self.max_concurrent_sessions}) reached")
        
        session = DownloadSession(
            session_id=session_id,
            name=name,
            metadata=metadata or {})
        
        self.sessions[session_id] = session
        self.session_locks[session_id] = threading.Lock()
        self.active_futures[session_id] = []
        
        return session
    
    def get_session(self, session_id: str) -> Optional[DownloadSession]:
        return self.sessions.get(session_id)
    
    def get_all_sessions(self) -> List[DownloadSession]:
        return list(self.sessions.values())
    
    def get_active_sessions(self) -> List[DownloadSession]:
        return [s for s in self.sessions.values() 
                if s.status in [SessionStatus.PENDING, SessionStatus.RUNNING]]
    
    def update_session_status(self, session_id: str, status: SessionStatus) -> None:
        if session_id in self.sessions:
            with self.session_locks[session_id]:
                self.sessions[session_id].status = status
                if status == SessionStatus.RUNNING and not self.sessions[session_id].started_at:
                    self.sessions[session_id].started_at = datetime.now()
                elif status in [SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.CANCELLED]:
                    self.sessions[session_id].completed_at = datetime.now()
    
    def update_download_item(self, session_id: str, item_id: str, 
                           status: Optional[DownloadStatus] = None,
                           progress: Optional[float] = None,
                           error_message: Optional[str] = None,
                           file_path: Optional[str] = None) -> None:
        if session_id in self.sessions:
            with self.session_locks[session_id]:
                session = self.sessions[session_id]
                for item in session.downloads:
                    if item.id == item_id:
                        if status:
                            item.status = status
                            if status == DownloadStatus.DOWNLOADING and not item.started_at:
                                item.started_at = datetime.now()
                            elif status in [DownloadStatus.COMPLETED, DownloadStatus.FAILED]:
                                item.completed_at = datetime.now()
                        if progress is not None:
                            item.progress = progress
                        if error_message is not None:
                            item.error_message = error_message
                        if file_path is not None:
                            item.file_path = file_path
                        break
                
                session.completed_items = sum(1 for download in session.downloads if download.status == DownloadStatus.COMPLETED)
                session.failed_items = sum(1 for download in session.downloads if download.status == DownloadStatus.FAILED)
    
    def cancel_session(self, session_id: str) -> bool:
        if session_id not in self.sessions:
            return False
        
        with self.session_locks[session_id]:
            session = self.sessions[session_id]
            if session.status in [SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.CANCELLED]:
                return False
            
            if session_id in self.active_futures:
                for future in self.active_futures[session_id]:
                    future.cancel()
            
            session.status = SessionStatus.CANCELLED
            session.completed_at = datetime.now()
            
            for item in session.downloads:
                if item.status in [DownloadStatus.QUEUED, DownloadStatus.DOWNLOADING]:
                    item.status = DownloadStatus.FAILED
                    item.error_message = "Session cancelled"
                    if not item.completed_at:
                        item.completed_at = datetime.now()
            
            return True
    
    def cleanup_session(self, session_id: str) -> bool:
        if session_id not in self.sessions:
            return False
        
        self.cancel_session(session_id)
        
        with self._cleanup_lock:
            self.sessions.pop(session_id, None)
            self.session_locks.pop(session_id, None)
            self.active_futures.pop(session_id, None)
        
        return True
    
    def _get_active_sessions_count(self) -> int:
        return len([s for s in self.sessions.values() 
                   if s.status in [SessionStatus.PENDING, SessionStatus.RUNNING]])
    
    def _cleanup_expired_sessions(self) -> None:
        with self._cleanup_lock:
            current_time = datetime.now()
            expired_sessions = []
            
            for session_id, session in self.sessions.items():
                session_age = current_time - session.created_at
                if session_age > self.session_timeout:
                    expired_sessions.append(session_id)
                elif (session.status in [SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.CANCELLED] and
                      session.completed_at and 
                      current_time - session.completed_at > timedelta(minutes=30)):
                    expired_sessions.append(session_id)
            
            for session_id in expired_sessions:
                self.cleanup_session(session_id)
    
    def get_session_statistics(self) -> Dict[str, Any]:
        active_count = self._get_active_sessions_count()
        total_count = len(self.sessions)
        
        return {
            'total_sessions': total_count,
            'active_sessions': active_count,
            'max_concurrent_sessions': self.max_concurrent_sessions,
            'session_timeout_minutes': self.session_timeout.total_seconds() / 60,
            'sessions_by_status': {
                status.value: len([s for s in self.sessions.values() if s.status == status])
                for status in SessionStatus
            }
        }

class SessionAwareDownloadExecutor:
    def __init__(self, session_manager: SessionManager, max_workers: int = 4):
        self.session_manager = session_manager
        self.max_workers = max_workers
    
    def execute_session_downloads(self, session_id: str, 
                                download_function: Callable,
                                max_concurrent_downloads: int = None) -> None:
        session: DownloadSession = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        
        if session.status != SessionStatus.PENDING:
            raise ValueError(f"Session {session_id} is not in pending state")
        
        self.session_manager.update_session_status(session_id, SessionStatus.RUNNING)
        
        concurrent_downloads = max_concurrent_downloads or self.max_workers
        
        ctx = get_script_run_ctx()
        
        try:
            with ThreadPoolExecutor(max_workers=concurrent_downloads) as executor:
                for thread in executor._threads:
                    try:
                        add_script_run_ctx(thread, ctx)
                    except Exception as e:
                        print(f"Failed to add ScriptRunContext to thread: {e}")
                
                futures = []
                for item in session.downloads:
                    future = executor.submit(self._download_with_session_context, 
                                           session_id, item, download_function)
                    futures.append(future)
                
                self.session_manager.active_futures[session_id] = futures
                
                completed_count = 0
                failed_count = 0
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            completed_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        failed_count += 1
                        print(f"Download task failed with exception: {e}")
                
                if failed_count == 0:
                    final_status = SessionStatus.COMPLETED
                elif completed_count == 0:
                    final_status = SessionStatus.FAILED
                else:
                    final_status = SessionStatus.COMPLETED
                
                self.session_manager.update_session_status(session_id, final_status)
        
        except Exception as e:
            self.session_manager.update_session_status(session_id, SessionStatus.FAILED)
            raise e
        finally:
            if session_id in self.session_manager.active_futures:
                del self.session_manager.active_futures[session_id]
    
    def _download_with_session_context(self, session_id: str, item: DownloadItem, 
                                     download_function: Callable) -> bool:
        try:
            self.session_manager.update_download_item(
                session_id, item.id, 
                status=DownloadStatus.DOWNLOADING)
            
            success = download_function(item, 
                                      progress_callback=lambda progress: self._progress_callback(session_id, item.id, progress),
                                      error_callback=lambda error: self._error_callback(session_id, item.id, error),
                                      completion_callback=lambda file_path: self._completion_callback(session_id, item.id, file_path))
            
            return success
            
        except Exception as e:
            self.session_manager.update_download_item(
                session_id, item.id,
                status=DownloadStatus.FAILED,
                error_message=str(e))
            return False
    
    def _progress_callback(self, session_id: str, item_id: str, progress: float) -> None:
        self.session_manager.update_download_item(session_id, item_id, progress=progress)
    
    def _error_callback(self, session_id: str, item_id: str, error: str) -> None:
        self.session_manager.update_download_item(
            session_id, item_id,
            status=DownloadStatus.FAILED,
            error_message=error)
    
    def _completion_callback(self, session_id: str, item_id: str, file_path: str) -> None:
        self.session_manager.update_download_item(
            session_id, item_id,
            status=DownloadStatus.COMPLETED,
            progress=100.0,
            file_path=file_path)