import uuid
import threading
from concurrent.futures import as_completed, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

class DownloadStatus(Enum):
    """
    Enumeration representing possible statuses of a download.
    """

    COMPLETED = "completed"
    DOWNLOADING = "downloading"
    FAILED = "failed"
    SKIPPED = "skipped"
    QUEUED = "queued"

class SessionStatus(Enum):
    """
    Enumeration representing possible statuses of a session.
    """

    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    RUNNING = "running"

@dataclass
class DownloadItem:
    """
    Data model representing a downloadable item with its metadata, progress, and status.

    Attributes:
        completed_at (Optional[datetime]): Timestamp when the download was completed.
        error_message (Optional[str]): Error details if the download failed.
        file_path (Optional[str]): Path to the downloaded file.
        id (str): Unique identifier of the download item.
        metadata (Dict[str, Any]): Additional metadata related to the download.
        name (str): Human-readable name of the download item.
        progress (float): Progress of the download expressed as a fraction (0.0-1.0).
        started_at (Optional[datetime]): Timestamp when the download started.
        status (DownloadStatus): Current status of the download.
        url (str): Source URL of the downloadable item.
    """

    id: str
    name: str
    url: str
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    file_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    progress: float = 0.0
    started_at: Optional[datetime] = None
    status: DownloadStatus = field(default=DownloadStatus.QUEUED)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the DownloadItem instance into a dictionary.

        Returns:
            Dict[str, Any]: Dictionary containing the attributes of the download item.
        """

        return {
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "file_path": self.file_path,
            "id": self.id,
            "metadata": self.metadata,
            "name": self.name,
            "progress": self.progress,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "status": self.status.value,
            "url": self.url
        }

@dataclass
class DownloadSession:
    """
    Data model representing a session that manages multiple downloads.

    Attributes:
        completed_at (Optional[datetime]): Timestamp when the session completed.
        completed_items (int): Number of successfully completed downloads.
        created_at (datetime): Timestamp when the session was created.
        downloads (List[DownloadItem]): List of download items in the session.
        failed_items (int): Number of failed downloads.
        metadata (Dict[str, Any]): Additional metadata related to the session.
        name (str): Human-readable name of the session.
        session_id (str): Unique identifier of the session.
        started_at (Optional[datetime]): Timestamp when the session started.
        status (SessionStatus): Current status of the session.
        total_items (int): Total number of downloads in the session.
    """

    name: str
    session_id: str
    completed_at: Optional[datetime] = None
    completed_items: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    downloads: List["DownloadItem"] = field(default_factory=list)
    failed_items: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    status: SessionStatus = field(default=SessionStatus.PENDING)
    total_items: int = 0
    
    def add_download(self, item: "DownloadItem") -> None:
        """
        Add a new download item to the session.

        Parameters:
            item (DownloadItem): The download item to add.
        """
        self.downloads.append(item)
        self.total_items = len(self.downloads)

    def get_progress_summary(self) -> Dict[str, Any]:
        """
        Summarise the progress of downloads in the session.

        Returns:
            Dict[str, Any]: Dictionary containing counts, progress, and status of the session.
        """

        completed = sum(1 for download in self.downloads if download.status == DownloadStatus.COMPLETED)
        failed = sum(1 for download in self.downloads if download.status == DownloadStatus.FAILED)
        downloading = sum(1 for download in self.downloads if download.status == DownloadStatus.DOWNLOADING)
        overall_progress = (completed + failed) / self.total_items * 100 if self.total_items > 0 else 0

        return {
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "completed_items": completed,
            "created_at": self.created_at.isoformat(),
            "downloading_items": downloading,
            "failed_items": failed,
            "name": self.name,
            "overall_progress": overall_progress,
            "queued_items": self.total_items - completed - failed - downloading,
            "session_id": self.session_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "status": self.status.value,
            "total_items": self.total_items,
        }

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the DownloadSession instance into a dictionary.

        Returns:
            Dict[str, Any]: Dictionary containing the attributes of the session.
        """

        return {
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "completed_items": self.completed_items,
            "created_at": self.created_at.isoformat(),
            "downloads": [download.to_dict() for download in self.downloads],
            "failed_items": self.failed_items,
            "metadata": self.metadata,
            "name": self.name,
            "session_id": self.session_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "status": self.status.value,
            "total_items": self.total_items,
        }

class SessionManager:
    """
    Executes download tasks associated with a session while tracking progress, errors, and completion.

    Attributes:
        max_workers (int): Maximum number of concurrent workers allowed for downloads.
        session_manager (SessionManager): Manager responsible for handling sessions and their states.
    """

    def __init__(self, max_concurrent_sessions: int = 5, session_timeout_minutes: int = 60):
        """
        Initialize the SessionManager with session control parameters and internal state tracking.

        Attributes:
            _cleanup_lock (threading.Lock): Lock to prevent concurrent cleanup operations.
            active_futures (Dict[str, List[Future]]): Tracks active future objects for ongoing session tasks.
            max_concurrent_sessions (int): Maximum number of sessions allowed to run concurrently (default 5).
            sessions (Dict[str, DownloadSession]): Dictionary to store all sessions by their session ID.
            session_locks (Dict[str, threading.Lock]): Locks to synchronize access for each session.
            session_timeout (timedelta): Time duration after which a session is considered expired (default 60 minutes).
        
        Parameters:
            max_concurrent_sessions (int): Optional maximum concurrent sessions (default 5).
            session_timeout_minutes (int): Optional timeout for session expiration in minutes (default 60).
        """

        self._cleanup_lock = threading.Lock()
        self.active_futures: Dict[str, List[Future]] = {}
        self.max_concurrent_sessions = max_concurrent_sessions
        self.sessions: Dict[str, DownloadSession] = {}
        self.session_locks: Dict[str, threading.Lock] = {}
        self.session_timeout = timedelta(minutes=session_timeout_minutes)

    def _cleanup_expired_sessions(self) -> None:
        """
        Remove sessions that have expired due to timeout or have completed long ago.

        This method checks the age of each session against the configured timeout and removes the
        expired or old completed sessions.
        """
        
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
    
    def _get_active_sessions_count(self) -> int:
        """
        Count how many sessions are currently active (pending or running).

        Returns:
            int: Number of active sessions.
        """

        return len([session for session in self.sessions.values() 
                   if session.status in [SessionStatus.PENDING, SessionStatus.RUNNING]])

    def cancel_session(self, session_id: str) -> bool:
        """
        Cancel an ongoing session and mark all active downloads within it as failed.

        Parameters:
            session_id (str): The ID of the session to cancel.

        Returns:
            bool: True if the cancellation was successful, False otherwise.
        """

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
        """
        Remove a session and its related resources from management after cancelling it.

        Parameters:
            session_id (str): The ID of the session to cleanup.

        Returns:
            bool: True if cleanup was successful, False if session was not found.
        """
        
        if session_id not in self.sessions:
            return False
        
        self.cancel_session(session_id)
        
        with self._cleanup_lock:
            self.sessions.pop(session_id, None)
            self.session_locks.pop(session_id, None)
            self.active_futures.pop(session_id, None)
        
        return True

    def create_session(self, name: str, metadata: Dict[str, Any] = None) -> DownloadSession:
        """
        Create a new download session with a unique session ID.

        This method cleans up expired sessions before creating a new one. It ensures the maximum number
        of concurrent sessions is not exceeded.

        Parameters:
            name (str): Name for the new session.
            metadata (Dict[str, Any], optional): Additional metadata for the session.

        Raises:
            ValueError: If the maximum number of concurrent sessions is reached.

        Returns:
            DownloadSession: The newly created download session instance.
        """

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
    
    def generate_session_id(self) -> str:
        """
        Generate a unique session ID string.

        Returns:
            str: A new unique session ID.
        """

        return str(uuid.uuid4()).replace('-', '')
    
    def get_all_sessions(self) -> List[DownloadSession]:
        """
        Get a list of all download sessions.

        Returns:
            List[DownloadSession]: All sessions currently managed.
        """

        return list(self.sessions.values())
    
    def get_active_sessions(self) -> List[DownloadSession]:
        """
        Get all sessions that are currently active (pending or running).

        Returns:
            List[DownloadSession]: List of active sessions.
        """

        return [session for session in self.sessions.values() 
                if session.status in [SessionStatus.PENDING, SessionStatus.RUNNING]]
    
    def get_session(self, session_id: str) -> Optional[DownloadSession]:
        """
        Retrieve a session by its session ID.

        Parameters:
            session_id (str): The session ID to look up.

        Returns:
            Optional[DownloadSession]: The session if found, otherwise None.
        """
        
        return self.sessions.get(session_id)
    
    def update_download_item(self, session_id: str, item_id: str, 
                           status: Optional[DownloadStatus] = None,
                           progress: Optional[float] = None,
                           error_message: Optional[str] = None,
                           file_path: Optional[str] = None) -> None:
        """
        Update the status, progress, error message, or file path of a download item within a session.

        Parameters:
            session_id (str): The session ID containing the download item.
            item_id (str): The ID of the download item to update.
            status (Optional[DownloadStatus]): New status of the download item.
            progress (Optional[float]): Progress percentage (0.0 to 100.0).
            error_message (Optional[str]): Error message if any failure occurred.
            file_path (Optional[str]): File path of the completed download.
        """
        
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
    
    def update_session_status(self, session_id: str, status: SessionStatus) -> None:
        """
        Update the status of a session and set timestamps accordingly.

        Parameters:
            session_id (str): The session ID to update.
            status (SessionStatus): The new status to assign.
        """

        if session_id in self.sessions:
            with self.session_locks[session_id]:
                self.sessions[session_id].status = status
                if status == SessionStatus.RUNNING and not self.sessions[session_id].started_at:
                    self.sessions[session_id].started_at = datetime.now()
                elif status in [SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.CANCELLED]:
                    self.sessions[session_id].completed_at = datetime.now()
    
    def get_session_statistics(self) -> Dict[str, Any]:
        """
        Retrieve statistics about current sessions, including counts by status and limits.

        Returns:
            Dict[str, Any]: A dictionary containing session counts, max concurrency, timeout, and
                            counts of sessions grouped by their status.
        """

        active_count = self._get_active_sessions_count()
        total_count = len(self.sessions)
        
        return {
            'total_sessions': total_count,
            'active_sessions': active_count,
            'max_concurrent_sessions': self.max_concurrent_sessions,
            'session_timeout_minutes': self.session_timeout.total_seconds() / 60,
            'sessions_by_status': {
                status.value: len([session for session in self.sessions.values() if session.status == status])
                for status in SessionStatus
            }
        }

class SessionAwareDownloadExecutor:
    """
    Executes download tasks associated with a session while tracking progress, errors, and completion.

    Attributes
        max_workers (int): Maximum number of concurrent workers allowed for downloads.
        session_manager (SessionManager): Manager responsible for handling sessions and their states.
    """

    def __init__(self, session_manager: SessionManager, max_workers: int = 4):
        """
        Initialise the SessionAwareDownloadExecutor.

        Parameters
            session_manager (SessionManager): The session manager to manage session states and downloads.
            max_workers (int): Maximum number of concurrent workers (default is 4).
        """

        self.session_manager = session_manager
        self.max_workers = max_workers
    
    def _completion_callback(self, session_id: str, item_id: str, file_path: str) -> None:
        """
        Update the status of a download item to completed.

        Parameters
            session_id (str): Identifier of the session.
            item_id (str): Identifier of the download item.
            file_path (str): Path of the completed downloaded file.
        """

        self.session_manager.update_download_item(
            session_id, item_id,
            status=DownloadStatus.COMPLETED,
            progress=100.0,
            file_path=file_path)

    def _download_with_session_context(self, session_id: str, item: DownloadItem,
                                       download_function: Callable) -> bool:
        """
        Execute a download task within the context of a session.

        Parameters
            session_id (str): Identifier of the session.
            item (DownloadItem): Download item containing metadata for the task.
            download_function (Callable): Function to execute the actual download.

        Returns
            bool: True if the download succeeds, False otherwise.
        """

        try:
            self.session_manager.update_download_item(
                session_id, item.id,
                status=DownloadStatus.DOWNLOADING)

            success = download_function(
                item,
                progress_callback=lambda progress: self._progress_callback(session_id, item.id, progress),
                error_callback=lambda error: self._error_callback(session_id, item.id, error),
                completion_callback=lambda file_path: self._completion_callback(session_id, item.id, file_path)
            )

            return success

        except Exception as e:
            self.session_manager.update_download_item(
                session_id, item.id,
                status=DownloadStatus.FAILED,
                error_message=str(e))
            return False

    def _error_callback(self, session_id: str, item_id: str, error: str) -> None:
        """
        Update the status of a download item when an error occurs.

        Parameters
            session_id (str): Identifier of the session.
            item_id (str): Identifier of the download item.
            error (str): Error message describing the failure.
        """

        self.session_manager.update_download_item(
            session_id, item_id,
            status=DownloadStatus.FAILED,
            error_message=error)

    def _progress_callback(self, session_id: str, item_id: str, progress: float) -> None:
        """
        Update the progress of a download item.

        Parameters
            session_id (str): Identifier of the session.
            item_id (str): Identifier of the download item.
            progress (float): Percentage of completion for the download.
        """

        self.session_manager.update_download_item(session_id, item_id, progress=progress)

    def execute_session_downloads(self, session_id: str,
                                  download_function: Callable,
                                  max_concurrent_downloads: int = None) -> None:
        """
        Execute all downloads within a given session, managing concurrency and session state.

        Parameters
            session_id (str): Identifier of the session whose downloads will be executed.
            download_function (Callable): Function to handle individual downloads.
            max_concurrent_downloads (int): Maximum number of concurrent downloads to allow. If None, falls back to `max_workers`.

        Raises
            ValueError: If the session is not found or not in a pending state.
            Exception: Propagates exceptions raised during execution.
        """

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
                    future = executor.submit(
                        self._download_with_session_context,
                        session_id, item, download_function
                    )
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