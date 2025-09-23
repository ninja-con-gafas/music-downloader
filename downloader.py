import datetime
import re
import threading
from google.youtube import download_audio_as_mp3, get_video_id, get_video_metadata, search_youtube
from logging import basicConfig, getLogger, INFO
from os import listdir, makedirs, path
from pandas import DataFrame, read_csv
from streamlit import session_state
from SessionManager import DownloadItem, DownloadSession

basicConfig(level=INFO)
logger = getLogger(__name__)

DOWNLOADS_PATH = path.expanduser("~/Downloads")
makedirs(DOWNLOADS_PATH, exist_ok=True)

def download_shazams_with_session(shazams: DataFrame, session_name: str = None) -> str:
    """
    Process Shazam data and initiate downloads with session management.

    Parameters:
        shazams (DataFrame): DataFrame containing Shazam track information.
        session_name (str): Optional name for the session.

    Returns:
        str: The session ID for tracking progress.
    """

    logger.info("Starting Shazam download process.")

    try:
        if not session_name:
            session_name = f"Shazam Downloads - {len(shazams)} tracks ({datetime.now().strftime('%H:%M:%S')})"

        session: DownloadSession = session_state.session_manager.create_session(
            name=session_name,
            metadata={"source": "shazam", "total_tracks": len(shazams)})
        
        logger.info(f"Created session {session.session_id} for Shazam downloads.")

        logger.info("Searching YouTube URLs for Shazam tracks.")
        shazams = shazams.assign(
            url=lambda x: x.apply(lambda row: search_youtube(f"{row['title']} {row['artist']} lyrics")[0], axis=1),
            video_id=lambda x: x['url'].apply(get_video_id),
            file_name=lambda x: x.apply(lambda row: f"{row['title']} {row['artist']} {row['video_id']}", axis=1))

        for _, row in shazams.iterrows():
            download_item = DownloadItem(
                id=f"shazam_{row['video_id']}",
                name=row['file_name'],
                url=row['url'],
                metadata={
                    "video_id": row['video_id'],
                    "title": row['title'],
                    "artist": row['artist'],
                    "source": "shazam"
                }
            )
            session.add_download(download_item)

        download_thread = threading.Thread(
            target=session_state.download_executor.execute_session_downloads,
            args=(session.session_id, download_wrapper),
            kwargs={"max_concurrent_downloads": 3}
        )
        download_thread.daemon = True
        download_thread.start()
        
        session_state.progress_summary = session.get_progress_summary()
        session_state.current_session_id = session.session_id
        
        logger.info(f"Started downloads for session {session.session_id}")
        return session.session_id
        
    except Exception as e:
        logger.error(f"Error during Shazam download process: {str(e)}")
        raise

def download_wrapper(item: DownloadItem, progress_callback, error_callback, completion_callback) -> bool:
    """
    Wrapper for downloading YouTube audio as MP3. Skips existing files, otherwise downloads, reports progress, 
    validates the result, and triggers callbacks.  

    Parameters:
        item (DownloadItem): The download item containing metadata (e.g., `video_id`, `name`, `url`).
        progress_callback (Callable[[float], None]): A callback to report progress percentage.
        error_callback (Callable[[str], None]): A callback to handle error messages.
        completion_callback (Callable[[str], None]): A callback to signal successful completion
            with the path of the downloaded MP3 file.

    Returns:
        bool: True if the download succeeds or the file already exists, False otherwise.

    Raises:
        Exception: Propagates unexpected errors that occur during the download process.
    """
    
    try:
        if is_audio_downloaded(item.metadata['video_id']):
            logger.info(f"Audio already exists for {item.name}")
            file_path = find_existing_audio_path(item.metadata['video_id'])
            completion_callback(file_path)
            return True
        
        progress_callback(5.0)
        
        logger.info(f"Downloading {item.name} from {item.url}")
        download_audio_as_mp3(download_path=DOWNLOADS_PATH,
                              file_name=item.name,
                              url=item.url)
        
        file_path: str = path.join(DOWNLOADS_PATH, f"{item.name}.mp3")
        if path.exists(file_path):
            logger.info(f"{item.name} downloaded as {file_path}")
            completion_callback(file_path)
            return True
        else:
            error_callback(f"Download completed but file not found: {file_path}")
            return False
        
    except Exception as e:
        logger.error(f"Download failed for {item.name}: {str(e)}")
        error_callback(str(e))
        return False

def download_youtube_with_session(urls: DataFrame, session_name: str = None) -> str:
    """
    Process YouTube URLs and initiate downloads with session management.

    Parameters:
        urls (DataFrame): DataFrame containing YouTube video URLs.
        session_name (str): Optional name for the session.

    Returns:
        str: The session ID for tracking progress
    """

    logger.info("Starting YouTube download process.")

    try:
        if not session_name:
            session_name = f"YouTube Downloads - {len(urls)} URLs ({datetime.now().strftime('%H:%M:%S')})"
        
        session: DownloadSession = session_state.session_manager.create_session(
            name=session_name,
            metadata={"source": "youtube", "total_urls": len(urls)})
        
        logger.info(f"Created session {session.session_id} for YouTube downloads.")

        urls = (urls.assign(video_id=lambda x: x['url'].apply(get_video_id))
        .drop_duplicates(subset=['video_id'])
        .assign(metadata=lambda x: x['video_id'].apply(get_video_metadata),
                name=lambda x: x.apply(lambda row: re.sub(
                    r'[^a-zA-Z0-9]',
                    ' ',
                    f"{row['metadata'].get('title')} {row['metadata'].get('author_name')}"
                ) + f" {row['video_id']}",
                axis=1)))

        for _, row in urls.iterrows():
            download_item = DownloadItem(
                id=f"youtube_{row['video_id']}",
                name=row['name'],
                url=row['url'],
                metadata={
                    "video_id": row['video_id'],
                    "title": row['metadata'].get('title'),
                    "author": row['metadata'].get('author_name'),
                    "source": "youtube"
                }
            )
            session.add_download(download_item)

        download_thread = threading.Thread(
            target=session_state.download_executor.execute_session_downloads,
            args=(session.session_id, download_wrapper),
            kwargs={"max_concurrent_downloads": 3})
        download_thread.daemon = True
        download_thread.start()
        
        session_state.progress_summary = session.get_progress_summary()
        session_state.current_session_id = session.session_id
        
        logger.info(f"Started downloads for session {session.session_id}")
        return session.session_id
        
    except Exception as e:
        logger.error(f"Error during YouTube download process: {str(e)}")
        raise

def extract_shazams(file_path: str) -> DataFrame:
    """
    Extract unique Shazam tracks from a CSV file, dropping unnecessary columns.

    Parameters:
        file_path (str): Path to the Shazam CSV file.

    Returns:
        DataFrame: A DataFrame containing the unique Shazam tracks.

    Raises:
        Exception: If the CSV file cannot be read or processed.
    """

    logger.info(f"Extracting Shazam data from: {file_path}")

    try:
        return (read_csv(filepath_or_buffer=file_path)
            .drop_duplicates(subset=["artist", "title"])
            .drop(columns=["date", "latitude", "longitude", "status"], errors="ignore")
            .sort_values(by=["artist", "title"]))
    except Exception as e:
        logger.error(f"Failed to extract Shazam data: {str(e)}")
        raise Exception(f"Failed to extract Shazam data: {str(e)}")

def extract_youtube_urls(file_path: str) -> DataFrame:
    """
    Extract and validate unique YouTube URLs from a CSV file.

    Parameters:
        file_path (str): Path to the CSV file containing YouTube URLs.

    Returns:
        DataFrame: A DataFrame containing the unique YouTube URLs.
    """

    YOUTUBE_URL_PATTERN = r"^((?:https?:)?\/\/)?((?:www|m|music)\.)?((?:youtube(?:-nocookie)?\.com|youtu\.be))(\/(?:[\w\-]+\?v=|embed\/|live\/|v\/)?)([\w\-]+)(\S+)?$"

    logger.info(f"Extracting and validating YouTube URLs from: {file_path}")

    try:
        return (read_csv(filepath_or_buffer=file_path)
        .assign(url=lambda df: df["url"].astype(str).str.strip())
        .loc[lambda df: df["url"].apply(lambda u: bool(re.match(YOUTUBE_URL_PATTERN, u)))]
        .drop_duplicates(subset=["url"])
        .sort_values(by=["url"])
        .reset_index(drop=True))
    except Exception as e:
        logger.error(f"Failed to extract YouTube URLs: {str(e)}")
        raise Exception(f"Failed to extract YouTube URLs: {str(e)}")

def find_existing_audio_path(video_id: str) -> str:
    """
    Search for an existing audio file (.mp3) matching the given YouTube video ID.

    Parameters:
        video_id (str): The YouTube video identifier.

    Returns:
        str | None: Full path to the matching MP3 file if found, otherwise None.
    """

    try:
        for filename in listdir(DOWNLOADS_PATH):
            if filename.endswith('.mp3'):
                file_video_id: str = filename[:-4].split()[-1]
                if file_video_id == video_id:
                    file_path = path.join(DOWNLOADS_PATH, filename)
                    logger.info(f"Found audio file for video_id {video_id} at {file_path}")
                    return file_path
    except Exception as e:
        logger.error(f"Error searching audio path: {str(e)}")
    return None

def is_audio_downloaded(video_id: str) -> bool:
    """
    Check if the audio stream of a video with the given video_id exists in the DOWNLOADS_PATH.

    Parameters:
        video_id (str): The video ID to search for.

    Returns:
        bool: True if the video is found, False otherwise.
    """

    try:
        for filename in listdir(DOWNLOADS_PATH):
            if filename.endswith('.mp3'):
                file_video_id: str = filename[:-4].split()[-1]
                if file_video_id == video_id:
                    return True
        logger.info(f"No existing audio found for video_id {video_id}")
        return False
    except Exception as e:
        logger.error(f"Error checking audio for video_id {video_id}: {str(e)}")
        return False