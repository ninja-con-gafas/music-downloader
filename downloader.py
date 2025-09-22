import re
from concurrent import futures
from google.youtube import download_audio_as_mp3, get_video_id, get_video_metadata, search_youtube
from logging import basicConfig, getLogger, INFO
from os import listdir, makedirs, path
from pandas import DataFrame, read_csv
from streamlit import session_state
from streamlit.runtime.scriptrunner import get_script_run_ctx, add_script_run_ctx
from StreamlitLogHandler import StreamlitLogHandler

basicConfig(level=INFO)
logger = getLogger(__name__)

DOWNLOADS_PATH = path.expanduser("~/Downloads")
makedirs(DOWNLOADS_PATH, exist_ok=True)

@StreamlitLogHandler.decorate
def download_shazams(shazams: DataFrame) -> None:
    """
    Process Shazam data and initiate downloads concurrently.

    Parameters:
        shazams (DataFrame): DataFrame containing Shazam track information.
    """

    logger.info("Starting Shazam download process.")

    try:
        logger.info("Searching YouTube URLs for Shazam tracks.")
        shazams = shazams.assign(
            url=lambda x: x.apply(lambda row: search_youtube(f"{row['title']} {row['artist']} lyrics")[0], axis=1),
            video_id=lambda x: x['url'].apply(get_video_id),
            file_name=lambda x: x.apply(lambda row: f"{row['title']} {row['artist']} {row['video_id']}", axis=1))

        start_concurrent_downloads(records=shazams.to_dict(orient="records"),
                                   name_key="file_name",
                                   url_key="url")

        logger.info("Marking downloaded tracks and storing report in session state.")
        session_state.report = (shazams.assign(is_downloaded=lambda x: x["video_id"].apply(is_audio_downloaded)))

        logger.info("Shazam download process completed successfully.")
    except Exception as e:
        logger.error(f"Error during Shazam download process: {str(e)}")
        raise

@StreamlitLogHandler.decorate
def download_youtube(urls: DataFrame) -> None:
    """
    Process YouTube URLs and initiate downloads concurrently.

    Parameters:
        urls (DataFrame): DataFrame containing YouTube video URLs.
    """

    logger.info("Starting YouTube download process.")

    try:
        urls = (urls.assign(video_id=lambda x: x['url'].apply(get_video_id))
        .drop_duplicates(subset=['video_id'])
        .assign(metadata=lambda x: x['video_id'].apply(get_video_metadata),
                name=lambda x: x.apply(lambda row: re.sub(
                    r'[^a-zA-Z0-9]',
                    ' ',
                    f"{row['metadata'].get('title')} {row['metadata'].get('author_name')}"
                ) + f" {row['video_id']}",
                axis=1)))

        start_concurrent_downloads(records=urls.to_dict(orient="records"),
                                   name_key="name",
                                   url_key="url")

        logger.info("Marking downloaded tracks and storing report in session state.")
        session_state.report = (urls.assign(is_downloaded=lambda x: x["video_id"].apply(is_audio_downloaded)))

        logger.info("YouTube download process completed successfully.")
    except Exception as e:
        logger.error(f"Error during YouTube download process: {str(e)}")
        raise

@StreamlitLogHandler.decorate
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
    
@StreamlitLogHandler.decorate
def extract_youtube_urls(file_path: str) -> DataFrame:
    """
    Extract unique YouTube URLs from a CSV file.

    Parameters:
        file_path (str): Path to the CSV file containing YouTube URLs.

    Returns:
        DataFrame: A DataFrame containing the unique YouTube URLs.
    """

    logger.info(f"Extracting YouTube URLs from: {file_path}")

    try:
        return (read_csv(filepath_or_buffer=file_path)
            .drop_duplicates(subset=["url"])
            .sort_values(by=["url"]))
    except Exception as e:
        logger.error(f"Failed to extract YouTube URLs: {str(e)}")
        raise Exception(f"Failed to extract YouTube URLs: {str(e)}")

@StreamlitLogHandler.decorate
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
        logger.info(f"No existing audio found for video_id {video_id}.")
        return False
    except Exception as e:
        logger.error(f"Error checking audio for video_id {video_id}: {str(e)}")
        return False

def start_concurrent_downloads(records: list[dict], name_key: str, url_key: str) -> None:
    """
    Start concurrent audio downloads for the given records.

    Parameters:
        records (list[dict]): List of records containing file names and URLs.
        name_key (str): The key in each record that provides the file name.
        url_key (str): The key in each record that provides the URL.

    Raises:
        Exception: If any error occurs during task submission or execution.
    """

    logger.info("Starting concurrent downloads of audio streams.")
    ctx = get_script_run_ctx()

    try:
        with futures.ThreadPoolExecutor() as executor:
            tasks = [executor.submit(download_audio_as_mp3,
                                     download_path=DOWNLOADS_PATH,
                                     file_name=row[name_key],
                                     url=row[url_key]) for row in records]

            for thread in executor._threads:
                try:
                    add_script_run_ctx(thread, ctx)
                except Exception as e:
                    logger.exception(f"Failed to add_script_run_ctx to thread {thread.name}: {e}")

            for future in futures.as_completed(tasks):
                try:
                    future.result()
                except Exception as e:
                    logger.exception(f"Error in download task: {e}")
    except Exception as e:
        logger.error(f"Error in concurrent download execution: {str(e)}")
        raise
