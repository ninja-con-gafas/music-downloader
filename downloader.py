from concurrent import futures
from google.youtube import download_audio_as_mp3, get_video_id, search_youtube
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
            video_id=lambda x: x['url'].apply(get_video_id))

        logger.info("Starting concurrent downloads of audio streams.")
        ctx = get_script_run_ctx()

        with futures.ThreadPoolExecutor() as executor:
            tasks = [
                executor.submit(
                    download_audio_as_mp3,
                    download_path=DOWNLOADS_PATH,
                    file_name=f"{row['title']} {row['artist']} {row['video_id']}",
                    url=row["url"]
                ) for row in shazams.to_dict(orient="records")]

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

        logger.info("Marking downloaded tracks and storing report in session state.")
        session_state.report = (
            shazams.assign(is_downloaded=lambda x: x["video_id"].apply(is_audio_downloaded)))

        logger.info("Shazam download process completed successfully.")
    except Exception as e:
        logger.error(f"Error during Shazam download process: {str(e)}")
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
        return read_csv(filepath_or_buffer=file_path) \
            .drop_duplicates(subset=["artist", "title"]) \
            .drop(columns=["date", "latitude", "longitude", "status"], errors="ignore") \
            .sort_values(by=["artist", "title"])
    except Exception as e:
        logger.error(f"Failed to extract Shazam data: {str(e)}")
        raise Exception(f"Failed to extract Shazam data: {str(e)}")

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
