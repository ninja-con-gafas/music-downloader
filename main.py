"""
Music Downloader.

A web-based application to download song identified by Shazam or from YouTube with correct metadata tagging.
"""

from datetime import datetime
from pandas import DataFrame
from downloader import extract_shazams, extract_youtube_urls, download_shazams, download_youtube
from streamlit import (dataframe, error, file_uploader, markdown, set_page_config, session_state, sidebar, success, 
                       tabs, text, text_area)
from typing import List

set_page_config(
    page_title="Music Downloader",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded")

sidebar.title("Music Downloader")

tab_labels: List[str] = ["Shazam", "YouTube", "Report"]
tab_shazam, tab_youtube, tab_report = tabs(tab_labels)

with tab_shazam:
    markdown(
        """
        1. Go to [Shazam Data Download](https://www.shazam.com/privacy/login/download).
        2. Log in and request your data.
        3. You'll receive a download link via email. Click the link to download a ZIP archive.
        4. Extract the `.csv` file from the archive.
        5. Upload it below.
        """,
        unsafe_allow_html=True)

    uploaded_csv = file_uploader(
        "Upload your Shazams CSV",
        type="csv",
        help="Export your Shazam data from the official site and upload here.")
    
    if uploaded_csv is not None:
        try:
            session_state.shazams = extract_shazams(uploaded_csv)
            success(f"Successfully loaded {len(session_state.shazams)} unique tracks from Shazam data.")
            
            download_shazams(session_state.shazams)

            sidebar.markdown("Download Statistics")
            total_downloads: int = len(session_state.report)
            successful_downloads: int = int(session_state.report["is_downloaded"].sum())
            failed_downloads: int = total_downloads - successful_downloads
            sidebar.metric("Total Processed", total_downloads)
            sidebar.metric("Successful", successful_downloads)
            sidebar.metric("Failed", failed_downloads)
                    
        except Exception as e:
            error(f"Error processing Shazam CSV: {str(e)}")

with tab_youtube:
    markdown(
        """
        1. Prepare a CSV file with a column named `url` containing YouTube or YouTube Music video URLs.
        2. Upload the CSV file below.
        """,
        unsafe_allow_html=True)

    uploaded_csv = file_uploader(
        "Upload your CSV file containing YouTube or YouTube Music videos URLs.",
        type="csv",
        help="The CSV file must contain a column named `url` with the video URLs.")
    
    if uploaded_csv is not None:
        try:
            session_state.youtube_urls = extract_youtube_urls(uploaded_csv)
            success(f"Successfully loaded {len(session_state.youtube_urls)} unique URLs from the given list.")
            
            download_youtube(session_state.youtube_urls)

            sidebar.markdown("Download Statistics")
            total_downloads: int = len(session_state.report)
            successful_downloads: int = int(session_state.report["is_downloaded"].sum())
            failed_downloads: int = total_downloads - successful_downloads
            sidebar.metric("Total Processed", total_downloads)
            sidebar.metric("Successful", successful_downloads)
            sidebar.metric("Failed", failed_downloads)
                    
        except Exception as e:
            error(f"Error processing Shazam CSV: {str(e)}")

with tab_report:
    if "report" in session_state:
        report: DataFrame = DataFrame(session_state.report)
        dataframe(report)
    else:
        text("No reports to display. Start downloading from Shazam or YouTube tabs.")

with sidebar.expander("About"):
    text("Download songs identified by Shazam or from YouTube with correct metadata tagging.")

sidebar.markdown("---")
sidebar.write(f"© {datetime.now().year} Music Downloader")