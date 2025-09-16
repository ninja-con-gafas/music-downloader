"""
Music Downloader.

A web-based application to download song identified by Shazam or from YouTube with correct metadata tagging.
"""

from datetime import datetime
from streamlit import file_uploader, markdown, set_page_config, sidebar, tabs, text, text_area
from typing import List

set_page_config(
    page_title="Music Downloader",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded")

sidebar.title("Music Downloader")
with sidebar.expander("About"):
    text("Download songs identified by Shazam or from YouTube with correct metadata tagging.")

sidebar.markdown("---")
sidebar.write(f"© {datetime.now().year} Music Downloader")

tab_labels: List[str] = ["Shazam", "YouTube", "Progress", "Files"]
tab_shazam, tab_youtube, tab_progress, tab_files = tabs(tab_labels)

with tab_shazam:
    markdown(
        """
        1. Go to [Shazam Data Download](https://www.shazam.com/privacy/login/download).
        2. Log in and request your data.
        3. You'll receive a download link via email. Click the link to download a ZIP archive.
        4. Extract the file named `SyncedShazams.csv` from the archive.
        5. Upload it below.
        """,
        unsafe_allow_html=True
    )

    uploaded_csv = file_uploader(
        "Upload your SyncedShazams.csv",
        type="csv",
        help="Export your Shazam data from the official site and upload here.")

with tab_youtube:
    markdown(
        """
        Paste one or more YouTube video URLs, each on a separate line.
        """,
        unsafe_allow_html=True
    )

    urls: str = text_area(
        "Paste YouTube video URLs",
        height=150)

with tab_progress:
    pass

with tab_files:
    pass
