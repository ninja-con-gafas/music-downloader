"""
Music Downloader.

A web-based application to download song identified by Shazam or from YouTube with correct metadata tagging.
"""

from datetime import datetime
from downloader import (download_shazams_with_session, download_youtube_with_session, extract_shazams, 
                        extract_youtube_urls)
from pandas import DataFrame
from streamlit import (balloons, button, caption, columns, divider, empty, error, expander, file_uploader, header, 
                       info, metric, markdown, rerun, selectbox, session_state, set_page_config, sidebar, spinner, 
                       subheader, success, tabs, text, write)
from streamlit.delta_generator import DeltaGenerator
from streamlit.runtime.uploaded_file_manager import UploadedFile
from SessionManager import DownloadSession, SessionAwareDownloadExecutor, SessionManager
from typing import Any, Dict, List

def cancel_session(session_id: str) -> bool:
    success: bool = session_state.session_manager.cancel_session(session_id)
    if success and hasattr(session_state, 'current_session_id') and session_state.current_session_id == session_id:
        session: DownloadSession = session_state.session_manager.get_session(session_id)
        if session:
            session_state.progress_summary = session.get_progress_summary()
    return success

def colored_metric(label, value, delta=None, color="black"):
    delta_coloured = f'<span style="color:{color}; font-weight:bold;">{delta}</span>' if delta else ''
    valu_coloured = f'<span style="color:{color}; font-weight:bold;">{value}</span>'
    markdown(f"""
        <div style='text-align:center;'>
            <div>{label}</div>
            <div style='font-size:28px; font-weight:bold;'>{valu_coloured}</div>
            <div style='font-size:18px;'>{delta_coloured}</div>
        </div>
    """, unsafe_allow_html=True)

def get_all_session_summaries() -> List[Dict[str, Any]]:
    sessions: List[DownloadSession] = session_state.session_manager.get_all_sessions()
    return [session.get_progress_summary() for session in sessions]

def icon_anchor(hyperlink: str, icon_url: str, border_radius: int=8, height: int=30, width: int=30) -> str:
    """
    Generate an HTML button-like anchor element with an icon background.

    Parameters:
        hyperlink (str): The URL to which the button should redirect when clicked.
        icon_url (str): The URL of the icon image to be displayed as the button background.
        border_radius (int, optional): Radius of the button corners in pixels. Defaults to 8.
        height (int, optional): Height of the button in pixels. Defaults to 30.
        width (int, optional): Width of the button in pixels. Defaults to 30.

    Returns:
        str: A formatted HTML string representing the icon-styled clickable button.
    """

    return f"""
    <a href="{hyperlink}" target="_blank" style="
        display: inline-block;
        background: url('{icon_url}') no-repeat center center;
        background-size: contain;
        width: {width}px;
        height: {height}px;
        border-radius: {border_radius}px;
        border: none;
        cursor: pointer;
        ">
    </a>
    """

def set_session_manager() -> None:
    if 'session_manager' not in session_state:
        session_state.session_manager = SessionManager(max_concurrent_sessions=1, 
                                                       session_timeout_minutes=1440)
    if 'download_executor' not in session_state:
        session_state.download_executor = SessionAwareDownloadExecutor(session_state.session_manager, 
                                                                       max_workers=4)
    if "active_sessions" not in session_state:
        session_state.active_sessions = {}

def set_sidebar() -> None:
    sidebar.title("Music Downloader")

    with sidebar:
        header("Session Managemenent")
        if 'session_manager' in session_state:
            statistics: Dict[str, Any] = session_state.session_manager.get_session_statistics()
            metric("Active Sessions", statistics['active_sessions'])
            metric("Total Sessions", statistics['total_sessions'])

    with sidebar.expander("About"):
        text("Download songs identified by Shazam or from YouTube with correct metadata tagging.")

    sidebar.markdown("---")
    sidebar.markdown(
        f"""
        Developed by [ninja-con-gafas](https://github.com/ninja-con-gafas). 
        Licensed under [AGPL-3.0](https://github.com/ninja-con-gafas/music-downloader?tab=AGPL-3.0-1-ov-file).
        {icon_anchor("https://github.com/ninja-con-gafas/music-downloader",
                                 "https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png")}
        """,
        unsafe_allow_html=True)
    sidebar.write(f"© {datetime.now().year} Music Downloader")

def set_tab_downloads() -> None:
    download_type: str = selectbox("Choose download source:",
                                    ["Shazam", "YouTube"],
                                    key="download_type")
    if download_type == "Shazam":
        markdown(
        """
        1. Go to [Shazam Data Download](https://www.shazam.com/privacy/login/download).
        2. Log in and request your data.
        3. You'll receive a download link via email. Click the link to download a ZIP archive.
        4. Extract the `.csv` file from the archive.
        5. Upload it below.
        """,
        unsafe_allow_html=True)

        uploaded_file: UploadedFile = file_uploader("Upload your Shazams CSV",
                                    type="csv",
                                    help="Export your Shazam data from the official site and upload here.")

        if uploaded_file is not None:
            try:
                records: DataFrame = extract_shazams(uploaded_file)
                write(f"Found {len(records)} unique Shazam tracks.")
                start_download(download_type, records)
            except Exception as e:
                error(f"Error processing file: {str(e)}")

    if download_type == "YouTube":
        markdown(
        """
        1. Prepare a CSV file with a column named `url` containing YouTube or YouTube Music video URLs.
        2. Upload the CSV file below.
        """,
        unsafe_allow_html=True)
        
        uploaded_file: UploadedFile = file_uploader("Upload your CSV file containing YouTube or YouTube Music videos URLs.",
                                    type="csv",
                                    help="The CSV file must contain a column named `url` with the video URLs.")

        if uploaded_file is not None:
            try:
                records: DataFrame = extract_youtube_urls(uploaded_file)
                write(f"Found {len(records)} unique YouTube URLs")
                start_download(download_type, records)
            except Exception as e:
                error(f"Error processing file: {str(e)}")

def set_tab_history() -> None:
    all_sessions: List[Dict] = get_all_session_summaries()
    inactive_sessions: List[Dict] = [session for session in all_sessions if session['status'] in ['cancelled', 'completed', 'failed']]
    if inactive_sessions:
        for session in inactive_sessions:
            with expander(f"{session['name']} - {session['status'].title()}"):
                column_session_details, column_session_statistics = columns(2)
                
                with column_session_details:
                    write(f"Session ID: {session['session_id']}")
                    write(f"Status: {session['status'].title()}")
                    created_time = datetime.fromisoformat(session['created_at'].replace('Z', '+00:00'))
                    write(f"Started: {created_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    if session.get('completed_at'):
                        completed_time = datetime.fromisoformat(session['completed_at'].replace('Z', '+00:00'))
                        write(f"Completed: {completed_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        runtime = completed_time - created_time
                        hours, remainder = divmod(runtime.total_seconds(), 3600)
                        minutes, seconds = divmod(remainder, 60)
                        write(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
                
                with column_session_statistics:
                    write(f"Progress: {session['overall_progress']:.1f}%")
                    write(f"Failed: {session['failed_items']}")
                    write(f"Total Items: {session['total_items']}")
                    write(f"Completed: {session['completed_items']}")
    else:
        info("No session history found. Complete some downloads to see history here.")

def set_tab_progress() -> None:
    all_sessions: List[Dict] = get_all_session_summaries()
    active_sessions: List[Dict] = [session for session in all_sessions if session['status'] in ['pending', 'running']]
    if active_sessions:
        column_header, column_refresh_button = columns([4, 1])
        with column_header:
            write(f"Monitoring {len(active_sessions)} active sessions:")
        with column_refresh_button:
            if button("Refresh"):
                rerun()
        for session in active_sessions:
            block: DeltaGenerator = empty()
            with block.container():
                column_session_details, column_session_id = columns([4, 1])
                
                with column_session_details:
                    subheader(f"{session['name']}")
                    write(f"Status: {session['status'].title()}")
                
                with column_session_id:
                    write(f"ID: `{session['session_id'][:8]}`")
                
                progress_bar = empty()
                progress_bar.progress(session['overall_progress'] / 100, 
                                    text=f"Progress: {session['overall_progress']:.1f}%")
                
                column_total_downloads, column_completed_downloads, column_failed_downloads, column_downloading = columns(4)
                
                with column_total_downloads:
                    metric("Total", session['total_items'])
                with column_completed_downloads:
                    colored_metric("Completed", session['completed_items'],
                                    delta=None if session['completed_items'] == 0 else f"+{session['completed_items']}", color="green")
                with column_failed_downloads:
                    colored_metric("Failed", session['failed_items'],
                                    delta=None if session['failed_items'] == 0 else f"+{session['failed_items']}", color="red")
                with column_downloading:
                    colored_metric("Downloading", session['downloading_items'],
                                delta=None if session['downloading_items'] == 0 else f"+{session['downloading_items']}", color="orange")
                    
                column_cancel_session, = columns(1)
                
                with column_cancel_session:
                    if session['status'] in ['running', 'pending']:
                        if button("Cancel", key=f"hist_cancel_{session['session_id']}"):
                            cancel_session(session['session_id'])
                            rerun()
                
                if session.get('started_at'):
                    start_time = datetime.fromisoformat(session['started_at'].replace('Z', '+00:00'))
                    elapsed = datetime.now() - start_time.replace(tzinfo=None)
                    caption(f"Started: {start_time.strftime('%H:%M:%S')} | Elapsed: {str(elapsed).split('.')[0]}")
                
                divider()
    else:
        info("No active sessions found. Start a download from the Downloads tab to see progress here.")

def set_tabs() -> None:
    tab_labels: List[str] = ["Downloads", "Progress", "History"]
    tab_downloads, tab_progress, tab_history = tabs(tab_labels)
    with tab_downloads:
        set_tab_downloads()
    with tab_progress:
        set_tab_progress()
    with tab_history:
        set_tab_history()

def start_download(download_type: str, records: DataFrame):
    session_name = f"{download_type}_{datetime.now().strftime('%H:%M:%S')}"
    if button("Start Download", type="primary", use_container_width=True):
        try:
            with spinner("Starting download session"):
                if download_type == "Shazam":
                    session_id = download_shazams_with_session(records, session_name)
                if download_type == "YouTube":
                    session_id = download_youtube_with_session(records, session_name)
                
                success(f"Session started: `{session_id}`")
                session_state.active_sessions[session_id] = session_name
                balloons()
        except Exception as e:
            error(f"Failed to start download: {str(e)}")

set_page_config(page_title="Music Downloader",
                page_icon="🎵",
                layout="wide",
                initial_sidebar_state="expanded")
set_session_manager()
set_sidebar()
set_tabs()