import mimetypes
import requests
from logging import basicConfig, getLogger, INFO
from mutagen.id3 import APIC, ID3, TALB, TCOM, TCON, TDRC, TIT2, TPE1, TRCK
from mutagen.mp3 import MP3
from shazamio import Shazam
from typing import Any, Dict, List

basicConfig(level=INFO)
logger = getLogger(__name__)

def apply_metadata(metadata: dict, audio_file_path: str) -> None:
    """
    Apply metadata and cover art from Apple Music-style dictionary to an audio file.

    Parameters
        metadata (dict): Metadata dictionary in Apple Music API style.
        audio_file_path (str): Path to the audio file where metadata should be applied.

    Raises
        ValueError: If the audio file format is unsupported.
    """

    logger.info(f"Applying metadata to file: {audio_file_path}")
    attributes = metadata.get('attributes', {})

    title = attributes.get('name')
    artist = attributes.get('artistName')
    album = attributes.get('albumName')
    genre_list = attributes.get('genreNames', [])
    genre = genre_list[0] if genre_list else None
    track_number = attributes.get('trackNumber')
    release_date = attributes.get('releaseDate')
    composer = attributes.get('composerName')

    artwork = attributes.get('artwork', {})
    height = artwork.get('height')
    width = artwork.get('width')
    artwork_url = artwork.get('url').replace('{w}', str(width)).replace('{h}', str(height))

    audio = MP3(audio_file_path, ID3=ID3)
    if audio is None:
        logger.error("Unsupported audio format or file not found")
        raise ValueError("Unsupported audio format or file not found")
    
    if audio.tags:
        logger.info("Deleting existing tags")
        audio.delete()
        audio.save()
        audio = MP3(audio_file_path, ID3=ID3)
    try:
        tags = audio.tags
    except Exception as e:
        logger.warning(f"Could not access tags: {e}")

    if title:
        logger.info(f"Setting title: {title}")
        tags.add(TIT2(encoding=3, text=title))
    if artist:
        logger.info(f"Setting artist: {artist}")
        tags.add(TPE1(encoding=3, text=artist))
    if album:
        logger.info(f"Setting album: {album}")
        tags.add(TALB(encoding=3, text=album))
    if genre:
        logger.info(f"Setting genre: {genre}")
        tags.add(TCON(encoding=3, text=genre))
    if track_number:
        logger.info(f"Setting track number: {track_number}")
        tags.add(TRCK(encoding=3, text=str(track_number)))
    if release_date:
        logger.info(f"Setting release date: {release_date}")
        tags.add(TDRC(encoding=3, text=release_date))
    if composer:
        logger.info(f"Setting composer: {composer}")
        tags.add(TCOM(encoding=3, text=composer))
    
    if artwork_url:
        logger.info(f"Downloading artwork from {artwork_url}")
        try:
            response = requests.get(artwork_url)
            response.raise_for_status()
            image_data = response.content
            mime_type = mimetypes.guess_type(artwork_url)[0] or 'image/jpeg'
            tags.add(APIC(encoding=3,
                          mime=mime_type,
                          type=3,
                          desc='Cover',
                          data=image_data))
        except Exception as e:
            logger.warning(f"Failed to download or embed artwork: {e}")

    audio.save()
    logger.info("Metadata applied and file saved.")

def search_shazam(term: str, types: str, limit: int = 1, country_code: str = "IN") -> List[Dict[str, Any]]:
    """
    Perform a search query on the Shazam API.

    Parameters
        term (str): The search term such as a song name or artist name.
        types (str): The type of search. Allowed values: 'artists', 'songs'.
        limit (int): The number of search results to return. Default is 1.
        country_code (str): The country/region code for the catalog. Default is 'IN'.

    Returns
        List{Dict[str, Any]}: The JSON response from Shazam API as a list of dictionaries.

    Raises
        ValueError: If an invalid `types` parameter is passed.
        requests.exceptions.RequestException: If the request encounters network issues.
    """

    logger.info(f"Searching Shazam for term='{term}', type='{types}', limit={limit}, country_code='{country_code}'")
    if types not in {"songs", "artists"}:
        logger.error(f"Invalid value for parameter 'types': {types}.")
        raise ValueError("Parameter 'types' must be either 'artists' or 'songs'.")

    headers: Dict[str, str] = {
        "sec-ch-ua-platform": '"Windows"',
        "Referer": "https://www.shazam.com/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Microsoft Edge";v="140"',
        "sec-ch-ua-mobile": "?0",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
        ),
        "Accept": "application/json",
        "DNT": "1",
        "Content-Type": "application/json"}

    params: Dict[str, str] = {"types": types,
                              "term": term,
                              "limit": str(limit)}

    url: str = f"https://www.shazam.com/services/amapi/v1/catalog/{country_code}/search"

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        json_response: Dict = response.json() or {}
        
        data = json_response.get("results", {}).get(types, {}).get("data")
        if data is not None:
            return data
        logger.warning(f"No data found for {types} {term} in response.")
        return []
    except (ValueError, KeyError, TypeError) as e:
        logger.warning(f"Failed to parse JSON or extract data for {types} {term}: {e}")
    except requests.exceptions.RequestException as e:
        logger.exception(f"Shazam API request failed: {e}")

async def recognize_music(file_path: str) -> Dict[str, Any]:
    """
    Recognise a music by analysing a local audio file using Shazamio.

    Parameters
        file_path (str): The path to the audio file.

    Returns
        List[Dict[str, Any]]: List containing recognised music information.
    """

    logger.info(f"Recognising music from file: {file_path}")
    try:
        shazam = Shazam()
        result = await shazam.recognize(file_path)
        if result:
            return result.get("track")
        logger.warning(f"No result returned for file: {file_path}")
        return
    except Exception as e:
        logger.error(f"Error recognising music: {e}")
        return