import csv
import gzip

from logzero import setup_logger
import requests
import os
from datetime import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, DefaultDict

logger = setup_logger()

IMDB_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_GZIP_NAME = "imdb_titles.tsv.gz"


class TitleFinder:
    def __init__(self, data: DefaultDict):
        self._dict = data

    def find_id(self, title: str, year: Optional[int] = None) -> str:
        films_with_title = self._dict.get(title)
        if not films_with_title:
            logger.info(f'No film with title "{title}"')
            return None

        if len(films_with_title) == 1:
            return films_with_title[0][0]

        logger.info(f'{len(films_with_title)} films with title "{title}"')
        if not year:
            return None

        films_with_correct_year = [f for f in films_with_title if f[1] == year]

        if len(films_with_title) == 1:
            return films_with_correct_year[0][0]

        if not films_with_correct_year:
            logger.info(f'No film with title "{title}" and year {year}')
            return None

        logger.info(
            f'{len(films_with_title)}) films with title "{title}" and year {year}'
        )
        return None


@dataclass
class Film:
    title: str
    year: int
    rating: int
    date: str
    imdb_id: str = None


def build_title_finder() -> TitleFinder:
    logger.info("Reading TSV.GZ file")
    finder_data = defaultdict(list)

    with gzip.open(IMDB_GZIP_NAME, "rt") as gzipfile:
        csvreader = csv.DictReader(gzipfile, delimiter="\t")
        logger.info("Building dictionary of original titles")
        for row in csvreader:
            finder_data[row["originalTitle"]].append((row["tconst"], row["startYear"]))

    logger.info("Dictionary built")
    logger.info(
        f"{len(finder_data.keys())} distinct titles, {csvreader.line_num - 1} films"
    )
    return TitleFinder(finder_data)


def is_imdb_download_needed() -> bool:
    try:
        ts = os.path.getmtime(IMDB_GZIP_NAME)
        delta = dt.now() - dt.fromtimestamp(ts)
        if delta.days <= 1:
            return False
        return True
    except OSError:
        return True


def download_imdb_titles_file():
    logger.info("Checking if IMDB data is already downloaded")
    download_needed = is_imdb_download_needed()
    if not download_needed:
        logger.info("IMDB data is already downloaded and fresh enough")
        return

    logger.info("Downloading IMDB data")
    try:
        req = requests.get(IMDB_URL)
        open(IMDB_GZIP_NAME, "wb").write(req.content)
    except requests.RequestException as e:
        logger.error("IMDB data download failed")
        raise
    logger.info("IMDB data downloaded")


def load_kinopoisk_data(kinopoisk_filename: str) -> List[Film]:
    logger.info(f"Loading Kinopoisk data from {kinopoisk_filename}")


def add_imdb_id(imdb: TitleFinder, film: Film) -> Film:
    imdb_id = imdb.find_id(film.title, film.year)
    if imdb_id:
        film.imdb_id = imdb_id
    return film


def export_to_csv(films_with_ids: List[Film]):
    logger.info("Exporting to CSV")


if __name__ == "__main__":
    download_imdb_titles_file()
    imdb = build_title_finder()

    films = load_kinopoisk_data(kinopoisk_filename)
    films_with_ids = [add_imdb_id(imdb, film) for film in films]
    export_to_csv(films_with_ids)
