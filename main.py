import csv
import gzip
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime as dt
from typing import Optional, List, DefaultDict

import requests
from bs4 import BeautifulSoup
from logzero import setup_logger

logger = setup_logger()

IMDB_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_GZIP_NAME = "imdb_titles.tsv.gz"


class TitleFinder:
    def __init__(self, data: DefaultDict):
        self._dict = data

    def find_id(self, title: str, year: Optional[int] = None) -> Optional[str]:
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


def parse_kinopoisk_row(row) -> Optional[Film]:
    try:
        tds = row.findAll("td")
        title = tds[1].getText()
        if not title:
            raise ValueError(f"No title found in {row}")

        try:
            year = int(tds[2].getText())
        except TypeError as e:
            logger.error(e)
            logger.error(f"No year data for {title}")
            year = None
        except ValueError as e:
            logger.error(e)
            logger.error(f"Invalid year data for {title}")
            year = None

        try:
            rating = int(tds[7].getText())
        except (ValueError, TypeError):
            logger.error(f"No rating data for {title}")
            rating = None

        date = dt.strptime(tds[-1].getText(), "%H:%M:%S %d.%m.%Y")
        date_formatted = dt.strftime(date, "%Y-%m-%d")

        return Film(title, year, rating, date_formatted)

    except ValueError as e:
        logger.error(e)
        return None

    except Exception as e:
        logger.error(e)
        logger.error(f"Parsing failed for {row}")
        return None


def load_kinopoisk_data(kinopoisk_filename: str) -> List[Film]:
    logger.info(f"Loading Kinopoisk data from {kinopoisk_filename}")
    with open(kinopoisk_filename, encoding="cp1251") as file:
        page = file.read()
        soup = BeautifulSoup(page, "html.parser")
        logger.info("Kinopoisk table loaded and parsed")
        table = soup.find("table")
        films_count = len(table.findAll("tr")) - 1
        logger.info(f"{films_count} films in table")

        films = (parse_kinopoisk_row(row) for row in table.findAll("tr")[1:])
        parsed_films = [f for f in films if f]
        logger.info(
            f"{len(parsed_films)} out of {films_count} films parsed successfully"
        )
        return parsed_films


def add_imdb_id(imdb: TitleFinder, film: Film) -> Film:
    imdb_id = imdb.find_id(film.title, film.year)
    if imdb_id:
        film.imdb_id = imdb_id
    return film


def export_to_csv(films_with_ids: List[Film]):
    logger.info("Exporting to CSV")
    with open("letterbox.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(("imdbID", "Title", "Year", "WatchedDate", "Rating10"))
        for film in films_with_ids:
            writer.writerow(
                (film.imdb_id, film.title, film.year, film.date, film.rating)
            )
    logger.info("Exported to CSV")


if __name__ == "__main__":
    if not sys.argv[1]:
        logger.error("Please provide a kinopoisk file to parse")
        sys.exit(1)

    kinopoisk_filename = sys.argv[1]
    films = load_kinopoisk_data(kinopoisk_filename)

    download_imdb_titles_file()
    imdb = build_title_finder()

    films_with_ids = [add_imdb_id(imdb, film) for film in films]
    export_to_csv(films_with_ids)
