"""Scrape explainxkcd.com"""

from pathlib import Path
import itertools
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from yarl import URL
from tqdm import tqdm

# For some reason the first 500 comics are not listed on the full page
BASE_URL = URL("https://www.explainxkcd.com")
COMICS_500 = URL("https://www.explainxkcd.com/wiki/index.php/List_of_all_comics_(1-500)")
COMICS_ALL = URL("https://www.explainxkcd.com/wiki/index.php/List_of_all_comics_(full)")
DATA_PATH = Path(__file__).parent / 'data' / 'xkcd'
ENABLE_TQDM = True


def _nop(it, *a, **k):
    return it


if not ENABLE_TQDM:
    tqdm = _nop


def gather_wiki_links(page_url):
    """For a particular explanation table page gather the table of links."""
    resp = requests.get(page_url)
    soup = BeautifulSoup(resp.content, features="html.parser")
    table = soup.find("table", {"class": "wikitable"})
    rows = table.find_all("tr")

    data = []
    for tr in rows[1:]:
        td = tr.find_all(['td', 'th'])
        row = [
            URL(tr.a.get('href', ''))
            if tr.find('a') is not None
            else tr.text.strip()
            for tr in td
        ]
        # Add the title text in addition to auto convert of URL
        if len(td) > 1:
            row.append(td[1].a.get_text().strip())
        data.append(row)

    return pd.DataFrame(data, columns=['xkcd', 'Title', 'Talk', 'Image', 'Date', 'TitleText'])


def gather_all_links():
    """Amalgamate all of the links to xkcd explanation pages."""
    # Gather the data
    first_500 = gather_wiki_links(COMICS_500)
    remaining = gather_wiki_links(COMICS_ALL)
    all_links = pd.concat([first_500, remaining], ignore_index=True)

    # Data cleaning
    all_links = all_links[~all_links.Title.isnull()]
    # Use YARL to convert relative URLs to absolute URLs
    fix_url_cols = ['Title', 'Image']
    all_links[fix_url_cols] = all_links[fix_url_cols].applymap(lambda x: BASE_URL.join(x))
    # Due to a bug with this page: https://explainxkcd.com/wiki/index.php/259 (there is an accent mark in the title)
    # we need to strip away the text after the colon to actually reach the page
    all_links.Title = all_links.Title.apply(lambda url: url.with_path(url.path.split(':')[0]))
    # Convert ISO date stamp to datetime object (Unknown to NaT)
    all_links.Date = pd.to_datetime(all_links.Date, errors='coerce')
    all_links.drop('Talk', axis=1, inplace=True)
    # Sort by xkcd number, extract the number from the xkcd link
    all_links.sort_values(
        'xkcd',
        inplace=True,
        key=lambda series: series.apply(lambda x: int(x.path[1:]))
    )
    # Convert all dtypes to string
    all_links = all_links.astype('string')

    return all_links


def get_paragraphs_below_header(soup, header_text):
    """Collect the paragraphs directly underneath a header."""
    # regex to match headers 1-6
    header = re.compile('^h[1-2]$')
    # We need to use a lambda here because we are checking the contents of the header which is within a span
    # nested inside the header
    header_tag = soup.find(lambda tag: header.match(tag.name) and re.match(header_text, tag.get_text()))
    # This page does not have a Transcript section
    # https://www.explainxkcd.com/wiki/index.php/1116:_Traffic_Lights
    body_contents = ''
    if header_tag is not None:
        # This is a little complicated list comprehension that iterates across the siblings of the header (i.e. the
        # paragraphs tags below it) until it encounters a header which tells itertools to stop. The text in these tags
        # are extracted and joined.
        body_contents = ''.join([
            tag.get_text()
            for tag in itertools.takewhile(lambda sibling: not header.match(sibling.name),
                                           header_tag.find_next_siblings())
        ])

    return body_contents


def process_page_contents(page_response):
    """Get the explanation and transcript of an xkcd explanation page."""
    page_soup = BeautifulSoup(page_response.content, features="html.parser")
    try:
        explanation = get_paragraphs_below_header(page_soup, r'Explanations?')
        transcript = get_paragraphs_below_header(page_soup, r'Transcript')
    except AttributeError:
        print(page_response.url)
        raise Exception("It's borked")

    return page_response.url, explanation, transcript


def get_page(url):
    sess = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    sess.mount('http://', HTTPAdapter(max_retries=retries))
    return sess.get(url)


def get_all_page_contents(links_list):
    with ThreadPoolExecutor(max_workers=50) as pool:
        page_responses = list(tqdm(
            pool.map(get_page, links_list),
            desc="Getting page",
            total=len(links_list)
        ))

    with ProcessPoolExecutor() as pool:
        processed_pages = list(tqdm(
            pool.map(process_page_contents, page_responses),
            desc="Processing page",
            total=len(page_responses)
        ))

    return pd.DataFrame(processed_pages, columns=['Title', 'Explanation', 'Transcript'])


if __name__ == "__main__":
    fetch_links = False
    fetch_pages = False
    # Notes: I ended up using parquet because hdf5 does not play nice with the "string" dtype and
    #        dask processing of csv does not play well with multiline strings
    links_df_path = DATA_PATH / 'links_df.parquet'
    pages_df_path = DATA_PATH / 'pages_df.parquet'

    if fetch_links or not links_df_path.exists():
        links_df = gather_all_links()
        links_df.to_parquet(links_df_path)
    else:
        links_df = pd.read_parquet(links_df_path)

    if fetch_pages or not pages_df_path.exists():
        exp_links = links_df["Title"].tolist()
        pages_df = get_all_page_contents(exp_links)
        pages_df.to_parquet(pages_df_path, index=True)
    else:
        pages_df = pd.read_parquet(pages_df_path, index=True)
