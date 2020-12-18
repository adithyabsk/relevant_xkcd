# Relevant xkcd

Inspiration: https://twitter.com/adithya_balaji/status/1330287375327563781?s=20

## Instructions

Tested on python 3.8

- Setup virtualenv / install requirements
- Run `get_xkcd_data.py`
- [Setup GoogleCloudAPI Credentials](https://cloud.google.com/bigquery/docs/reference/libraries#setting_up_authentication)
- Add credentials to `~/.config/gcloud_creds.json`
- Run `get_reddit_data.py`

    - NOTE: run this at your own risk (it should fall under the free tier, but no
      guarantees)

## Plan

- [x] Get Data
- [ ] Figure out how to get main topics of a random piece of text
  * Use this to be able to benchmark eventual solution
- [ ] Look into search metrics for suggestion relevance which you can use to
measure the later steps
- [ ] Build simple version of idea that uses just cosine similarity with title
- [ ] Build more complex version that uses simple machine learning model to
predict relevance using NLP techniques
- [ ] Try GPT-3 semantic search approach
