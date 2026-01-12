# 99.co Scraper

Scrapes property listings from 99.co.

## Usage

```bash
python data_scraper_99co.py --purpose <sale|rent|both> [options]
```

## Parameters

| Argument      | Required | Description                                     | Default |
| :------------ | :------: | :---------------------------------------------- | :------ |
| `--purpose`   |   Yes    | Transaction type: `sale`, `rent`, or `both`.    | -       |
| `--max-pages` |    No    | Maximum number of pages to scrape per category. | `9999`  |
| `--headless`  |    No    | Run browser in background.                      | `False` |

## Example

```bash
# Scrape first 10 pages of both Sale and Rent listings
python data_scraper_99co.py --purpose both --max-pages 10 --headless
```

## Dependencies

Please install the required packages from the project root:

```bash
pip install -r ../requirements.txt
playwright install chromium
```
