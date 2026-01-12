# EdgeProp Scraper

Scrapes property listings from EdgeProp.sg.

## Usage

```bash
python edgeprop_scraper_v1.py --purpose <sale|rental> --type <hdb|condo|landed> [options]
```

## Parameters

| Argument      | Required | Description                                    | Default |
| :------------ | :------: | :--------------------------------------------- | :------ |
| `--purpose`   |   Yes    | Transaction type: `sale` or `rental`.          | -       |
| `--type`      |   Yes    | Property type: `hdb`, `condo`, or `landed`.    | -       |
| `--max-pages` |    No    | Maximum number of pages to scrape.             | `100`   |
| `--headless`  |    No    | Run browser in background (no visible window). | `False` |

## Example

```bash
# Scrape first 5 pages of Condo Sales in headless mode
python edgeprop_scraper_v1.py --purpose sale --type condo --max-pages 5 --headless
```

## Dependencies

Please install the required packages from the project root:

```bash
pip install -r ../requirements.txt
playwright install chromium
```
