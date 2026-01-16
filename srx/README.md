# SRX Scraper

Advanced scraper for SRX with support for concurrency, specific towns, and fail-safe CSV writing.

## Usage

```bash
python srx_data_scraper_6.py --purpose <sale|rent|both> [options]
```

## Parameters

| Argument            | Required | Description                                                 | Default         |
| :------------------ | :------: | :---------------------------------------------------------- | :-------------- |
| `--purpose`         |    No    | Transaction type: `sale`, `rent`, or `both`.                | `rent`          |
| `--town`            |    No    | Scrape a specific Town ID (e.g., `26`).                     | -               |
| `--towns`           |    No    | List/Range of Town IDs to scrape (e.g., `1-28` or `1,3,5`). | `1-28`          |
| `--out`             |    No    | Custom output directory path.                               | `~/Desktop/...` |
| `--concurrency`     |    No    | Number of parallel browser contexts/tabs.                   | `6`             |
| `--max-pages`       |    No    | Max pages to scrape per town.                               | `None` (All)    |
| `--img-block`       |    No    | Block images/media to speed up scraping.                    | `False`         |
| `--headless`        |    No    | Run browser in background.                                  | `False`         |
| `--retries`         |    No    | Number of retries per page load failure.                    | `5`             |
| `--retry-forever`   |    No    | Keep retrying indefinitely on failure.                      | `False`         |
| `--nav-timeout-ms`  |    No    | Navigation timeout in milliseconds.                         | `30000`         |
| `--wait-timeout-ms` |    No    | Selector wait timeout in milliseconds.                      | `6000`          |

## Example

```bash
# Scrape Sales for Towns 1 to 5 with 3 parallel tabs, max 2 pages each
python srx_data_scraper_6.py --purpose sale --towns "1-5" --concurrency 3 --max-pages 2 --headless
```

## Dependencies

Please install the required packages from the project root:

```bash
pip install -r ../requirements.txt
playwright install chromium
```
