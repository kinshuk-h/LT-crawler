# LT-Crawler

The LT-Crawler project is developed for the task of curation of a dataset of paragraphs
Legal Documents (Judgments) from the websites of Indian Courts.

## Requirements

- Python, v3.7 or newer
- Module requirements, as given in the included `requirements.txt` file.

## Structure

The project is organized into the following hierarchy:

- `config`: Configuration and credential files for usage of proprietory APIs (such as the [Adobe API](AdobePDFExtractAPI.md)).
- `data`: Default location for downloaded Judgments and generated JSON files.
  - `judgments`: Default location for storing downloaded judgment files.
    - `<court> Judgments`: Judgments pertaining to a specific court.
      - `extracted_<extractor>`: Text extraction results for corresponding judgment files
  - `json`: JSON data describing downloaded judgments, search parameters and extracted paragraphs.
  - `logs`: Log files of executions and function invocations.
- `dumps`: Temporary generated files and results for various operations.
- `src`: Main source code for various modules of the pipeline:
  - `retrievers`: Module for retrievers, responsible for extracting information from websites
  - `extractors`: Module for extractors, responsible for extracting text from collected judgments
  - `segregators`: Module for segregators, responsible for segregating extracted text into paragraph units
  - `filters`: Module for filters, responsible for filtering undesirable paragraphs from the generated paragraph units
  - `pipeline`: Module implementing the phases of the pipeline, providing dedicated functionality for phase-specific tasks via sub-modules.
  - `scripts`: Scripts for testing module functionality and benchmarking.
- `paracurate.py`: Main script implementating the curation pipeline, utilizing the developed modules.
- `tests`: Bundles files implementating unit and functional tests for modules.
  - `data`: Sample data to use for testing. Follows the same structure as the core `data` directory.

## Data Description

The curated dataset, generated as a set of JSON files, comprises of the following information:

- Judgment Title, usually of form `<PETITIONER> VS <RESPONDENT>`
- Judgment Metadata: Court, Case Number, Date of Judgment, etc.
- Link to the judgment document, available online
- A list of paragraphs extracted from the judgment, with each paragraph described by:
  - Paragraph Number, relative to the document,
  - Page Number, based on where the paragraph starts from,
  - Reference, indicating the reference to the paragraph in the document (may not be the same as paragraph number)
  - Paragraph Content

## Utilized Search Terms for Curation (suggested by Experts from the Law Faculty)

- Patents
- Copyrights
- Licensing
- Trademarks
- Infringement
- Industrial Design
- Design
- Geographical Indications
- Trade Secrets

## Caveats

- Some documents cannot be processed even by the Adobe API, so as a result the corresponding paragraph
  results in the JSON files may not be present.
- Some extractors do not work without the specification of the required parameters
  (e.g.: `--adobe-credentials` for the `adobe_api` extractor).

## Helper Scripts (under `src/scripts`)

- `validate_docs.py`: This script validates downloaded judgment files with reference to dataset JSON files,
                      and can redownload missing files or remove unused files.  
    Example Usage:

    ```powershell
    python3 src/scripts/validate_docs.py --fix-missing --remove-unused
    ```

- `test_dhc_doc_urls.py`: This script demonstates the inconsistent results returned by the DHC website, by downloading 4 judgments via different URLs whose PDFs have different hashes. Post extraction, the text content of all PDFs is the same.
- `get_data_stats.py`: This script generates a markdown compatible table of statistics for the dataset, comprising of aggregate and query-wise information about collected judgments. The information includes judgment frequency, paragraph frequency, etc.

## Examples

- Generate paragraphs from the results on the first 10 pages for the search term 'trade marks' over the website of the Delhi High Court, using only the Adobe API extractor, bypassing the `sent_count` filter and skipping results already generated:

  ```powershell
  python3 paracurate.py "trade marks" --courts DHC --extractors adobe_api --adobe-credentials config/pdfservices-api-credentials.json --page 1 --pages 10 --skip-existing --sent-count-min-sents 0
  ```

- Generate paragraphs for the results from the first 2 pages of all the specified search terms over the website of the Delhi High Court, using only the Adobe API extractor, filtering paragraphs with less than 2 sentences AND less than 20 words, and skipping results already generated:

  ```powershell
  python3 paracurate.py "Patents" "Copyrights" "Infringement" "Licensing" "Industrial Design" "Trade Secrets" "Geographical Indications" "Design" "Trademarks" --courts DHC --extractors adobe_api --adobe-credentials "config/pdfservices-api-credentials.json" --page 1 --pages 2 --skip-existing --sent-count-min-sents 2 --sent-count-min-words 20
  ```

## Testing

Defined unit and functional tests can be executed via use of `pytest`:

  ```powershell
  pytest tests/
  ```

## Dataset

### Information

- Curated on: 2022-01-09
- Directory: [data/json/DHC Judgments](https://github.com/kinshuk-h/LT-crawler/tree/main/data/json/DHC%20Judgments)
- Curation command:

```powershell
python3 paracurate.py "Licensing" "Copyrights" "Patents" "Trademarks" "Infringement" "Industrial Design" "Design" "Geographical Indications" "Trade Secrets" --courts DHC --extractors adobe_api --adobe-credentials "config/pdfservices-api-credentials.json" --page 1 --pages 5 --skip-existing --sent-count-min-sents 2 --sent-count-min-words 20
```

### Statistics

- Per-query statistics:

|                          | pages | count | paragraphs | without_file | max_paragraphs | avg_paragraphs |
|--------------------------|-------|-------|------------|--------------|----------------|----------------|
|               Copyrights |   5   |  45   |    1055    |      12      |      137       |     23.444     |
|                   Design |   5   |  34   |    1342    |      0       |      192       |     39.471     |
| Geographical Indications |   3   |  21   |    894     |      4       |      165       |     42.571     |
|        Industrial Design |   1   |   8   |    491     |      1       |      136       |     61.375     |
|             Infringement |   5   |  14   |    607     |      0       |      137       |     43.357     |
|                Licensing |   5   |   5   |    543     |      0       |      248       |     108.6      |
|                  Patents |   5   |  42   |    1638    |      0       |      205       |      39.0      |
|            Trade Secrets |   5   |  36   |    1276    |      7       |      215       |     35.444     |
|               Trademarks |   5   |  32   |    910     |      0       |       78       |     28.438     |

- Aggregate statistics over all queries:

|       | pages | count | paragraphs | without_file | max_paragraphs | avg_paragraphs |
|-------|-------|-------|------------|--------------|----------------|----------------|
| total |  39   |  237  |    8756    |      24      |      248       |     36.945     |

## About

Created as a Minor Project for Masters in Computer Science at [Department of Computer Science](https://cs.du.ac.in), [University of Delhi](https://du.ac.in)
