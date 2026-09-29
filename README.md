# nomescenso2022

Scraper + full dataset for the **IBGE "Nomes no Brasil"** tool of the 2022 Census:
<https://censo2022.ibge.gov.br/nomes>

## Data (JSONL)

The complete Brazilian name/surname rankings of Censo 2022, one JSON object per line,
in official ranking order:

| File | Rows | Description |
| --- | ---: | --- |
| `data/nomes_censo2022_nomes.jsonl` | **128,458** | all first names |
| `data/nomes_censo2022_sobrenomes.jsonl` | **201,601** | all surnames |

```json
{"rank": 1, "nome": "maria", "frequencia": 12284478, "percent": 6.0491}
{"rank": 1, "nome": "silva", "frequencia": 34030104, "percent": 16.7569}
```

Fields: `rank` (ties share a rank and the next rank skips, e.g. two names at
rank 84 then rank 86), `nome` (lowercase, accent-stripped, as in the API),
`frequencia` (number of people counted with that name), `percent`
(frequencia / 203,080,756 residents × 100, rounded to 4 decimals).

Want just the top 10,000? Take the first 10,000 lines — e.g.
`head -n 10000 data/nomes_censo2022_nomes.jsonl`.

## The scraper

`ibge_nomes_scraper.py` — Python 3.10+, **stdlib only**. Three interchangeable modes:

```bash
# 1) Live: crawl the official API directly (30 names/page, paginated, retries, threads)
python3 ibge_nomes_scraper.py --tipo nome --max-names 10000 \
    --cache-dir cache/nome_pages -o nomes_top10000.jsonl
#    --tipo nome|sobrenome, --localidade 0 (= Brasil) or any IBGE locality code,
#    --cache-dir keeps every raw API page on disk (resume-friendly);
#    omit --max-names to fetch the entire ranking (4,282 pages for nomes).

# 2) Offline: merge previously cached raw page files (JSON or markdown-fenced JSON)
python3 ibge_nomes_scraper.py --tipo nome --from-cache --cache-dir cache/nome_pages \
    -o nomes.jsonl

# 3) Offline: rebuild the full ranking from a SQLite dump of the API
#    (table frequencias(id_local, nome, tipo_nome, ano, frequencia),
#     e.g. github.com/alexsantee/nomes_do_brasil_2022, MIT)
python3 ibge_nomes_scraper.py --tipo sobrenome --from-sqlite nomes.sqlite \
    -o sobrenomes.jsonl
```

API behind the site:

```
GET https://servicodados.ibge.gov.br/api/v3/nomes/2022/localidade/{localidade}/ranking/{nome|sobrenome}?page={n}
```

30 items per page (server-imposed), `totalPages`-driven pagination.

## Provenance & verification

* Ranking semantics were reproduced exactly from the live API: ordering is
  `frequencia DESC`, ties broken by `nome DESC` — verified against 1,728
  tie-groups observed in a page-by-page crawl of the API.
* The first 10,000 rows of each file are **byte-identical** to a direct
  page-by-page crawl of the official API; row counts match the API's `count`
  field exactly (128,458 nomes / 201,601 sobrenomes).
* Live anchor checks performed against the API this session (ranks 1–330 for
  nomes, top-30 sobrenomes, including the rank-84 `sonia`/`simone` tie): all passed.
* Rows beyond the top 10,000 were rebuilt from a public SQLite dump of the same
  API ([alexsantee/nomes_do_brasil_2022](https://github.com/alexsantee/nomes_do_brasil_2022))
  after verifying its top-10,000 prefix is byte-identical to the direct crawl.
* IBGE suppresses names with fewer than 20 bearers, hence the minimum
  `frequencia` of 20 at the end of each ranking.
