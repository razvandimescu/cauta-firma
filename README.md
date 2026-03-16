# Cauta Firma

Cauta in peste 4 milioane de companii din Romania — cu date financiare ANAF in timp real. Gratuit, open source, gazduit integral pe Cloudflare (free tier).

**Live:** https://cauta-firma.ro

## Stack

- **Frontend** — single-page app (`public/index.html`), no build step
- **Backend** — Cloudflare Worker (`src/worker.js`)
- **Database** — Cloudflare D1 (serverless SQLite, ~4.2M companies)

## Features

- Autocomplete search by company name or CUI
- Company profile: status, legal form, address, EUID, incorporation date, website
- All authorized CAEN codes with descriptions
- Live financial data from ANAF (revenue, profit, employees, full balance sheet)
- Year-over-year comparison
- Similar companies (same CAEN + county)

## Data Sources

- **ONRC Open Data** — https://data.gov.ro/dataset/lista-firme-din-registrul-comertului — bulk CSVs with all registered Romanian companies
- **ANAF Bilant API** — `https://webservicesp.anaf.ro/bilant?an=YEAR&cui=CUI` — live financial indicators (free, no auth)

## D1 Schema

| Table | Rows | Source |
|---|---|---|
| `firme` | 4,267,278 | od_firme.csv |
| `stare_firma` | 4,546,912 | od_stare_firma.csv |
| `caen_autorizat` | 8,833,781 | od_caen_autorizat.csv (max 5 per company) |
| `n_caen` | 2,392 | n_caen.csv |
| `n_stare` | 197 | n_stare_firma.csv |

## Deployment

```bash
npx wrangler deploy
```

## Rebuilding D1 from scratch

1. Download CSVs from data.gov.ro into the project root:
   - `od_firme.csv`, `od_stare_firma.csv`, `od_caen_autorizat.csv`
   - `n_stare_firma.csv`, `n_caen.csv`

2. Generate SQL chunks:
   ```bash
   python3 prepare_d1.py            # firme, stare, nomenclators
   python3 prepare_caen_slim.py     # caen (capped at 5 per company)
   ```
   Output goes to `sql_chunks/` (~223 files, each <10MB for D1 limits).

3. Apply schema then seed:
   ```bash
   npx wrangler d1 execute company-lookup-db --remote --file=schema.sql

   for f in sql_chunks/*.sql; do
     echo "Importing $f..."
     npx wrangler d1 execute company-lookup-db --remote --file="$f"
   done
   ```
   Full import takes ~30 min. Transient `D1_RESET_DO` errors can be retried.

## API Endpoints

| Endpoint | Params | Description |
|---|---|---|
| `/api/search` | `q` | Name (LIKE) or CUI (exact) search, returns top 10 |
| `/api/company` | `cui` | Full company profile + CAEN codes |
| `/api/bilant` | `cui`, `an` | Proxies ANAF bilant API (with retry) |
| `/api/similar` | `cui`, `caen`, `judet` | 8 random active companies with same CAEN in same county |

## Known Issues

- ANAF bilant proxy may return 520 errors from Cloudflare Workers (ANAF's infrastructure is unreliable). Retry logic is in place (3 attempts with backoff).

## License

MIT
