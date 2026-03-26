# Betclic — cotes (gRPC-Web)

Scraping des cotes Betclic via `offering.begmedia.com`.

## Script principal : `betclic_fast.py`

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python betclic_fast.py all          # mapping + scrape une fois
python betclic_fast.py loop 90      # boucle toutes les 90 s
```

Variables optionnelles :

| Variable | Exemple | Rôle |
|----------|---------|------|
| `BETCLIC_MODE` | `loop` | Si pas d’arguments CLI |
| `BETCLIC_LOOP_INTERVAL` | `120` | Secondes entre deux cycles (mode loop) |
| `BETCLIC_OUTPUT_FILE` | `/data/odds.json` | Fichier JSON de sortie |

## Railway (depuis GitHub)

1. [railway.app](https://railway.app) → **New project** → **Deploy from GitHub repo** → ce dépôt.
2. Railway lit `railway.toml` : `python betclic_fast.py loop 90`.
3. Variable : `PYTHONUNBUFFERED=1` pour les logs.
4. Volume optionnel + `BETCLIC_OUTPUT_FILE` pour conserver le JSON.

Python **3.11** : `runtime.txt`.

## Ancien scraper `betclic.py` (optionnel)

```bash
pip install -r requirements.txt
python betclic.py
```

Génère `output.json`. Développement : `pip install -r requirements-dev.txt` puis `pytest tests/`.

## Modes CLI (`betclic_fast.py`)

- `map` — mapping des marchés  
- `scrape` — scrape une fois → JSON  
- `all` — map + scrape  
- `loop [secondes]` — scrape en continu  
