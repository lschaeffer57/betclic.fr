# Betclic — cotes (gRPC-Web)

Scraping des cotes Betclic via `offering.begmedia.com` — script **`betclic_fast.py`**.

## Local

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python betclic_fast.py all          # mapping + scrape une fois
python betclic_fast.py loop 90      # boucle toutes les 90 s
```

| Variable | Exemple | Rôle |
|----------|---------|------|
| `BETCLIC_MODE` | `loop` | Si pas d’arguments CLI |
| `BETCLIC_LOOP_INTERVAL` | `120` | Secondes entre deux cycles (mode loop) |
| `BETCLIC_OUTPUT_FILE` | `/data/odds.json` | Fichier JSON de sortie |

## Railway

1. [railway.app](https://railway.app) → déployer ce dépôt depuis GitHub.
2. `railway.toml` lance : `python betclic_fast.py loop 90`.
3. Variable : `PYTHONUNBUFFERED=1` pour les logs en direct.
4. Volume optionnel + `BETCLIC_OUTPUT_FILE` pour conserver `betclic_odds.json`.

Python **3.11** : `runtime.txt`.

## Modes CLI

| Commande | Effet |
|----------|--------|
| `map` | Mapping des marchés |
| `scrape` | Scrape une fois → JSON |
| `all` | Map + scrape |
| `loop [secondes]` | Scrape en continu |
