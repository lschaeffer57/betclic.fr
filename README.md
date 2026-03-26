# Betclic — cotes (gRPC-Web)

Scraping des cotes Betclic via `offering.begmedia.com` (voir `betclic_fast.py`).

## Local

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

## Push sur GitHub

```bash
cd /chemin/vers/ce/dossier
git init
git add .
git commit -m "Betclic scraper + Railway"
git branch -M main
git remote add origin https://github.com/VOTRE_USER/VOTRE_REPO.git
git push -u origin main
```

## Railway (depuis GitHub)

1. [railway.app](https://railway.app) → **New project** → **Deploy from GitHub repo** → choisir ce dépôt.
2. Créer un service **sans** exposer de port HTTP (worker / commande personnalisée). Railway lit `railway.toml` : démarrage `python betclic_fast.py loop 90`.
3. Dans **Variables**, ajouter au minimum :
   - `PYTHONUNBUFFERED` = `1` (logs en temps réel)
4. Optionnel : `BETCLIC_LOOP_INTERVAL`, `BETCLIC_OUTPUT_FILE`.

**Volume** : le fichier `betclic_odds.json` est réécrit à chaque cycle. Pour le conserver entre redéploiements, montez un volume Railway et pointez `BETCLIC_OUTPUT_FILE` vers un chemin sur ce volume.

Python **3.11** est indiqué dans `runtime.txt` (Nixpacks / Railway).

**Plan** : un service qui tourne en boucle doit rester actif (éviter les offres qui mettent le conteneur en veille).

## Modes CLI

- `map` — mapping des marchés  
- `scrape` — scrape une fois → JSON  
- `all` — map + scrape  
- `loop [secondes]` — scrape en continu  
