# Betclic (gRPC-Web)

Scraper prematch pour [Betclic.fr](https://www.betclic.fr/) via l’API gRPC-Web `offering.begmedia.com`.

## Usage

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python betclic.py
```

Génère `output.json` à la racine du projet. Options : `--debug`, `--supabase` (valuebot), `--check-json [fichier]` (vérifie l’absence de doublons par match, code 0 si OK).

## Développement

```bash
pip install -r requirements-dev.txt
pytest tests/
```
