# Usenet rehydration for esCorpiusDialog 1.3

The new standalone adapter, common runtime, metadata/index helpers and synthetic tests are MIT licensed by LICENSE-hydration-MIT.txt. Pre-existing modules have no tracked licence in the inspected Git history and are not relicensed or imported by this adapter. It independently implements the inspected algorithm, including historical MIME/CLD2/cleaning behaviour. No alternate conversation graph is reconstructed.

Place usenet-dehydrated-v3.zip and usenet-companions-1.3.zip from the new distribution in this directory:

```bash
unzip usenet-companions-1.3.zip
python3 -m venv .hydration-venv
.hydration-venv/bin/python -m pip install -r requirements-hydration.txt
.hydration-venv/bin/python rehydrate_usenet.py --archive usenet-dehydrated-v3.zip --verification usenet-companions/verification.json --source-manifest usenet-companions/source-selection.json --container es.eunet.spanish-tex --source archive --dry-run
.hydration-venv/bin/python -m unittest discover -s tests_hydration -v
```

Use CPython 3.11. See the release REHYDRATION.md for exact local-mbox and selected archive commands, transfer/expansion bounds, caching, source metadata refresh, resumability and the scopes of exact verification. Default mode is network free; --execute enables the selected source operation. Respect any requirement for a human operator to initiate source connections.

The source-selection manifest enumerates actual selected exports; no broad es.* search is substituted. Message-ID hashes retain brackets and distinguish absent from empty headers. The private one-to-many SQLite index retains duplicate matches. Historical first-row selection is explicit, conflicts and missing messages are retained, and partial chains are never emitted as complete dialogues. Header identity, recovered text, historically cleaned candidates, complete-chain reconstruction and original-file fingerprint equality are distinct outcomes. Downloaded messages and native headers remain outside release staging.
