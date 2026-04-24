import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

from scraper.engine import HTTPScraperEngine
from scraper.cli import DryRunPersistence
from scraper.pipeline import process
from scraper.sources.sitl_lxvi import _AGGREGATE_URL, _NOMINAL_URL, _SOURCE_TAG

with HTTPScraperEngine(config_path='f1/config/xp_config.toml') as engine:
    # Votacion 137
    votacion_id = "137"
    agg_url = _AGGREGATE_URL.format(votacion_id=votacion_id)
    agg_fetch = engine.fetch(agg_url, method="GET", source_tag=_SOURCE_TAG)
    agg_proc = process(agg_fetch, _SOURCE_TAG)
    print(f"agg_proc: class={agg_proc.classification}, counts={agg_proc.counts}")

    nominal_procs = []
    for partidot in range(1, 31):
        nom_url = _NOMINAL_URL.format(partidot=partidot, votacion_id=votacion_id)
        nom_fetch = engine.fetch(nom_url, method="GET", source_tag=_SOURCE_TAG)
        nom_proc = process(nom_fetch, _SOURCE_TAG)
        print(f"partidot={partidot}: class={nom_proc.classification}, casts={len(nom_proc.casts)}")
        if nom_proc.classification == "INDETERMINATE":
            continue
        if not nom_proc.casts:
            continue
        nominal_procs.append(nom_proc)

    all_casts = [cast for proc in nominal_procs for cast in proc.casts]
    print(f"Total nominal_procs: {len(nominal_procs)}, total casts: {len(all_casts)}")
    if agg_proc.parsed_data and agg_proc.parsed_data.get("counts") and nominal_procs:
        from shared.transform_bridge import validate_counts_vs_nominal
        counts_dict = agg_proc.parsed_data["counts"]
        all_nominal = [{"sentido": cast["sentido"]} for cast in all_casts]
        validation = validate_counts_vs_nominal(counts_dict, all_nominal)
        print(f"Validation: {validation}")
