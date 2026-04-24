"""Generador de test packets para Fase 1 (validación empírica controlada).

Uso:
    uv run python f1/packets/generate_packets.py
"""
from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    """Escribe los test packets JSON en el directorio del script."""
    out_dir = Path(__file__).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    packets = [
        {
            "packet_id": "XP_DIP_SITL_LXVI_UNANIME",
            "source_tag": "dip_sitl",
            "era": "LXVI",
            "method": "GET",
            "url": "http://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonplxvi.php?numero=1",
            "url_pattern": None,
            "parameters": {},
            "repetitions": 2,
            "expected_count": None,
            "expected_schema": ["counts", "nominal"],
            "risks_covered": ["encoding", "estructura_tabla"],
            "pending_manual_capture": False,
            "pending_note": "",
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_DIP_SITL_LXVI_DIVIDIDA",
            "source_tag": "dip_sitl",
            "era": "LXVI",
            "method": "GET",
            "url": "http://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?numero=2",
            "url_pattern": None,
            "parameters": {},
            "repetitions": 2,
            "expected_count": None,
            "expected_schema": ["counts", "nominal"],
            "risks_covered": ["conteo_agregado", "nominal"],
            "pending_manual_capture": False,
            "pending_note": "",
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_DIP_GACETA_LXVI_20251210",
            "source_tag": "dip_gaceta",
            "era": "LXVI",
            "method": "POST",
            "url": "https://gaceta.diputados.gob.mx/voto66/ordi21/lanordi21.php3",
            "url_pattern": None,
            "parameters": {
                "evento": "1|82",
                # Valores de nomtit/lola provenientes del probe del analista
                "nomtit": "Dictamen de las Comisiones Unidas de ...",
                "lola[11]": "Seleccion",
            },
            "repetitions": 2,
            "expected_count": None,
            "expected_schema": ["metadata", "group_sentido"],
            "risks_covered": ["metadata", "grupo_x_sentido"],
            "pending_manual_capture": False,
            "pending_note": (
                "Endpoint confirmado por analista 2026-04-23. "
                "POST a lanordi21.php3 con payload x-www-form-urlencoded."
            ),
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_SEN_LXVI_INDEX",
            "source_tag": "senado_lxvi_html",
            "era": "LXVI",
            "method": "GET",
            "url": "https://www.senado.gob.mx/66/votacion",
            "url_pattern": None,
            "parameters": {},
            "repetitions": 2,
            "expected_count": None,
            "expected_schema": ["metadata", "counts", "nominal"],
            "risks_covered": ["estructura_lxvi", "jsessionid"],
            "pending_manual_capture": False,
            "pending_note": "",
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_SEN_LXVI_VOT_5001",
            "source_tag": "senado_lxvi_html",
            "era": "LXVI",
            "method": "GET",
            "url": "https://www.senado.gob.mx/66/votacion/5001",
            "url_pattern": None,
            "parameters": {},
            "repetitions": 3,
            "expected_count": None,
            "expected_schema": ["metadata", "counts", "nominal"],
            "risks_covered": ["conteo_agregado", "nominal"],
            "pending_manual_capture": False,
            "pending_note": "",
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_SEN_HIST_LXIV_INDEX",
            "source_tag": "senado_historico_lxiv",
            "era": "LXIV",
            "method": "GET",
            "url": None,
            "url_pattern": "https://www.senado.gob.mx/{era_lower}/votacion",
            "parameters": {"era_lower": "64"},
            "repetitions": 3,
            "expected_count": None,
            "expected_schema": ["classification", "counts", "metadata"],
            "risks_covered": ["waf_cache", "fragmentacion_era"],
            "pending_manual_capture": True,
            "pending_note": (
                "URL de índice histórico no verificada para LXIV. "
                "Puede requerir URL distinta por legislatura o redirección. "
                "Requiere captura manual de Network para confirmar endpoint."
            ),
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_SEN_LXVI_AJAX_5001",
            "source_tag": "senado_lxvi_ajax",
            "era": "LXVI",
            "method": "GET",
            "url": "https://www.senado.gob.mx/66/app/votaciones/functions/viewTableVot.php?action=ajax&cell=1&order=DESC&votacion=5001&q=",
            "url_pattern": None,
            "parameters": {},
            "headers": {
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.senado.gob.mx/66/votacion/5001",
            },
            "repetitions": 3,
            "expected_count": None,
            "expected_schema": ["metadata", "counts", "nominal"],
            "risks_covered": ["ajax_oculto", "estructura_tabla"],
            "pending_manual_capture": False,
            "pending_note": (
                "Endpoint confirmado por analista 2026-04-23. "
                "AJAX GET con headers Referer y X-Requested-With."
            ),
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_DIP_SITL_LXVI_DETALLE",
            "source_tag": "dip_sitl",
            "era": "LXVI",
            "method": "GET",
            "url": "https://sitl.diputados.gob.mx/LXVI_leg/listados_votacionesnplxvi.php?partidot=14&votaciont=2",
            "url_pattern": None,
            "parameters": {},
            "repetitions": 2,
            "expected_count": 255,
            "expected_schema": ["counts", "nominal"],
            "risks_covered": ["nominal", "encoding", "estructura_tabla"],
            "pending_manual_capture": False,
            "pending_note": (
                "Detalle nominal validado por analista 2026-04-23. "
                "URL corregida con sufijo nplxvi.php. Requiere User-Agent de navegador."
            ),
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
        {
            "packet_id": "XP_DIP_SITL_LXVI_AGREGADO",
            "source_tag": "dip_sitl",
            "era": "LXVI",
            "method": "GET",
            "url": "https://sitl.diputados.gob.mx/LXVI_leg/estadistico_votacionnplxvi.php?votaciont=2",
            "url_pattern": None,
            "parameters": {},
            "repetitions": 2,
            "expected_count": None,
            "expected_schema": ["counts"],
            "risks_covered": ["conteo_agregado", "estructura_tabla"],
            "pending_manual_capture": False,
            "pending_note": "Agregado por partido validado por analista 2026-04-23.",
            "created_by": "ingeniero_f1",
            "created_at": "2026-04-23",
        },
    ]

    for pkt in packets:
        out_path = out_dir / f"{pkt['packet_id']}.json"
        out_path.write_text(
            json.dumps(pkt, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"Generated {out_path}")

    print(f"\nTotal packets generated: {len(packets)}")


if __name__ == "__main__":
    main()
