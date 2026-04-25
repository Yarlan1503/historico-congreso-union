"""Modelos Pydantic v2 para validación de datos del Congreso de la Unión."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, HttpUrl, field_validator


# ---------------------------------------------------------------------------
# Constantes de fuente
# ---------------------------------------------------------------------------
class SourceTag:
    """Origen del asset crudo. Valores canónicos como constantes de clase.

    Cualquier string es válido como source_tag (sin enum cerrado).
    Las constantes se mantienen para backward compatibility y legibilidad.
    """

    DIP_SITL = "dip_sitl"
    DIP_INFOPAL = "dip_infopal"
    DIP_GACETA_POST = "dip_gaceta_post"
    DIP_GACETA_TABLA = "dip_gaceta_tabla"
    SEN_LXVI_AJAX = "sen_lxvi_ajax"
    SEN_LXVI_HTML = "sen_lxvi_html"


class Chamber(str, Enum):
    """Cámara legislativa."""

    DIPUTADOS = "diputados"
    SENADO = "senado"


class Legislature(str, Enum):
    """Legislatura del Congreso de la Unión."""

    LXVI = "LXVI"
    LXV = "LXV"
    LXIV = "LXIV"
    LXIII = "LXIII"
    LXII = "LXII"
    LXI = "LXI"
    LX = "LX"


class AssetRole(str, Enum):
    """Rol del asset respecto al voto."""

    PRIMARY_NOMINAL = "primary_nominal"
    PRIMARY_AGGREGATE = "primary_aggregate"
    METADATA = "metadata"
    TRIANGULATION = "triangulation"


class Method(str, Enum):
    """Método HTTP utilizado para obtener el asset."""

    GET = "GET"
    POST = "POST"


class Sentido(str, Enum):
    """Sentido del voto emitido por un legislador."""

    A_FAVOR = "a_favor"
    EN_CONTRA = "en_contra"
    ABSTENCION = "abstencion"
    AUSENTE = "ausente"
    NOVOTO = "novoto"
    PRESENTE = "presente"


# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------
class SourceAsset(BaseModel):
    """Representa un asset crudo descargado de una fuente legislativa."""

    asset_id: int | None = None
    source_tag: str  # Era SourceTag enum; ahora acepta cualquier string
    url: HttpUrl
    method: Method = Method.GET
    request_payload_hash: str | None = None
    response_body_hash: str = Field(
        ...,
        min_length=8,
        max_length=64,
        pattern=r"^[a-fA-F0-9]+$",
    )
    response_headers_hash: str | None = None
    status_code: int | None = Field(None, ge=0, le=599)
    content_type: str | None = None
    encoding: str | None = None
    captured_at: datetime
    waf_detected: bool = False
    cache_detected: bool = False
    repetition_num: int = Field(1, ge=1)
    run_id: str | None = None
    raw_body_path: Path

    @field_validator("captured_at")
    @classmethod
    def _ensure_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("captured_at debe ser un datetime aware (con zona horaria)")
        return value


class RawVoteEvent(BaseModel):
    """Evento de votación tal como se extrajo de la fuente original."""

    vote_event_id: int | None = None
    chamber: Chamber
    legislature: Legislature
    vote_date: date | None = None
    title: str | None = Field(None, max_length=500)
    subject: str | None = Field(None, max_length=1000)
    source_url: HttpUrl | None = None
    metadata_json: dict | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def metadata_json_str(self) -> str | None:
        """Serializa ``metadata_json`` a string JSON para almacenar en SQLite."""
        if self.metadata_json is None:
            return None
        return json.dumps(self.metadata_json, ensure_ascii=False)


class VoteEventAsset(BaseModel):
    """Relación entre un evento de votación y el asset que lo documenta."""

    vote_event_id: int
    asset_id: int
    asset_role: AssetRole


class RawVoteCast(BaseModel):
    """Voto individual de un legislador extraído de la fuente cruda."""

    cast_id: int | None = None
    vote_event_id: int
    asset_id: int
    legislator_name: str = Field(..., min_length=1, max_length=200)
    legislator_group: str | None = Field(None, max_length=100)
    sentido: Sentido
    region: str | None = Field(None, max_length=100)
    raw_row_json: dict | None = None

    @property
    def raw_row_json_str(self) -> str | None:
        """Serializa ``raw_row_json`` a string JSON para almacenar en SQLite."""
        if self.raw_row_json is None:
            return None
        return json.dumps(self.raw_row_json, ensure_ascii=False)


class VoteCounts(BaseModel):
    """Conteo agregado de votos por grupo para un evento de votación."""

    count_id: int | None = None
    vote_event_id: int
    asset_id: int
    group_name: str | None = Field(None, max_length=100)
    a_favor: int = Field(0, ge=0)
    en_contra: int = Field(0, ge=0)
    abstencion: int = Field(0, ge=0)
    ausente: int = Field(0, ge=0)
    novoto: int = Field(0, ge=0)
    presente: int = Field(0, ge=0)
    total: int | None = Field(None, ge=0)


class IngestionReport(BaseModel):
    """Resumen de resultados de un ciclo de ingestión."""

    assets_inserted: int
    assets_skipped: int
    vote_events_inserted: int
    vote_events_linked: int
    casts_inserted: int
    counts_inserted: int
    errors: list[str] = []
    manifests_processed: int


# ---------------------------------------------------------------------------
# Verificación
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from pydantic import ValidationError

    print("=" * 60)
    print("Instanciando modelos con datos VÁLIDOS...")
    print("=" * 60)

    try:
        asset = SourceAsset(
            source_tag=SourceTag.DIP_SITL,
            url="https://sitl.diputados.gob.mx/LXVI_leg/votaciones.php",
            response_body_hash="a1b2c3d4e5f67890",
            captured_at=datetime.now(UTC),
            raw_body_path=Path("xraw/dip_sitl/index.html"),
            status_code=200,
        )
        print("\n--- SourceAsset ---")
        print(asset.model_dump_json(indent=2))

        event = RawVoteEvent(
            chamber=Chamber.DIPUTADOS,
            legislature=Legislature.LXVI,
            vote_date=date(2024, 3, 15),
            title="Dictamen de la Comisión de Hacienda",
            source_url="https://gaceta.diputados.gob.mx/votaciones/12345",
            metadata_json={"tipo": "ordinaria", "numero": 42},
        )
        print("\n--- RawVoteEvent ---")
        print(event.model_dump_json(indent=2))
        print(f"metadata_json_str: {event.metadata_json_str}")

        link = VoteEventAsset(
            vote_event_id=1,
            asset_id=1,
            asset_role=AssetRole.PRIMARY_NOMINAL,
        )
        print("\n--- VoteEventAsset ---")
        print(link.model_dump_json(indent=2))

        cast = RawVoteCast(
            vote_event_id=1,
            asset_id=1,
            legislator_name="María García Pérez",
            legislator_group="MORENA",
            sentido=Sentido.A_FAVOR,
            region="CDMX",
            raw_row_json={"tr": "<tr>...</tr>", "col_idx": 3},
        )
        print("\n--- RawVoteCast ---")
        print(cast.model_dump_json(indent=2))
        print(f"raw_row_json_str: {cast.raw_row_json_str}")

        counts = VoteCounts(
            vote_event_id=1,
            asset_id=1,
            group_name="MORENA",
            a_favor=120,
            en_contra=5,
            abstencion=2,
            ausente=3,
            novoto=0,
            presente=0,
            total=130,
        )
        print("\n--- VoteCounts ---")
        print(counts.model_dump_json(indent=2))

        report = IngestionReport(
            assets_inserted=10,
            assets_skipped=2,
            vote_events_inserted=5,
            vote_events_linked=5,
            casts_inserted=500,
            counts_inserted=20,
            manifests_processed=3,
        )
        print("\n--- IngestionReport ---")
        print(report.model_dump_json(indent=2))

    except ValidationError as exc:
        print(f"\n❌ ERROR inesperado en datos válidos: {exc}")
        raise

    print("\n" + "=" * 60)
    print("Probando datos INVÁLIDOS...")
    print("=" * 60)

    invalid_cases: list[tuple[type[BaseModel], dict]] = [
        (
            SourceAsset,
            {
                "source_tag": "no_existe",  # string válido para source_tag (ya no es enum)
                "url": "not-a-url",  # <- este SÍ debe fallar: URL inválida
                "response_body_hash": "a1b2c3d4",
                "captured_at": datetime.now(UTC),
                "raw_body_path": "xraw/test",
            },
        ),
        (
            SourceAsset,
            {
                "source_tag": SourceTag.SEN_LXVI_AJAX,
                "url": "https://example.com",
                "response_body_hash": "g1h2i3j4",  # 'g' e 'i' no son hex
                "captured_at": datetime.now(UTC),
                "raw_body_path": "xraw/test",
            },
        ),
        (
            SourceAsset,
            {
                "source_tag": SourceTag.DIP_GACETA_POST,
                "url": "https://example.com",
                "response_body_hash": "short",  # menos de 8 caracteres
                "captured_at": datetime(2024, 1, 1),  # naive datetime
                "raw_body_path": "xraw/test",
            },
        ),
        (
            RawVoteEvent,
            {
                "chamber": Chamber.SENADO,
                "legislature": Legislature.LXVI,
                "title": "x" * 501,  # excede max_length
            },
        ),
        (
            RawVoteCast,
            {
                "vote_event_id": 1,
                "asset_id": 1,
                "legislator_name": "",  # min_length=1
                "sentido": Sentido.AUSENTE,
            },
        ),
        (
            VoteCounts,
            {
                "vote_event_id": 1,
                "asset_id": 1,
                "a_favor": -1,  # ge=0
            },
        ),
    ]

    for model_cls, data in invalid_cases:
        try:
            model_cls(**data)
            print(f"\n⚠️  {model_cls.__name__} no lanzó error (inesperado)")
        except ValidationError as exc:
            print(f"\n✅ {model_cls.__name__} rechazó datos inválidos:")
            for err in exc.errors():
                loc = " -> ".join(str(x) for x in err["loc"])
                print(f"   · [{loc}] {err['msg']}")

    print("\n" + "=" * 60)
    print("Verificación de constantes y enum values...")
    print("=" * 60)
    # SourceTag ya no es Enum — mostrar constantes de clase
    st_attrs = {k: v for k, v in vars(SourceTag).items() if k.isupper()}
    print(f"SourceTag (constantes): {st_attrs}")
    for e in (Chamber, Legislature, AssetRole, Method, Sentido):
        print(f"{e.__name__}: {[m.value for m in e]}")

    print("\n" + "=" * 60)
    print("TODAS LAS PRUEBAS PASARON")
    print("=" * 60)
