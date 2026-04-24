import marimo

__generated_with = "0.23.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import os
    import sqlite3

    import polars as pl

    DB_PATH = "data/historico.db"
    db_exists = os.path.exists(DB_PATH)
    return DB_PATH, db_exists, mo, pl, sqlite3


@app.cell(hide_code=True)
def _(DB_PATH, db_exists, mo):
    if not db_exists:
        mo.md(
            f"""
            ## ⚠️ Base de datos no encontrada

            No se encontró la base de datos en `{DB_PATH}`.
            """
        )
    return


@app.cell
def _(DB_PATH, db_exists, pl, sqlite3):
    # Cargar datos si la base existe
    df_events_by_source = None
    df_validation = None
    df_sample = None

    if db_exists:
        conn = sqlite3.connect(DB_PATH)

        # 3b: Conteo de vote_events por cámara y fuente
        df_events_by_source = pl.read_database(
            query="""
                SELECT
                    rve.chamber,
                    sa.source_tag,
                    COUNT(DISTINCT rve.vote_event_id) AS event_count
                FROM raw_vote_event rve
                JOIN vote_event_asset vea ON rve.vote_event_id = vea.vote_event_id
                JOIN source_asset sa ON vea.asset_id = sa.asset_id
                GROUP BY rve.chamber, sa.source_tag
                ORDER BY rve.chamber, event_count DESC
            """,
            connection=conn,
        )

        # 3c: Validación counts vs nominal
        # Conteos desde raw_vote_cast
        raw_counts = pl.read_database(
            query="""
                SELECT
                    vote_event_id,
                    sentido,
                    COUNT(*) AS n
                FROM raw_vote_cast
                GROUP BY vote_event_id, sentido
            """,
            connection=conn,
        )

        # Pivotar raw_counts a columnas por sentido
        raw_pivot = raw_counts.pivot(
            values="n",
            index="vote_event_id",
            on="sentido",
        ).fill_null(0)

        # Renombrar columnas para claridad
        rename_map = {
            "a_favor": "raw_a_favor",
            "en_contra": "raw_en_contra",
            "abstencion": "raw_abstencion",
            "ausente": "raw_ausente",
            "novoto": "raw_novoto",
            "presente": "raw_presente",
        }
        raw_pivot = raw_pivot.rename(
            {col: rename_map.get(col, col) for col in raw_pivot.columns}
        )

        # Asegurar que existan todas las columnas raw_* esperadas
        for raw_col in rename_map.values():
            if raw_col not in raw_pivot.columns:
                raw_pivot = raw_pivot.with_columns(pl.lit(0).alias(raw_col))

        # Totales desde vote_counts (sumar todos los grupos por vote_event)
        counts_agg = pl.read_database(
            query="""
                SELECT
                    vote_event_id,
                    SUM(a_favor)    AS cnt_a_favor,
                    SUM(en_contra)  AS cnt_en_contra,
                    SUM(abstencion) AS cnt_abstencion,
                    SUM(ausente)    AS cnt_ausente,
                    SUM(novoto)     AS cnt_novoto,
                    SUM(presente)   AS cnt_presente
                FROM vote_counts
                GROUP BY vote_event_id
            """,
            connection=conn,
        )

        # Unir y comparar
        df_validation = raw_pivot.join(
            counts_agg, on="vote_event_id", how="full", coalesce=True
        ).fill_null(0)

        # Calcular diferencias por sentido
        sentidos = [
            ("a_favor", "raw_a_favor", "cnt_a_favor"),
            ("en_contra", "raw_en_contra", "cnt_en_contra"),
            ("abstencion", "raw_abstencion", "cnt_abstencion"),
            ("ausente", "raw_ausente", "cnt_ausente"),
            ("novoto", "raw_novoto", "cnt_novoto"),
            ("presente", "raw_presente", "cnt_presente"),
        ]

        for sentido, raw_col, cnt_col in sentidos:
            if raw_col in df_validation.columns and cnt_col in df_validation.columns:
                df_validation = df_validation.with_columns(
                    (pl.col(raw_col) - pl.col(cnt_col)).alias(f"diff_{sentido}")
                )

        # Determinar MISMATCH si alguna diferencia es distinta de cero
        diff_cols = [
            f"diff_{s[0]}"
            for s in sentidos
            if f"diff_{s[0]}" in df_validation.columns
        ]
        if diff_cols:
            df_validation = df_validation.with_columns(
                pl.when(pl.sum_horizontal([pl.col(c).abs() for c in diff_cols]) > 0)
                .then(pl.lit("MISMATCH"))
                .otherwise(pl.lit("OK"))
                .alias("status")
            )
        else:
            df_validation = df_validation.with_columns(pl.lit("N/A").alias("status"))

        # Traer metadata del evento
        events_meta = pl.read_database(
            query="""
                SELECT vote_event_id, chamber, title
                FROM raw_vote_event
            """,
            connection=conn,
        )
        df_validation = df_validation.join(events_meta, on="vote_event_id", how="left")

        # Reordenar columnas para legibilidad
        display_cols = [
            "vote_event_id",
            "chamber",
            "title",
            "status",
        ]
        for sentido, raw_col, cnt_col in sentidos:
            if raw_col in df_validation.columns:
                display_cols.append(raw_col)
            if cnt_col in df_validation.columns:
                display_cols.append(cnt_col)
            diff_col = f"diff_{sentido}"
            if diff_col in df_validation.columns:
                display_cols.append(diff_col)

        # Filtrar solo columnas que existen
        display_cols = [c for c in display_cols if c in df_validation.columns]
        df_validation = df_validation.select(display_cols)

        # 3d: Sample aleatorio de raw_vote_cast
        df_sample = pl.read_database(
            query="""
                SELECT
                    rvc.legislator_name AS nombre,
                    rvc.legislator_group AS grupo,
                    rvc.sentido,
                    sa.source_tag AS fuente
                FROM raw_vote_cast rvc
                JOIN source_asset sa ON rvc.asset_id = sa.asset_id
                ORDER BY RANDOM()
                LIMIT 10
            """,
            connection=conn,
        )

        conn.close()
    return df_events_by_source, df_sample, df_validation


@app.cell(hide_code=True)
def _(db_exists, df_events_by_source, mo):
    if db_exists and df_events_by_source is not None:
        _md = mo.md("""
        ## 1. Conteo de vote_events por cámara y fuente
        """)
        _table = mo.ui.table(df_events_by_source, selection=None)
        mo.vstack([_md, _table])
    return


@app.cell(hide_code=True)
def _(db_exists, df_validation, mo):
    if db_exists and df_validation is not None:
        _md = mo.md("""
        ## 2. Validación: raw_vote_cast vs vote_counts

        Compara la suma de sentidos en `raw_vote_cast` contra los totales
        agregados en `vote_counts`. Se marca **MISMATCH** si difieren.
        """)
        _table = mo.ui.table(df_validation, selection=None)
        mo.vstack([_md, _table])
    return


@app.cell(hide_code=True)
def _(db_exists, df_sample, mo):
    if db_exists and df_sample is not None:
        _md = mo.md("""
        ## 3. Muestra aleatoria de raw_vote_cast
        """)
        _table = mo.ui.table(df_sample, selection=None)
        mo.vstack([_md, _table])
    return


if __name__ == "__main__":
    app.run()
