import duckdb
import pytest

from otai import catalog, schema_builder
from otai.croissant import DatasetInfo


def _datasets_for(dataset_rows):
    return [
        DatasetInfo(
            name=name, description=f"{name} dataset", file_glob=f"{name}/*.parquet"
        )
        for name in dataset_rows
    ]


def test_build_release_schema_creates_schema_and_queryable_tables(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder.build_release_schema(
            conn, release, _datasets_for(dataset_rows), base_uri=base_uri
        )

        assert release in catalog.list_cached_schemas(conn)

        target_rows = conn.execute(
            f'SELECT * FROM {catalog.LAKE_ALIAS}."{release}".target ORDER BY id'
        ).fetchall()
        assert len(target_rows) == len(dataset_rows["target"])
        assert target_rows[0][0] == "ENSG00000141510"  # TP53 sorts before BRAF's id
    finally:
        conn.close()


def test_build_release_schema_creates_one_table_per_dataset(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder.build_release_schema(
            conn, release, _datasets_for(dataset_rows), base_uri=base_uri
        )

        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = ? AND table_schema = ?",
                [catalog.LAKE_ALIAS, release],
            ).fetchall()
        }
        assert tables == set(dataset_rows)
    finally:
        conn.close()


def test_failed_table_creation_rolls_back_the_whole_schema(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    datasets = _datasets_for(dataset_rows)
    datasets.append(
        DatasetInfo(
            name="broken",
            description="dataset whose glob matches nothing",
            file_glob="no_such_dataset/*.parquet",
        )
    )
    conn = catalog.connect_catalog(tmp_path)
    try:
        with pytest.raises(duckdb.Error):
            schema_builder.build_release_schema(
                conn, release, datasets, base_uri=base_uri
            )

        assert release not in catalog.list_cached_schemas(conn), (
            "a mid-build failure must roll back CREATE SCHEMA too, otherwise "
            "list_cached_schemas would mistake the partial schema for already-built"
        )
    finally:
        conn.close()


def test_build_release_schema_reads_real_parquet_content(
    tmp_path, fixture_release_layout
):
    base_uri, release, dataset_rows = fixture_release_layout
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder.build_release_schema(
            conn, release, _datasets_for(dataset_rows), base_uri=base_uri
        )

        association_rows = conn.execute(
            "SELECT targetId, diseaseId, CAST(score AS DOUBLE) FROM "
            f'{catalog.LAKE_ALIAS}."{release}".association_by_datasource_direct '
            "ORDER BY score DESC"
        ).fetchall()
        assert association_rows == [
            ("ENSG00000157764", "EFO_0000305", 0.8),
            ("ENSG00000141510", "EFO_0000616", 0.5),
        ]
    finally:
        conn.close()


def test_ensure_s3_access_creates_anonymous_secret_for_real_s3_base_uri(tmp_path):
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder._ensure_s3_access(
            conn, "s3://open-targets-public-data-releases/platform"
        )
        secrets = conn.execute(
            "SELECT name, provider, scope FROM duckdb_secrets() WHERE name = ?",
            [schema_builder.S3_ANONYMOUS_SECRET_NAME],
        ).fetchall()
        assert secrets == [
            (
                schema_builder.S3_ANONYMOUS_SECRET_NAME,
                "config",
                ["s3://open-targets-public-data-releases"],
            )
        ]
    finally:
        conn.close()


def test_ensure_s3_access_is_noop_for_local_file_uri(tmp_path):
    conn = catalog.connect_catalog(tmp_path)
    try:
        schema_builder._ensure_s3_access(conn, f"file://{tmp_path}")
        secrets = conn.execute("SELECT name FROM duckdb_secrets()").fetchall()
        assert secrets == []
    finally:
        conn.close()
