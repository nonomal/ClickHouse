import time
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    generate_data,
    get_uuid_str,
    write_iceberg_from_df,
    default_upload_directory,
)

_ASYNC_CACHE_REFRESH_CONFIG_PATH = "/etc/clickhouse-server/config.d/iceberg_async_cache_refresh.xml"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_selecting_with_stale_vs_latest_metadata(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_iceberg_getting_stale_data"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # disabling async refresher to validate that the latest metadata will be pulled at SELECT
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, iceberg_metadata_async_refresh_period_ms=3_600_000)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # we accept stale metadata at SELECT, and running it with using cached metadata only - no call to remote catalog
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 100

    # by default, SELECT will query remote catalog for the latest metadata
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200


@pytest.mark.parametrize("storage_type", ["s3"])
def test_default_async_metadata_refresh(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_iceberg_getting_stale_data"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # The expection is that async metadata refresher starts for each table if the cache is enabled; default interval is DEFAULT_ICEBERG_METADATA_ASYNC_REFRESH_PERIOD=10sec
    # It could be explicitly set at table creation as iceberg_metadata_async_refresh_period_ms, but we're checking the default in this scenario
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # the fresh metadata won't get pulled at SELECT, so we see stale data
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 100
    # sleeping twice the update interval to let the refresh finish even if the server is overloaded
    time.sleep(10 * 2)
    # we don't pull fresh metadata at SELECT, but the data is up to date because of the async refresh
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 200


@pytest.mark.parametrize("storage_type", ["s3"])
def test_async_metadata_refresh(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_iceberg_async_refresh"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    ASYNC_METADATA_REFRESH_PERIOD_MS=5000
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, iceberg_metadata_async_refresh_period_ms=ASYNC_METADATA_REFRESH_PERIOD_MS)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )
    # the fresh metadata won't get pulled at SELECT, so we see stale data
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 100
    # Wait for the background async refresher to pick up the new metadata (2 periods of ASYNC_METADATA_REFRESH_PERIOD_MS)
    time.sleep(ASYNC_METADATA_REFRESH_PERIOD_MS/1000 * 2)
    # we don't pull fresh metadata at SELECT, but the data is up to date because of the async refresh
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 200


@pytest.mark.parametrize("storage_type", ["s3"])
def test_insert_updates_metadata_cache(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = (
        "test_iceberg_write_updates_metadata"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    schema = "(a Int64)"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, iceberg_metadata_async_refresh_period_ms=3_600_000)

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 100

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100, 100)",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 200
