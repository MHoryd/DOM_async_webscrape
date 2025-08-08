import datetime
import os
import json
from typing import Dict
from logging import Logger
import polars as pl
from prefect import task, flow, get_run_logger
import oci

OCI_USER = os.environ.get('OCI_USER_OCID')
OCI_FINGERPRINT = os.environ.get('OCI_FINGERPRINT')
OCI_TENANCY = os.environ.get('OCI_TENANCY_OCID')
OCI_REGION = os.environ.get('OCI_REGION')
OCI_BUCKET = os.environ.get('OCI_BUCKET_NAME')
OCI_NAMESPACE = os.environ.get('OCI_NAMESPACE')


oci_config = {
    "user": OCI_USER,
    "fingerprint": OCI_FINGERPRINT,
    "key_file": ".\\oci_api_key.pem",
    "tenancy": OCI_TENANCY,
    "region": OCI_REGION
}
object_storage = oci.object_storage.ObjectStorageClient(oci_config)


def safe_col(df: pl.DataFrame, col_name: str, dtype = pl.Utf8):
    if col_name in df.columns:
        return pl.col(col_name)
    else:
        return pl.lit(None).cast(dtype).alias(col_name)

@task
def extract_data_from_dictobject(raw_dict_object):
    raw_df = pl.json_normalize(raw_dict_object)

    df = raw_df.select([
        
        pl.col("id"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "price")
            .struct.field("value")
        ).list.first().cast(pl.String).alias("price"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "m")
            .struct.field("value")
        ).list.first().cast(pl.Float64).alias("property_size_m2"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "terrain_area")
            .struct.field("value")
        ).list.first().cast(pl.Float64).alias("terrain_area"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "market")
            .struct.field("value")
        ).list.first().alias("market"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "rooms_num")
            .struct.field("value")
        ).list.first().cast(pl.String).alias("rooms_num"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "building_type")
            .struct.field("value")
        ).list.first().alias("building_type"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "floors_num")
            .struct.field("value")
        ).list.first().alias("floors_num"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "build_year")
            .struct.field("value")
        ).list.first().cast(pl.String).alias("build_year"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "construction_status")
            .struct.field("value")
        ).list.first().cast(pl.String).alias("construction_status"),

        pl.col("characteristics")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("key") == "rent")
            .struct.field("value")
        ).list.first().cast(pl.Float64).alias("rent"),

        safe_col(raw_df, "location.address.city.name").alias("city_name"),
        safe_col(raw_df, "location.address.province.code").alias("province_code"),
        safe_col(raw_df, "location.coordinates.latitude", pl.Float64).alias("latitude"),
        safe_col(raw_df, "location.coordinates.longitude", pl.Float64).alias("longitude"),
        safe_col(raw_df, "location.address.street.name").alias("street_name"),

        pl.when(
            safe_col(raw_df, "agency.type") == 'agency'
        ).then(pl.lit(1)).otherwise(pl.lit(0))
        .alias("is_agency_offer"),
        
        pl.when(
            pl.col("advertType") == "DEVELOPER_UNIT"
        ).then(pl.lit(1)).otherwise(pl.lit(0))
        .alias("is_developer_offer"),

        safe_col(raw_df, "target.Security_types", pl.List(pl.Utf8)).alias('Security_types'),
        safe_col(raw_df, "target.Media_types", pl.List(pl.Utf8)).alias('Media_types'),
        pl.col("slug").alias("slug")
    ])
    if len(raw_df["featuresByCategory"].to_list()[0]) != 0:
        featuresByCategory_df = pl.from_dict({i.get('label'):None if i.get('values') == '' else [i.get('values')] for i in raw_df["featuresByCategory"].explode()})
    else:
        featuresByCategory_df = pl.DataFrame()
    new_df = pl.concat([df,featuresByCategory_df], how='horizontal')
    new_df = new_df.cast({"price":pl.Float64,"property_size_m2":pl.Float64,"terrain_area":pl.Float64})

    dict_object = new_df.rows(named=True)[0]
    return dict_object


def construct_file_name(slug: str):
    return f"{datetime.date.today().year}/{datetime.date.today().month}/{datetime.date.today().day}/{slug}.json"


@task(retries=3, retry_delay_seconds=20)
def store_data_in_bucket(dict_object: Dict, logger: Logger):
    file_name  = construct_file_name(dict_object['slug'])
    body_bytes = json.dumps(dict_object, indent=4).encode('utf-8')
    put = object_storage.put_object(
        namespace_name=OCI_NAMESPACE,
        bucket_name=OCI_BUCKET,
        object_name=file_name,
        put_object_body=body_bytes
    )
    logger.info(f"Uploaded {file_name} to {OCI_BUCKET} (etag={put.headers['etag']})")

@flow(log_prints=True)
def extract_data_from_dict_object_and_store_it_in_bucket(dict_object: Dict):
    logger = get_run_logger()
    transformed_data = extract_data_from_dictobject(dict_object)
    store_data_in_bucket(dict_object=transformed_data, logger=logger) # type: ignore