from schemas.schemas import (
    BRONZE_DATASET_SCHEMAS,
    DATASET_PRIMARY_KEYS,
    DATASET_REQUIRED_FIELDS,
    gold_validation_failure_schema,
    ingestion_watermark_schema,
)


def test_dataset_maps_have_identical_coverage():
    assert set(BRONZE_DATASET_SCHEMAS) == set(DATASET_PRIMARY_KEYS) == set(DATASET_REQUIRED_FIELDS)
    assert len(BRONZE_DATASET_SCHEMAS) == 13


def test_required_fields_and_primary_keys_exist_in_schema():
    for dataset, schema in BRONZE_DATASET_SCHEMAS.items():
        fields = schema.fieldNames()
        assert len(fields) == len(set(fields))
        assert set(DATASET_REQUIRED_FIELDS[dataset]) <= set(fields)
        assert set(DATASET_PRIMARY_KEYS[dataset]) <= set(DATASET_REQUIRED_FIELDS[dataset])


def test_all_incremental_datasets_have_updated_at():
    for schema in BRONZE_DATASET_SCHEMAS.values():
        assert "updated_at" in schema.fieldNames()


def test_business_fields_are_nullable_for_quality_classification():
    assert all(field.nullable for schema in BRONZE_DATASET_SCHEMAS.values() for field in schema.fields)


def test_control_schemas_keep_non_nullable_identity():
    assert all(not field.nullable for field in ingestion_watermark_schema.fields)
    required = {"run_id", "datamart", "validation_type", "validation_error", "validation_status", "execution_ts", "execution_date"}
    actual = {field.name for field in gold_validation_failure_schema.fields if not field.nullable}
    assert actual == required

