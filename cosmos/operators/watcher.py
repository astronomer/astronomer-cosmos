status_model_fhir_dbt_analytics_active_encounters_daily_nodefinished = {
    "info": {
        "name": "NodeFinished",
        "code": "Q025",
        "msg": "Finished running node model.fhir_dbt_utils.fhir_table_list",
        "level": "debug",
        "invocation_id": "3ba596c6-a9ef-4e9c-9682-85feab4b2516",
        "pid": 34,
        "thread": "Thread-4 (worker)",
        "ts": {"seconds": 1757613874, "nanos": 97893000},
    },
    "data": {
        "node_info": {
            "node_path": "fhir_resources/fhir_table_list.sql",
            "node_name": "fhir_table_list",
            "unique_id": "model.fhir_dbt_utils.fhir_table_list",
            "resource_type": "model",
            "materialized": "table",
            "node_status": "success",
            "node_started_at": "2025-09-11T18:04:30.755110",
            "node_finished_at": "2025-09-11T18:04:34.094207",
            "meta": {"fields": {"description": "List of FHIR resource tables present in the database"}},
            "node_relation": {
                "database": "astronomer-dag-authoring",
                "schema": "fhir_airflow3",
                "alias": "fhir_table_list",
                "relation_name": "`astronomer-dag-authoring`.`fhir_airflow3`.`fhir_table_list`",
            },
        },
        "run_result": {
            "status": "success",
            "message": "CREATE TABLE (17.0 rows, 10.0 MiB processed)",
            "timing_info": [
                {
                    "name": "compile",
                    "started_at": {"seconds": 1757613870, "nanos": 796102000},
                    "completed_at": {"seconds": 1757613870, "nanos": 848620000},
                },
                {
                    "name": "execute",
                    "started_at": {"seconds": 1757613870, "nanos": 848916000},
                    "completed_at": {"seconds": 1757613874, "nanos": 91289000},
                },
            ],
            "thread": "Thread-4 (worker)",
            "execution_time": 3.32948399,
            "adapter_response": {
                "slot_ms": 6785,
                "rows_affected": 17,
                "project_id": "astronomer-dag-authoring",
                "location": "US",
                "job_id": "a63ef0bf-d3dc-4146-af3a-a03802e7e493",
                "code": "CREATE TABLE",
                "bytes_processed": 10485760,
                "bytes_billed": 10485760,
                "_message": "CREATE TABLE (17.0 rows, 10.0 MiB processed)",
            },
        },
    },
}

dbt_watcher_mainreportversion = {
    "event": {
        "info": {
            "name": "MainReportVersion",
            "code": "A001",
            "msg": "Running with dbt=1.9.0",
            "level": "info",
            "invocation_id": "3ba596c6-a9ef-4e9c-9682-85feab4b2516",
            "pid": 34,
            "thread": "MainThread",
            "ts": {"seconds": 1757613865, "nanos": 813262000},
        },
        "data": {"version": "=1.9.0", "log_version": 3},
    }
}

dbt_watcher_adapterregistered = {
    "event": {
        "info": {
            "name": "AdapterRegistered",
            "code": "E034",
            "msg": "Registered adapter: bigquery=1.9.0",
            "level": "info",
            "invocation_id": "3ba596c6-a9ef-4e9c-9682-85feab4b2516",
            "pid": 34,
            "thread": "MainThread",
            "ts": {"seconds": 1757613866, "nanos": 467522000},
        },
        "data": {"adapter_name": "bigquery", "adapter_version": "=1.9.0"},
    }
}
