select
    sha2(
        concat_ws(
            '||',
            coalesce(run_id, ''),
            cast(ingestion_timestamp as string),
            coalesce(source_stop_id, ''),
            coalesce(status, ''),
            coalesce(cast(rows_extracted as string), ''),
            coalesce(error_message, '')
        ),
        256
    ) as audit_key,
    cast(run_id as string) as run_id,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
    coalesce(cast(ingestion_date as date), cast(to_date(ingestion_timestamp) as date)) as ingestion_date,
    cast(source_stop_id as string) as source_stop_id,
    cast(request_url as string) as request_url,
    cast(status as string) as status,
    cast(http_status_code as int) as http_status_code,
    cast(rows_extracted as bigint) as rows_extracted,
    cast(duration_seconds as double) as duration_seconds,
    cast(error_message as string) as error_message
from {{ source('cumtd_raw', 'departure_ingestion_audit') }}
