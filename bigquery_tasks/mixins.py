from google.cloud import bigquery


class WriteMonitoringFalse:

    write_monitoring = False


class WriteAppend:

    write_disposition = bigquery.WriteDisposition.WRITE_APPEND


class LongLogPrefix:

    log_prefix = '--'
