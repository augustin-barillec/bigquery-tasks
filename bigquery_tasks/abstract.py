import logging
from google.cloud import bigquery
from reusable.bigquery_operator import Operator
from reusable.bigquery_tasks.dict_to_query import dict_to_query
from reusable.bigquery_tasks.decorators import add_start_end_logs, \
    set_time_to_live


logger = logging.getLogger(__name__)


class LogTask:

    log_prefix = ''

    def __init__(self, developed_conf, **extra_conf):
        self.task_name = type(self).__name__
        self.developed_conf = developed_conf
        self.extra_conf = extra_conf

    def prefix_log_message(self, log_message):
        return self.log_prefix + log_message

    @property
    def start_log_message(self):
        return f'Starting {self.task_name}...'

    @property
    def end_log_message(self):
        return f'Ended {self.task_name}'

    def log(self, msg):
        msg = self.prefix_log_message(msg)
        logger.info(msg)

    def emit_start_log_message(self):
        self.log(self.start_log_message)

    def emit_end_log_message(self):
        self.log(self.end_log_message)


class Task(LogTask):

    sample_size = None
    write_monitoring = True

    query_template = None
    query_templates = None

    source_table_name = None
    source_table_names = None

    destination_table_name = None
    destination_table_names = None

    source_uri = None
    source_uris = None

    destination_uri = None
    destination_uris = None

    source_dataset_id = None

    schema = None
    schemas = None

    field_delimiter = '|'
    print_header = True
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    def __init__(self, developed_conf, **extra_conf):
        super().__init__(developed_conf, **extra_conf)
        self.table_names = self.extra_conf['table_names']

        client = bigquery.Client(
            project=self.developed_conf.project_id,
            credentials=self.developed_conf.credentials)

        self.operator = Operator(client, self.developed_conf.dataset_id)

        self.format_helper = {
            **vars(self.developed_conf),
            **self.table_names.global_register}

    def monitoring_to_bq(self, monitoring):
        named_monitoring = dict_to_query({self.task_name: monitoring})
        table_name = self.table_names.build_tmp_monitoring(self.task_name)
        self.operator.run_query(
            query=named_monitoring,
            destination_table_name=table_name)
        self.operator.set_time_to_live(
            table_name, self.developed_conf.short_time_to_live)

    def fmt(self, template):
        return template.format(**self.format_helper)

    @property
    def query(self):
        return self.fmt(self.query_template)

    @property
    def queries(self):
        return [self.fmt(qt) for qt in self.query_templates]

    @property
    def time_to_live(self):
        return self.developed_conf.short_time_to_live

    def set_time_to_live(self):
        c1 = self.destination_table_name is not None
        c2 = self.destination_table_names is not None
        assert c1 or c2
        assert not (c1 and c2)
        if c1:
            to_set_time_to_live = [self.destination_table_name]
        else:
            to_set_time_to_live = self.destination_table_names
        for n in to_set_time_to_live:
            self.operator.set_time_to_live(n, self.time_to_live)

    def delete_table_if_exists(self, table_name):
        if self.operator.table_exists(table_name):
            self.operator.delete_table(table_name)

    def delete_tables(self, table_names):
        for table_name in table_names:
            self.operator.delete_table(table_name)

    def delete_table_if_mismatches(self, reference, table_name):
        if not self.operator.table_exists(reference):
            return
        elif not self.operator.table_exists(table_name):
            return
        else:
            reference_format = self.operator.get_format_attributes(reference)
            table_format = self.operator.get_format_attributes(table_name)
            if reference_format != table_format:
                self.operator.delete_table(table_format)


class CreateDataset(Task):

    location = None

    @add_start_end_logs
    def run(self):
        assert self.location is not None
        if self.operator.dataset_exists():
            assert self.location == self.operator.get_dataset().location
        else:
            self.operator.create_dataset(location=self.location)


class DeleteTableIfMismatches(Task):

    @add_start_end_logs
    def run(self):
        self.delete_table_if_mismatches(
            self.source_table_name,
            self.destination_table_name)


class DeleteTablesIfMismatch(Task):

    @add_start_end_logs
    def run(self):
        for s, d in zip(self.source_table_names, self.destination_table_names):
            self.delete_table_if_mismatches(s, d)


class CreateEmptyTable(Task):

    schema = None
    time_partitioning = None
    range_partitioning = None
    require_partition_filter = None
    clustering_fields = None

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        self.delete_table_if_exists(self.destination_table_name)
        self.operator.create_empty_table(
            table_name=self.destination_table_name,
            schema=self.schema,
            time_partitioning=self.time_partitioning,
            range_partitioning=self.range_partitioning,
            require_partition_filter=self.require_partition_filter,
            clustering_fields=self.clustering_fields)


class CreateView(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        self.delete_table_if_exists(self.destination_table_name)
        self.operator.create_view(
            query=self.query,
            destination_table_name=self.destination_table_name)


class CreateViews(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        for q, d in zip(self.queries, self.destination_table_names):
            self.delete_table_if_exists(d)
            self.operator.create_view(
                query=q,
                destination_table_name=d)


class RunQuery(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        query = self.query
        if self.sample_size is not None:
            query = self.operator.sample_query(query, self.sample_size)
        monitoring = self.operator.run_query(
            query=query,
            destination_table_name=self.destination_table_name,
            write_disposition=self.write_disposition)
        if self.write_monitoring:
            monitoring_to_write = {
                'query_duration': monitoring['duration'],
                'query_cost': monitoring['cost']}
            self.monitoring_to_bq(monitoring_to_write)
        return monitoring


class RunQueries(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        queries = self.queries
        if self.sample_size is not None:
            queries = [self.operator.sample_query(q, self.sample_size)
                       for q in queries]
        monitoring = self.operator.run_queries(
            queries=queries,
            destination_table_names=self.destination_table_names,
            write_disposition=self.write_disposition)
        if self.write_monitoring:
            monitoring_to_write = {
                'query_duration': monitoring['duration'],
                'query_cost': monitoring['cost']}
            self.monitoring_to_bq(monitoring_to_write)
        return monitoring


class ExtractTable(Task):

    @add_start_end_logs
    def run(self):
        self.operator.extract_table(
            self.source_table_name,
            self.destination_uri,
            self.field_delimiter,
            self.print_header)


class ExtractTables(Task):

    @add_start_end_logs
    def run(self):
        self.operator.extract_tables(
            self.source_table_names,
            self.destination_uris,
            self.field_delimiter,
            self.print_header)


class LoadTable(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        self.operator.load_table(
            self.source_uri,
            self.destination_table_name,
            self.schema,
            self.field_delimiter,
            self.write_disposition)


class LoadTables(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        self.operator.load_tables(
            self.source_uris,
            self.destination_table_names,
            self.schemas,
            self.field_delimiter,
            self.write_disposition)


class CopyTable(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        self.operator.copy_table(
            self.source_table_name,
            self.destination_table_name,
            self.source_dataset_id,
            self.write_disposition)


class CopyTables(Task):

    @add_start_end_logs
    @set_time_to_live
    def run(self):
        self.operator.copy_tables(
            self.source_table_names,
            self.destination_table_names,
            self.source_dataset_id,
            self.write_disposition)
