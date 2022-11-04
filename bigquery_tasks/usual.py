from reusable.bigquery_tasks import abstract
from reusable.bigquery_tasks import mixins
from reusable.bigquery_tasks.decorators import add_start_end_logs
from reusable.bigquery_tasks.dict_to_query import dict_to_query


class WriteConf(mixins.WriteMonitoringFalse, abstract.RunQuery):

    @property
    def time_to_live(self):
        return self.developed_conf.long_time_to_live

    @property
    def destination_table_name(self):
        return self.table_names.report.conf

    @property
    def query(self):
        return dict_to_query(self.developed_conf.base_conf_to_write_in_bq)


class GatherMonitorings(mixins.WriteMonitoringFalse,
                        abstract.RunQuery):

    @property
    def time_to_live(self):
        return self.developed_conf.long_time_to_live

    @property
    def destination_table_name(self):
        return self.table_names.report.monitoring_details

    @property
    def query(self):
        table_names = self.operator.list_tables()
        tmp_monitoring_table_names = [
            n for n in table_names if
            self.table_names.is_tmp_monitoring(n)]

        monitorings = [
            f'(select * from {self.operator.build_table_id(table_name)})'
            for table_name in sorted(tmp_monitoring_table_names)]
        monitorings = ' cross join '.join(monitorings)
        query = f'select * from {monitorings}'
        return query


class MonitorQuery(mixins.WriteMonitoringFalse, abstract.RunQuery):

    query_template = """
    with
    monitoring_details as (select * from
    {dataset_id}.{report_monitoring_details}),

    monitoring_query as (
    select
    {query_costs} as query_cost,
    {query_durations} as query_duration
    from monitoring_details)

    select * from monitoring_query
    """

    @property
    def time_to_live(self):
        return self.developed_conf.long_time_to_live

    @property
    def destination_table_name(self):
        return self.table_names.report.monitoring_query

    @staticmethod
    def _get_query_monitoring_cols(kind, monitoring_details_schema):
        assert kind in ('query_cost', 'query_duration')
        res = []
        for f in monitoring_details_schema:
            for sf in f.fields:
                if sf.name == kind:
                    res.append(f'{f.name}.{sf.name}')
        return res

    @staticmethod
    def _sum_cols(cols):
        return ' +\n'.join(cols)

    @property
    def query(self):
        monitoring_details_schema = self.operator.get_table(
            self.table_names.report.monitoring_details).schema
        query_costs = self._get_query_monitoring_cols(
            'query_cost', monitoring_details_schema)
        query_durations = self._get_query_monitoring_cols(
            'query_duration', monitoring_details_schema)

        query_costs = self._sum_cols(query_costs)
        query_durations = self._sum_cols(query_durations)

        res = self.query_template.format(
            query_costs=query_costs,
            query_durations=query_durations,
            **self.format_helper)

        return res


class Clean(abstract.Task):

    @add_start_end_logs
    def run(self):
        table_names = self.operator.list_tables()
        to_delete = [n for n in table_names
                     if self.table_names.is_to_delete(n)]
        self.delete_tables(to_delete)
