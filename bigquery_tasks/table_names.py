from argparse import Namespace

COMPUTED = 'computed'
EXPOSED = 'exposed'
REPORT = 'report'
TMP_MONITORING = 'tmp_monitoring'


def has_no_duplicates(list_):
    return len(list_) == len(set(list_))


class TableNames:

    def __init__(self):
        self.computed_core_names = []
        self.exposed_core_names = []
        self.report_core_names = [
            'conf', 'monitoring_details', 'monitoring_query']

        self.computed_to_exclude_from_to_check = []

    @staticmethod
    def build_prefix(kind):
        return kind + '_'

    @staticmethod
    def build_suffix(long):
        return ''

    @property
    def computed_prefix(self):
        return self.build_prefix(COMPUTED)

    @property
    def computed_suffix(self):
        return self.build_suffix(long=True)

    @property
    def exposed_prefix(self):
        return self.build_prefix(EXPOSED)

    @property
    def exposed_suffix(self):
        return self.build_suffix(long=False)

    @property
    def report_prefix(self):
        return self.build_prefix(REPORT)

    @property
    def report_suffix(self):
        return self.build_suffix(long=True)

    @property
    def tmp_monitoring_prefix(self):
        return self.build_prefix(TMP_MONITORING)

    @property
    def tmp_monitoring_suffix(self):
        return self.build_suffix(long=True)

    def _get_prefix(self, kind):
        return getattr(self, f'{kind}_prefix')

    def _get_suffix(self, kind):
        return getattr(self, f'{kind}_suffix')

    def _build(self, kind, core_name):
        prefix = self._get_prefix(kind)
        suffix = self._get_suffix(kind)
        return prefix + core_name + suffix

    def build_tmp_monitoring(self, core_name):
        return self._build(TMP_MONITORING, core_name)

    def _build_specific_register(self, kind):
        res = dict()
        core_names = getattr(self, kind + '_core_names')
        assert has_no_duplicates(core_names)
        for core_name in core_names:
            table_name = self._build(kind, core_name)
            res[core_name] = table_name
        res = Namespace(**res)
        return res

    @property
    def computed(self):
        return self._build_specific_register(COMPUTED)

    @property
    def exposed(self):
        return self._build_specific_register(EXPOSED)

    @property
    def report(self):
        return self._build_specific_register(REPORT)

    def is_computed(self, table_name):
        return table_name in vars(self.computed).values()

    def is_exposed(self, table_name):
        return table_name in vars(self.exposed).values()

    def is_report(self, table_name):
        return table_name in vars(self.report).values()

    def filter(self, kind, table_name):
        prefix = self._get_prefix(kind)
        suffix = self._get_suffix(kind)
        return table_name.startswith(prefix) and table_name.endswith(suffix)

    def is_tmp_monitoring(self, table_name):
        return self.filter(TMP_MONITORING, table_name)

    def is_to_delete(self, table_name):
        return self.is_tmp_monitoring(table_name)

    @property
    def global_register(self):
        res = dict()
        for kind in [COMPUTED, EXPOSED, REPORT]:
            core_names = getattr(self, kind + '_core_names')
            for core_name in core_names:
                kind_core_name = kind + '_' + core_name
                specific_register = vars(getattr(self, kind))
                table_name = specific_register[core_name]
                assert kind_core_name not in res
                assert table_name not in res.values()
                res[kind_core_name] = table_name
        return res

    @property
    def to_check(self):
        res = []
        computed = vars(self.computed)
        exposed = vars(self.exposed)
        for core_name in computed:
            if core_name not in self.computed_to_exclude_from_to_check:
                res.append(computed[core_name])
        res += list(exposed.values())
        assert has_no_duplicates(res)
        return res
