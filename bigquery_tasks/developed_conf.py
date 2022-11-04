class DevelopedConf:

    def __init__(self, base_conf):
        self.base_conf_to_write_in_bq = {
            k: base_conf[k] for k in base_conf if k != 'credentials'}

        self.sample_size = base_conf.get('sample_size')
        self.short_time_to_live = base_conf.get('short_time_to_live', 5)
        self.long_time_to_live = base_conf.get('long_time_to_live', 10)
        self.project_id = base_conf['project_id']
        self.dataset_name = base_conf['dataset_name']
        self.credentials = base_conf.get('credentials')

        assert self.short_time_to_live <= self.long_time_to_live

        self.dataset_id = self.build_dataset_id()

    def build_dataset_id(self, project_id=None, dataset_name=None):
        if project_id is None:
            project_id = self.project_id
        if dataset_name is None:
            dataset_name = self.dataset_name
        return f'{project_id}.{dataset_name}'
