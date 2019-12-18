from collections import OrderedDict
from pathlib import Path

import param
import pandas as pd
import yaml

from ..uit import Client


class HpcConfigurable(param.Parameterized):
    configuration_file = param.String()
    uit_client = param.ClassSelector(Client)
    environment_variables = param.ClassSelector(OrderedDict, default=OrderedDict())
    modules_to_load = param.ListSelector(default=[])
    modules_to_unload = param.ListSelector(default=[])
    load_modules = param.List()
    unload_modules = param.List()

    @param.depends('uit_client', watch=True)
    def update_configurable_hpc_parameters(self):
        if not self.uit_client.connected:
            return

        self.load_config_file()
        self.param.modules_to_unload.objects = sorted(self.uit_client.get_loaded_modules())
        self.param.modules_to_load.objects = self._get_modules_available_to_load()
        self.modules_to_load = self._validate_modules(self.param.modules_to_load.objects, self.modules_to_load)
        self.unload_modules = self._validate_modules(self.param.modules_to_unload.objects, self.modules_to_unload)

    def _get_modules_available_to_load(self):
        modules = set(self.uit_client.get_available_modules(flatten=True)) - set(self.param.modules_to_unload.objects)
        return sorted(modules)

    def _validate_modules(self, possible, candidates):
        df = pd.DataFrame([v.split('/', 1) for v in possible], columns=['Name', 'Version'])
        df['Default'] = df['Version'].apply(lambda v: True if v is None else v.endswith('(default)'))
        dfg = df.groupby('Name')

        modules = list()
        for m in candidates:
            if m in possible:
                modules.append(m)
                continue
            elif m in dfg.groups:
                group = dfg.get_group(m)
                row = group.iloc[0]
                if group.shape[0] > 1:
                    row = group[group['Default']].iloc[0]
                module = f'{row.Name}/{row.Version}'
                modules.append(module)
            else:
                print(f'Module "{m}" is  invalid.')
        return sorted(modules)

    def load_config_file(self):
        config_file = Path(self.configuration_file)
        if config_file.is_file():
            with config_file.open() as f:
                config = yaml.safe_load(f).get(self.uit_client.system, {})
            modules = config.get('modules')
            if modules:
                self.modules_to_load = modules.get('load') or self.modules_to_load
                self.modules_to_unload = modules.get('unload') or self.modules_to_unload
            self.environment_variables = OrderedDict(config.get('environment_variables')) or self.environment_variables
