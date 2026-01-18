import pandas as pd


COLUMNS = [
    'cep',
    'logradouro',
    'complemento',
    'unidade',
    'bairro',
    'localidade',
    'uf',
    'estado',
    'região',
    'ibge',
    'gia',
    'ddd',
    'siafi'
]


class Exporter():
    def __init__(self):
        self._success_items = []
        self._error_items = []

    def process_results(self, results):
        for result in results:
            if result['success']:
                self._success_items.append(result['result'])
            else:
                self._error_items.append(result['result'])

    def export_to_csv(self, filename):
        dataframe = pd.DataFrame(self._success_items, columns=COLUMNS)
        dataframe.to_csv(filename, index=False)

    def export_to_xml(self, filename):
        dataframe = pd.DataFrame(self._success_items, columns=COLUMNS)
        dataframe.to_xml(
            filename, index=False, root_name='ceps', row_name='cep'
        )

    @property
    def success_results(self):
        return self._success_items

    def export_errors_to_csv(self, filename):
        dataframe = pd.DataFrame(self._error_items, columns=['error_str'])
        dataframe.to_csv(filename, index=False)
