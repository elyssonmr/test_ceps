import pandas as pd
import glob
import os


print('Concatenando Ceps')
csv_path = 'datasource_ceps'
all_files = glob.glob(os.path.join(csv_path, "*.csv"))
column_names = ['CEP', 'logradouro', 'logradouro2', 'bairro', 'id_cidade', 'id_estado']
ceps_data = []

for filename in all_files:
    ceps = pd.read_csv(filename, header=None, names=column_names)
    cleaned_dataframe = ceps[['CEP']]
    ceps_data.append(cleaned_dataframe)

concatenated_ceps = pd.concat(ceps_data, ignore_index=True)
print(f'Total Ceps Processados: {len(concatenated_ceps)}')
concatenated_ceps.to_csv('ceps.csv', index=False)

print('CSV Mergeado')
