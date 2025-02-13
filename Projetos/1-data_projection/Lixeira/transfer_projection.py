import pandas as pd
from dateutil.relativedelta import relativedelta
import os
from tqdm import tqdm

# Função para calcular o próximo mês em formato yyyy-mm-dd
def get_next_month(date_str):
    date = pd.to_datetime(date_str)
    next_month = date + relativedelta(months=1)
    return next_month.replace(day=1).strftime('%Y-%m-%d')

# Função para calcular o ciclo de parcelas
def calculate_parcel_cycle_v4(df):
    df['transfer_due_date'] = pd.to_datetime(df['transfer_due_date'])
    first_due_date = df['transfer_due_date'].min()
    df['transfer_parcel_cycle'] = (
        (df['transfer_due_date'].dt.year - first_due_date.year) * 12 +
        (df['transfer_due_date'].dt.month - first_due_date.month) + 1
    )
    return df

# Reajuste de valor de aluguel
def apply_adjustment(last_median, index_value):
    return last_median * (1 + index_value)

# Caminhos dos arquivos
index_table_path = "./trusted/index_table.csv"
data_path = "./trusted/transfers_projection_etl_result.csv"
output_path = "./result/transfer_projection_result.csv"

# Cutoff date (data de referência para a classificação dos contratos)
cutoff_date = pd.to_datetime("2024-12-31")

# Leitura dos dados
index_table = pd.read_csv(index_table_path, sep=',')
data = pd.read_csv(data_path, sep='|')

# Verificar se a coluna 'contract_duration_status' existe
if 'contract_duration_status' not in data.columns:
    raise KeyError("A coluna 'contract_duration_status' não está presente no arquivo de entrada. Verifique o CSV.")

# Placeholder para lançamentos futuros
future_entries = []

# Processar contratos com barra de progresso
with tqdm(total=data['contract_id'].nunique(), desc="Processando contratos", unit="contrato") as pbar:
    for contract_id, group in data.groupby('contract_id'):
        group = group.sort_values(by='transfer_due_date')
        group = calculate_parcel_cycle_v4(group)
        last_transfer = group.iloc[-1]
        contract_status = last_transfer['contract_duration_status']
        last_due_date = pd.to_datetime(last_transfer['transfer_due_date'])
        last_median = last_transfer['median_rental_value']

        if contract_status == 'holdover':
            # Criar uma única parcela
            next_due_date = get_next_month(last_due_date)
            index_row = index_table[(index_table['index_name'] == last_transfer['contract_readjustment_index']) &
                                    (index_table['month'] == pd.to_datetime(next_due_date).month) &
                                    (index_table['year'] == pd.to_datetime(next_due_date).year)]
            if not index_row.empty:
                index_value = index_row.iloc[0]['value']
                adjusted_value = apply_adjustment(last_median, index_value)
            else:
                adjusted_value = last_median  # Fallback

            future_entries.append({
                **last_transfer,
                'transfer_due_date': next_due_date,
                'transfer_real_rental_value': adjusted_value,
                'median_rental_value': adjusted_value,
                'transfer_parcel_cycle': group['transfer_parcel_cycle'].max() + 1,
                'transfer_id': f"{contract_id}-{str(len(group) + 1).zfill(3)}",
                'source': 'generated_by_script',
                'transfer_total_value': 0,
                'rental_value': 0,
                'damage_value': 0,
                'early_termination_value': 0
            })
        else:
            # Criar parcelas para contratos in_term
            current_duration = last_transfer['contract_current_duration']
            original_duration = last_transfer['contract_original_duration']

            while current_duration < original_duration:
                cycle_start = current_duration + 1
                cycle_end = min(cycle_start + 11, original_duration)

                # Gerar parcelas para o ciclo atual
                for next_cycle in range(cycle_start, cycle_end + 1):
                    next_due_date = get_next_month(last_due_date)
                    adjusted_value = last_median

                    # Adicionar nova parcela
                    future_entries.append({
                        **last_transfer,
                        'transfer_due_date': next_due_date,
                        'transfer_real_rental_value': adjusted_value,
                        'median_rental_value': last_median,
                        'transfer_parcel_cycle': group['transfer_parcel_cycle'].max() + 1,
                        'transfer_id': f"{contract_id}-{str(next_cycle).zfill(3)}",
                        'source': 'generated_by_script',
                        'transfer_total_value': 0,
                        'rental_value': 0,
                        'damage_value': 0,
                        'early_termination_value': 0
                    })
                    last_due_date = next_due_date

                # Atualizar a mediana ao final do ciclo
                cycle_group = pd.DataFrame(future_entries[-(cycle_end - cycle_start + 1):])
                last_median = cycle_group['transfer_real_rental_value'].median()

                # Aplicar reajuste na primeira parcela do próximo ciclo
                if cycle_end < original_duration:
                    next_due_date = get_next_month(last_due_date)
                    index_row = index_table[(index_table['index_name'] == last_transfer['contract_readjustment_index']) &
                                            (index_table['month'] == pd.to_datetime(next_due_date).month) &
                                            (index_table['year'] == pd.to_datetime(next_due_date).year)]
                    if not index_row.empty:
                        index_value = index_row.iloc[0]['value']
                        last_median = apply_adjustment(last_median, index_value)

                current_duration = cycle_end

        pbar.update(1)

# Converter os lançamentos futuros para DataFrame
future_df = pd.DataFrame(future_entries)

# Concatenar com os dados originais
result = pd.concat([data, future_df], ignore_index=True)

# Recalcular os ciclos de parcelas após todas as projeções
result = calculate_parcel_cycle_v4(result)

# Calcular medianas por contrato e ciclo
result['median_rental_value'] = result.groupby(['contract_id', 'transfer_parcel_cycle'])['transfer_real_rental_value']\
                                      .transform('median')

# Ordenar o resultado final
result = result.sort_values(by=['contract_id', 'transfer_parcel_cycle', 'transfer_due_date']).reset_index(drop=True)

# Salvar o resultado em CSV
os.makedirs(os.path.dirname(output_path), exist_ok=True)
result.to_csv(output_path, index=False, sep='|')

print(f"Projeção concluída. Arquivo gerado em: {output_path}")
