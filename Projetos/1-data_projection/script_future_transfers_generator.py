import pandas as pd
from dateutil.relativedelta import relativedelta
import os
from tqdm import tqdm

# Função para calcular o próximo mês em formato yyyy-mm-dd
def get_next_month(date_str):
    date = pd.to_datetime(date_str)
    next_month = date + relativedelta(months=1)
    return next_month.replace(day=1).strftime('%Y-%m-%d')

# Função para calcular o ciclo de parcelas com base no primeiro transfer_due_date
def calculate_parcel_cycle_v5(first_due_date, due_date):
    first_due_date = pd.to_datetime(first_due_date)
    due_date = pd.to_datetime(due_date)
    delta_years = due_date.year - first_due_date.year
    delta_months = due_date.month - first_due_date.month
    total_months = delta_years * 12 + delta_months
    return (total_months // 12) + 1

# Reajuste de valor de aluguel
def apply_adjustment(last_median, index_value):
    return last_median * (1 + index_value)

# Caminhos dos arquivos
index_table_path = "./2-trusted/index_table.csv"
data_path = "./2-trusted/transfers_projection_ETL_result.csv"
output_path = "./3-result/transfer_projection_result.csv"

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
        first_due_date = pd.to_datetime(group['transfer_due_date'].min())
        last_transfer = group.iloc[-1]
        contract_status = last_transfer['contract_duration_status']
        last_due_date = pd.to_datetime(last_transfer['transfer_due_date'])
        last_median = last_transfer['median_rental_value']
        previous_cycle = calculate_parcel_cycle_v5(first_due_date, last_due_date)

        # Garantir que o ciclo do histórico é tratado corretamente
        expected_last_date = first_due_date + relativedelta(months=(previous_cycle * 12) - 1)
        if last_due_date == expected_last_date:
            previous_cycle += 1

        if contract_status == 'holdover':
            next_due_date = get_next_month(last_due_date)
            index_row = index_table[(index_table['index_name'] == last_transfer['contract_readjustment_index']) &
                                    (index_table['month'] == pd.to_datetime(next_due_date).month) &
                                    (index_table['year'] == pd.to_datetime(next_due_date).year)]
            if not index_row.empty:
                index_value = index_row.iloc[0]['value']
                adjusted_value = apply_adjustment(last_median, index_value)
            else:
                adjusted_value = last_median

            future_entries.append({
                **last_transfer,
                'transfer_due_date': next_due_date,
                'transfer_real_rental_value': round(adjusted_value, 2),
                'median_rental_value': round(adjusted_value, 2),
                'transfer_parcel_cycle': calculate_parcel_cycle_v5(first_due_date, next_due_date),
                'transfer_id': f"{contract_id}-{str(len(group) + 1).zfill(3)}",
                'source': 'generated_by_script',
                'transfer_total_value': 0.0,
                'rental_value': 0.0,
                'damage_value': 0.0,
                'early_termination_value': 0.0
            })
        else:
            current_duration = last_transfer['contract_current_duration']
            original_duration = last_transfer['contract_original_duration']

            while current_duration < original_duration:
                cycle_start = current_duration + 1
                cycle_end = min(cycle_start + 11, original_duration)

                for next_cycle in range(cycle_start, cycle_end + 1):
                    next_due_date = get_next_month(last_due_date)
                    current_cycle = calculate_parcel_cycle_v5(first_due_date, next_due_date)

                    if current_cycle > previous_cycle:
                        index_row = index_table[(index_table['index_name'] == last_transfer['contract_readjustment_index']) &
                                                (index_table['month'] == pd.to_datetime(next_due_date).month) &
                                                (index_table['year'] == pd.to_datetime(next_due_date).year)]
                        if not index_row.empty:
                            index_value = index_row.iloc[0]['value']
                            adjusted_value = apply_adjustment(last_median, index_value)
                            last_median = adjusted_value
                        previous_cycle = current_cycle
                    else:
                        adjusted_value = last_median

                    future_entries.append({
                        **last_transfer,
                        'transfer_due_date': next_due_date,
                        'transfer_real_rental_value': round(adjusted_value, 2),
                        'median_rental_value': round(last_median, 2),
                        'transfer_parcel_cycle': current_cycle,
                        'transfer_id': f"{contract_id}-{str(next_cycle).zfill(3)}",
                        'source': 'generated_by_script',
                        'transfer_total_value': 0.0,
                        'rental_value': 0.0,
                        'damage_value': 0.0,
                        'early_termination_value': 0.0
                    })

                    last_due_date = next_due_date

                current_duration = cycle_end

        pbar.update(1)

future_df = pd.DataFrame(future_entries)

# Adicionar rent_fee_value calculado
future_df['rent_fee'] = future_df['rent_fee'].astype(float)  # Garantir que rent_fee é float
future_df['rent_fee_value'] = future_df['rent_fee'] / 100 * future_df['transfer_real_rental_value']

result = pd.concat([data, future_df], ignore_index=True)

numeric_fields = ['transfer_real_rental_value', 'median_rental_value', 'transfer_total_value', 'rental_value', 'damage_value', 'early_termination_value', 'rent_fee_value']
result[numeric_fields] = result[numeric_fields].applymap(lambda x: round(x, 2))

result = result.sort_values(by=['contract_id', 'transfer_parcel_cycle', 'transfer_due_date']).reset_index(drop=True)

os.makedirs(os.path.dirname(output_path), exist_ok=True)
result.to_csv(output_path, index=False, sep='|')

print(f"Projeção concluída. Arquivo gerado em: {output_path}")
