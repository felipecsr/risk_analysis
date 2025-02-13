import pandas as pd
from dateutil.relativedelta import relativedelta
import os
from tqdm import tqdm

# Função para calcular o próximo mês em formato yyyy-mm-dd
def get_next_month(date_str):
    date = pd.to_datetime(date_str)
    next_month = date + relativedelta(months=1)
    return next_month.replace(day=1).strftime('%Y-%m-%d')

# Função para calcular o ciclo de parcelas com base em 12 meses
def calculate_parcel_cycle_v5(start_date, due_date):
    start_date = pd.to_datetime(start_date)
    due_date = pd.to_datetime(due_date)
    delta_years = due_date.year - start_date.year
    delta_months = due_date.month - start_date.month
    total_months = delta_years * 12 + delta_months
    return (total_months // 12) + 1

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
        contract_start_date = group['contract_start_date'].iloc[0]
        last_transfer = group.iloc[-1]
        contract_status = last_transfer['contract_duration_status']
        last_due_date = pd.to_datetime(last_transfer['transfer_due_date'])
        last_median = last_transfer['median_rental_value']
        previous_cycle = calculate_parcel_cycle_v5(contract_start_date, last_due_date)

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
                'transfer_real_rental_value': round(adjusted_value, 2),
                'median_rental_value': round(adjusted_value, 2),
                'transfer_parcel_cycle': calculate_parcel_cycle_v5(contract_start_date, next_due_date),
                'transfer_id': f"{contract_id}-{str(len(group) + 1).zfill(3)}",
                'source': 'generated_by_script',
                'transfer_total_value': 0.0,
                'rental_value': 0.0,
                'damage_value': 0.0,
                'early_termination_value': 0.0
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
                    current_cycle = calculate_parcel_cycle_v5(contract_start_date, next_due_date)

                    # Aplicar reajuste somente na troca de ciclo
                    if current_cycle > previous_cycle:
                        index_row = index_table[(index_table['index_name'] == last_transfer['contract_readjustment_index']) &
                                                (index_table['month'] == pd.to_datetime(next_due_date).month) &
                                                (index_table['year'] == pd.to_datetime(next_due_date).year)]
                        if not index_row.empty:
                            index_value = index_row.iloc[0]['value']
                            adjusted_value = apply_adjustment(last_median, index_value)
                            last_median = adjusted_value
                        else:
                            adjusted_value = last_median
                    else:
                        adjusted_value = last_median

                    # Adicionar nova parcela
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
                    previous_cycle = current_cycle

                current_duration = cycle_end

        pbar.update(1)

# Converter os lançamentos futuros para DataFrame
future_df = pd.DataFrame(future_entries)

# Concatenar com os dados originais
result = pd.concat([data, future_df], ignore_index=True)

# Arredondar campos numéricos
numeric_fields = ['transfer_real_rental_value', 'median_rental_value', 'transfer_total_value', 'rental_value', 'damage_value', 'early_termination_value']
result[numeric_fields] = result[numeric_fields].applymap(lambda x: round(x, 2))

# Ordenar o resultado final
result = result.sort_values(by=['contract_id', 'transfer_parcel_cycle', 'transfer_due_date']).reset_index(drop=True)

# Salvar o resultado em CSV
os.makedirs(os.path.dirname(output_path), exist_ok=True)
result.to_csv(output_path, index=False, sep='|')

print(f"Projeção concluída. Arquivo gerado em: {output_path}")
