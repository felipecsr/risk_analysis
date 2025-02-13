import pandas as pd
from datetime import datetime
import os

# Configuração da data de corte
CUTOFF_DATE = datetime.strptime('2024-12-31', '%Y-%m-%d')

# Caminhos de entrada e saída
INPUT_PATH = './1-raw/'  # Atualize conforme necessário
OUTPUT_PATH = './2-trusted/'  # Atualize conforme necessário

try:
    # 1. Leitura dos Dados
    print("Lendo os arquivos de contratos e transferências...")
    contracts = pd.read_csv(f'{INPUT_PATH}contracts_data.csv', sep=',')
    transfers = pd.read_csv(f'{INPUT_PATH}transfers_data.csv', sep=',')
    print("Arquivos lidos com sucesso.")

    # 2. Tratamento Inicial de Campos
    print("Normalizando campos de data...")
    contracts['termination_date'] = contracts['termination_date'].replace('', pd.NA)
    transfers['liquidation_date'] = transfers['liquidation_date'].replace('', pd.NA)
    print("Campos normalizados.")

    # 3. União das Tabelas
    print("Realizando o merge das tabelas antes dos filtros...")
    data = pd.merge(transfers, contracts, left_on='contract_id', right_on='id', suffixes=('_trans', '_contract'))
    print(f"Registros após o merge: {len(data)}")

    # Renomear colunas durante o merge (garantir nomes únicos)
    data.rename(columns={
        'id_trans': 'transfer_id',
        'id_contract': 'contract_id_merged',
        'value': 'transfer_total_value',
        'status_trans': 'transfer_status',
        'status_contract': 'contract_status',
        'due_date': 'transfer_due_date',
        'start_date': 'contract_start_date',
        'end_date': 'contract_end_date'
    }, inplace=True)
    print("Colunas renomeadas.")

    # Passo 1: Adicionando os campos rent_fee e damage_fee
    print("Incluindo os campos rent_fee e damage_fee no DataFrame...")
    data['rent_fee'] = data['rent_fee']
    data['damage_fee'] = data['damage_fee']
    print("Campos adicionados com sucesso.")

    # 4. Definição do Status do Contrato
    print("Determinando o status de duração do contrato...")
    data['contract_end_date'] = pd.to_datetime(data['contract_end_date'])
    data['contract_duration_status'] = data.apply(
        lambda x: 'holdover' if x['contract_end_date'] <= CUTOFF_DATE else 'in_term',
        axis=1
    )
    print("Status de duração do contrato determinado.")

    # 5. Filtragem Pós-Merge
    print("Aplicando filtros pós-merge...")
    data = data[
        (data['termination_date'].isna()) &  # Contratos ativos
        (pd.to_datetime(data['transfer_due_date']) <= CUTOFF_DATE) &  # Transferências dentro da data de corte
        (~data['liquidation_date'].isna())  # Transferências liquidadas
    ]
    print(f"Registros após os filtros: {len(data)}")

    # 6. Transformações Iniciais
    print("Realizando transformações iniciais...")
    data['contract_status'] = 'active'
    data['contract_original_duration'] = (
        (data['contract_end_date'] - pd.to_datetime(data['contract_start_date'])).dt.days // 30
    )
    data['contract_current_duration'] = (
        (CUTOFF_DATE - pd.to_datetime(data['contract_start_date'])).dt.days // 30
    )
    data['contract_readjustment_index'] = data['readjustment_index']
    print("Transformações iniciais concluídas.")

    # 7. Cálculo do Ciclo de Parcelas
    print("Calculando os ciclos de parcelas...")

    def calculate_parcel_cycle_v4(df):
        df['transfer_due_date'] = pd.to_datetime(df['transfer_due_date'])
        first_due_date = df['transfer_due_date'].min()
        df['transfer_parcel_cycle'] = (
            ((df['transfer_due_date'].dt.year - first_due_date.year) * 12 +
             (df['transfer_due_date'].dt.month - first_due_date.month)) // 12 + 1
        )
        return df

    data = data.groupby('contract_id').apply(calculate_parcel_cycle_v4).reset_index(drop=True)
    print("Cálculo dos ciclos concluído.")

    # 8. Ajuste de Valores
    print("Ajustando os valores reais de aluguel...")
    data['transfer_real_rental_value'] = (
        data['transfer_total_value'].fillna(0) - data['damage_value'].fillna(0) - data['early_termination_value'].fillna(0)
    )
    print("Ajuste de valores concluído.")

    # 9. Cálculo de Medianas
    print("Calculando medianas por contrato e ciclo...")
    data['median_rental_value'] = data.groupby(['contract_id', 'transfer_parcel_cycle'])['transfer_real_rental_value']\
                                      .transform('median')
    print("Cálculo de medianas concluído.")

    # 10. Adicionar Campos Fixos
    print("Adicionando campos fixos...")
    data['source'] = 'alpop_database'
    print("Campos fixos adicionados.")

    # 11. Ordenar os dados
    print("Ordenando os dados por contrato, ciclo e data de vencimento...")
    data.sort_values(by=['contract_id', 'transfer_parcel_cycle', 'transfer_due_date'], ascending=True, inplace=True)

    # 12. Garantir que o diretório de saída exista
    print(f"Garantindo que o diretório de saída ({OUTPUT_PATH}) exista...")
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # 13. Preparar para exportação
    print("Preparando dados para exportação...")
    final_columns = [
        'contract_id', 'contract_start_date', 'contract_end_date', 'contract_status',
        'contract_original_duration', 'contract_current_duration', 'contract_duration_status',
        'contract_readjustment_index', 'transfer_parcel_cycle', 'transfer_id', 'transfer_due_date',
        'transfer_real_rental_value', 'median_rental_value', 'source', 'transfer_total_value',
        'rental_value', 'damage_value', 'early_termination_value', 'rent_fee', 'damage_fee'
    ]

    data = data[final_columns]

    output_file = f'{OUTPUT_PATH}transfers_projection_ETL_result.csv'
    data.to_csv(output_file, sep='|', index=False)
    print(f"Exportação concluída com sucesso! Arquivo salvo em: {output_file}")

except Exception as e:
    print(f"Erro durante a execução: {e}")
