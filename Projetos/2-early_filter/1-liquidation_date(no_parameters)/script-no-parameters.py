import pandas as pd
import sqlite3
import os

# Caminho dos arquivos
transfers_data_path = './raw/transfers_data.csv'
contracts_data_path = './raw/contracts_data.csv'
output_path = './result/early_median_result.csv'

# Carregar os dados
transfers_data = pd.read_csv(transfers_data_path, sep=',')
contracts_data = pd.read_csv(contracts_data_path, sep=',')

# Criar o banco SQLite em memória
conn = sqlite3.connect(":memory:")

# Carregar os DataFrames como tabelas SQL no banco
transfers_data.to_sql('transfers_data', conn, index=False, if_exists='replace')
contracts_data.to_sql('contracts_data', conn, index=False, if_exists='replace')

# Consulta SQL para realizar o JOIN e a transformação inicial
query = """
    SELECT 
        t.id AS id_transfer,
        t.liquidation_date,
        t.due_date AS transfer_due_date,
        t.value AS transfer_total_value,
        t.status AS transfer_status,
        t.contract_id,
        t.bill_id,
        t.accrual,
        t.real_estate_agency_id,
        t.rental_value,
        t.damage_value,
        t.early_termination_value,
        c.early_termination_penalty AS early_termination_guarantee,
        CASE 
            WHEN c.damage_fee = 0 THEN 'false'
            ELSE 'true'
        END AS damage_guarantee
    FROM 
        transfers_data t
    JOIN 
        contracts_data c
    ON 
        t.contract_id = c.id
    WHERE 
        c.early_termination_penalty > 0
"""

# Executar a consulta SQL
filtered_data = pd.read_sql_query(query, conn)

# Fechar a conexão
conn.close()

# Converter `transfer_due_date` para datetime
filtered_data['transfer_due_date'] = pd.to_datetime(filtered_data['transfer_due_date'])

# Cálculo do ciclo de parcelas
def calculate_parcel_cycle_v4(df):
    first_due_date = df['transfer_due_date'].min()
    df['transfer_parcel_cycle'] = (
        ((df['transfer_due_date'].dt.year - first_due_date.year) * 12 +
         (df['transfer_due_date'].dt.month - first_due_date.month)) // 12 + 1
    )
    return df

filtered_data = filtered_data.groupby('contract_id').apply(calculate_parcel_cycle_v4).reset_index(drop=True)

# Cálculo dos valores reais de aluguel
filtered_data['transfer_real_rental_value'] = (
    filtered_data['transfer_total_value'] - 
    filtered_data['damage_value'].fillna(0) - 
    filtered_data['early_termination_value'].fillna(0)
)

# Cálculo de medianas por contrato e ciclo
filtered_data['median_rental_value'] = filtered_data.groupby(
    ['contract_id', 'transfer_parcel_cycle']
)['transfer_real_rental_value'].transform('median')

# Ordenar os dados por contrato, ciclo e data de vencimento
filtered_data.sort_values(by=['contract_id', 'transfer_parcel_cycle', 'transfer_due_date'], ascending=True, inplace=True)

# Criar o diretório de saída, se necessário
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Salvar o resultado em CSV
filtered_data.to_csv(output_path, sep=',', index=False)

print(f"Processo concluído! Resultado salvo em: {output_path}")
