import pandas as pd

# Variáveis para definir o limite da projeção
final_year = 2040
final_month = 12

# Dados realizados (últimos disponíveis)
data_realizado = {
    "index_name": ["igpm", "ipca", "inpc"],
    "month": [12, 12, 12],
    "year": [2024, 2024, 2024],
    "value": [6.54 / 100, 4.83 / 100, 4.77 / 100]  # Convertendo para formato decimal
}

df_realizado = pd.DataFrame(data_realizado)

print(f"[INFO] Base de dados realizada referente a {data_realizado['month'][0]}/{data_realizado['year'][0]}:")
for idx, row in df_realizado.iterrows():
    print(f"  - {row['index_name'].upper()}: {row['value']:.4f}")

# Projeções anuais (Focus e IPEA)
projecoes_anuais = {
    "year": [2025, 2026, 2027],
    "igpm": [4.26 / 100, 4.00 / 100, 3.94 / 100],
    "ipca": [4.10 / 100, 4.00 / 100, 3.62 / 100],
    "inpc": [4.20 / 100, 4.20 / 100, 4.20 / 100]  # Apenas uma entrada real
}

# Extendendo os últimos valores de igpm, ipca, e inpc até o limite
while projecoes_anuais["year"][-1] < final_year:
    projecoes_anuais["year"].append(projecoes_anuais["year"][-1] + 1)
    projecoes_anuais["igpm"].append(projecoes_anuais["igpm"][-1])
    projecoes_anuais["ipca"].append(projecoes_anuais["ipca"][-1])
    projecoes_anuais["inpc"].append(projecoes_anuais["inpc"][-1])

df_projecoes = pd.DataFrame(projecoes_anuais)

print("[INFO] Projeções anuais baseadas em fontes:")
print("  - Boletim Focus para IGP-M e IPCA")
print("  - IPEA para INPC")
for idx, row in df_projecoes.iterrows():
    print(f"  Ano {row['year']} - IGP-M: {row['igpm']:.4f}, IPCA: {row['ipca']:.4f}, INPC: {row['inpc']:.4f}")

# Função para interpolar valores corretamente

def interpolar_valores(start_value, end_value):
    step = (end_value - start_value) / 12  # Divisão pelos 12 meses para incluir janeiro corretamente
    valores = [start_value + step * i for i in range(1, 13)]
    return [round(v, 4) for v in valores]  # Arredondando para 4 casas decimais

# Função para gerar a tabela completa
def gerar_tabela(df_realizado, df_projecoes):
    tabela_completa = []

    for i in range(len(df_projecoes)):
        ano_atual = df_projecoes.iloc[i]["year"]

        for index_name in ["igpm", "ipca", "inpc"]:
            if i == 0:
                start_value = (
                    df_realizado[df_realizado["index_name"] == index_name]["value"].values[0]
                )
            else:
                start_value = df_projecoes.iloc[i - 1][index_name]

            end_value = df_projecoes.iloc[i][index_name]

            valores_interpolados = interpolar_valores(start_value, end_value)
            for mes, valor in enumerate(valores_interpolados, start=1):
                tabela_completa.append({
                    "index_name": index_name,
                    "month": mes,
                    "year": ano_atual,
                    "value": valor,
                })

    tabela_realizado = df_realizado.rename(columns={"value": "value"})
    tabela_realizado["source"] = "realizado"
    tabela_completa = pd.DataFrame(tabela_completa)
    tabela_completa["source"] = "previsto"

    tabela_completa = pd.concat([tabela_realizado, tabela_completa], ignore_index=True)

    tabela_completa = tabela_completa[
        (tabela_completa["year"] < final_year) | 
        ((tabela_completa["year"] == final_year) & (tabela_completa["month"] <= final_month))
    ]

    return tabela_completa

print("[INFO] Aplicando interpolação linear para projeções mensais...")
# Gerar tabela completa
resultado = gerar_tabela(df_realizado, df_projecoes)

# Salvar como CSV
output_path = "index_table.csv"
resultado.to_csv(output_path, index=False)

print(f"[INFO] Tabela criada e salva no caminho: {output_path}")
