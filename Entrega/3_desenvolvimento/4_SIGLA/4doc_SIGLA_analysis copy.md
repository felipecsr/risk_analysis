⬅️ [Retornar para Desenvolvimento](../desenvolvimento.md)

# 📺 Visão Geral: Etapas de Análise de Risco


Antes de nos aprofundarmos, vejamos um **resumo** do que cada etapa se propõe a fazer dentro de todo o processo de análise:

1. **Análise Preliminar (Histograma de duração dos contratos)**
    * **Objetivo**: Observar frequências de duração dos contratos, corrigir possíveis inconsistências de data e apoiar a definição de prazos em contratos com falhas (por exemplo, contratos sem data final informada).

2. **Fluxo de Caixa Passado (Criação e organização de dados históricos)**
    * **Objetivo**: Construir uma base consolidada de fluxos (entradas e saídas) efetivamente realizados, separando valores cobertos pela garantia (aluguéis, IPTU, condomínio etc.) e enriquecendo com informações adicionais (taxas, receitas financeiras).

3. **Cálculo do Fee Teórico e seu Consumo pelo Risco**
    * **Objetivo**: Com base nos valores efetivamente recebidos (historicamente), estimar quanto de “fee” (taxa de administração ou garantia) é gerado ao longo de diferentes períodos do contrato, levando em conta reajustes, parcelas e datas de liquidação/vencimento.

4. **Tratamento de Dados de Repasses para Projeção**
    * **Objetivo**: Em um script Python, executar a leitura das bases brutas de contratos e transferências, aplicar filtros e normalizações, unificá-las num dataset confiável, pronto para projeções e simulações de risco.

5. **Criação de Tabela de Inflação (Projeções de Índices)**
    * **Objetivo**: Gerar uma série mensal de índices de inflação futuros (IGP-M, IPCA, INPC), combinando valores realizados e projeções anuais, para serem aplicados nos reajustes de fluxos futuros.

6. **Projeção de Dados de Repasses de Contratos Ativos (com reajuste de inflação)**
    * **Objetivo**: Projetar automaticamente as parcelas futuras (repasses) dos contratos ativos, aplicando os índices de reajuste (inflação).

7. **Aplicação de Análise na Base de Repasses Projetados**
    * **Objetivo**: Utilizar esse fluxo projetado em consultas SQL para compor a análise de risco, distribuindo valores de aluguel/condomínio/IPTU, receitas financeiras, taxas de desconto (valor presente), penalidades por rescisão, exposição a danos etc.
	Essa última etapa permitirá chegar a bases consolidadas que permitem estimar a necessidade de reserva técnica (quanto a empresa precisa ter para cobrir eventuais sinistros e inadimplências numa determinada posição da carteira).  

<br/>

# 🕵 Detalhamento: Etapas de Análise de Risco

## 1) Análise Preliminar (Histograma de Duração dos Contratos)

Nesta **primeira etapa**, avalia-se a **frequência de duração dos contratos** (em dias) para entender o comportamento geral da carteira e corrigir inconsistências (por exemplo, contratos sem data de término). O código cria uma visão de “duração em dias” e depois agrupa em faixas de 30 dias para montar um **histograma**.

### Código utilizado

``` sql
	/* Versão 1 - 25/11/24 - Thiago Goularte */
	/* Procedimento inicial: Ativar conexão com alpop_data.sqlite */

	/* Análise Preliminar - 0a Cria variável de quantidade de dias de vigência de contrato, para análise de histograma */

	DROP VIEW IF EXISTS duracao_vigencia_contratos;

	CREATE VIEW duracao_vigencia_contratos AS
	SELECT
	    id,
	    start_date,
	    end_date,
	    termination_date,
	    status,
	    (JULIANDAY(end_date) - JULIANDAY(start_date)) AS vigencia_dias
	FROM
	    contracts_data as a
	ORDER BY
	    start_date, id;


	/* Análise Preliminar - 0b Cria tabela para análise de frequência por vigência de dias em histograma */

	SELECT
	    (FLOOR(vigencia_dias / 30) * 30) AS BinStart,
	    ((FLOOR(vigencia_dias / 30) + 1) * 30) AS BinEnd,
	    COUNT(*) AS Frequency
	FROM
	    duracao_vigencia_contratos
	GROUP BY
	    BinStart, BinEnd
	ORDER BY
	    BinStart;
	SELECT , COUNT(id) AS Frequency
	FROM duracao_vigencia_contratos
	GROUP BY vigencia_dias
```

### Comentários Explicativos

* **Criação da variável `vigencia_dias`**  
    Usamos `JULIANDAY(end_date) - JULIANDAY(start_date)` para calcular quantos dias de fato cada contrato ficou (ou ficaria) vigente.
* **Histograma em “bins” de 30 dias**  
    O comando agrupa as durações em intervalos de 30 dias (`FLOOR(vigencia_dias / 30) * 30`) para facilitar a visualização da quantidade de contratos que duram até 30 dias, de 31 a 60, etc.
* **Utilidade na análise de risco**  
    Ajuda a determinar a **duração típica** e a detectar outliers (muito longos ou muito curtos). Esses dados subsidiam a correção de inconsistências e serão úteis nas projeções posteriores, em especial para contratos que não têm data de término claramente definida.  

<br/>


## 2) Fluxo de Caixa Passado (Criação e Organização dos Dados Históricos)

Nesta etapa, concentramo-nos na **estruturação do fluxo de caixa efetivamente realizado**, separando o que foi recebido ou repassado, e marcando especificamente os valores cobertos pela garantia (aluguéis, condomínio, IPTU, danos, rescisão antecipada etc.).

### Código utilizado

``` sql
	WITH params AS ( SELECT '2024-12-31' AS cutoff_date, )

	/*1. Cria Fluxo de Caixa por contrato */


	/*1.1.1 Enriquece base de bills com informações dividindo o valor do bill por garantia*/
	DROP VIEW IF EXISTS guaranteed_bills1;

	CREATE VIEW guaranteed_bills1 AS
	SELECT
		a.*,
		CASE WHEN b.rental_value IS NULL THEN 0
		ELSE b.rental_value END AS rental_value,
		CASE WHEN b.damage_value IS NULL THEN 0
		ELSE b.damage_value END AS damage_value,
		CASE WHEN (c.damage_exposure IS NULL OR c.damage_exposure = 0) THEN 'FALSE'
		ELSE 'TRUE' END AS damage_guarantee,
		c.early_termination_penalty AS early_termination_guarantee
	FROM
	    bills_data AS a
	LEFT JOIN transfers_data AS b ON a.id = b.bill_id
	LEFT JOIN contracts_data AS c ON a.contract_id = c.id
	WHERE a.guaranteed = 'true';


	DROP VIEW IF EXISTS guaranteed_bills2;

	CREATE VIEW guaranteed_bills2 AS
	SELECT
		*,
		CASE WHEN rental_value IS NULL THEN 0
		ELSE rental_value END AS rental_value_guaranteed,
		CASE WHEN (damage_value IS NULL OR damage_guarantee = 'FALSE') THEN 0
		ELSE damage_value END AS damage_value_guaranteed,
		CASE WHEN (early_termination_value IS NULL OR early_termination_guarantee = 'false') THEN 0
		ELSE early_termination_value END AS early_termination_value_guaranteed,
		(value - damage_value - early_termination_value - alpop_fee - alpop_setup - financial_revenues) AS rental_cond_iptu_value_guaranteed
	FROM
	    guaranteed_bills1;


	/*1.1.2 Enriquece base de contracts com informações dividindo o valor do transfer por garantia*/

	DROP VIEW IF EXISTS guaranteed_transfers1a;

	CREATE VIEW guaranteed_transfers1a AS
	SELECT
		a.*,
		b.guaranteed,
	/*	CASE WHEN a.rental_value IS NULL THEN 0
		ELSE a.rental_value END AS rental_value_guaranteed,*/
		CASE WHEN (a.damage_value IS NULL OR a.damage_guarantee = 'FALSE') THEN 0
		ELSE a.damage_value END AS damage_value_guaranteed,
		CASE WHEN (a.early_termination_value IS NULL OR a.early_termination_guarantee = 'false') THEN 0
		ELSE a.early_termination_value END AS early_termination_value_guaranteed
	FROM
	    transfers_with_guarantees_info AS a
	LEFT JOIN bills_data AS b ON a.bill_id = b.id and a.contract_id = b.contract_id;

	/* Trata casos de primeiro transfer = 0 e imobiliária Alpop (59) */
	DROP VIEW IF EXISTS guaranteed_transfers1b;

	CREATE VIEW guaranteed_transfers1b AS
	SELECT
		a.*,
		CASE WHEN a.liquidation_date <> '' THEN a.liquidation_date
		ELSE a.due_date END AS transfer_cash_flow_date,
		b.value AS bill_value,
		b.alpop_fee,
		b.alpop_setup,
		b.financial_revenues
	FROM
	    guaranteed_transfers1a as a
	LEFT JOIN bills_data AS b ON a.bill_id = b.id and a.contract_id = b.contract_id;


	/* Trata casos de primeiro transfer = 0 e imobiliária Alpop (59)*/
	DROP VIEW IF EXISTS guaranteed_transfers1c;

	CREATE VIEW guaranteed_transfers1c AS
	SELECT
		*,
	    ROW_NUMBER() OVER (
		PARTITION BY contract_id
	    ORDER BY transfer_cash_flow_date
	    ) AS transfer_cash_flow_order,
	    (bill_value - alpop_fee - alpop_setup - financial_revenues) AS transferred_bill_value
	FROM
	    guaranteed_transfers1b
	    

	DROP VIEW IF EXISTS guaranteed_transfers1d;

	CREATE VIEW guaranteed_transfers1d AS
	SELECT
		*,
		transferred_bill_value AS alpop_value_adjusted
	FROM
	    guaranteed_transfers1c
	WHERE value = 0 AND transfer_cash_flow_order = 1 AND real_estate_agency_id = 59

	/* Junta valores de transfers com valores ajustados para primeiro transfer = 0 quando imobiliária é Alpop*/
	DROP VIEW IF EXISTS guaranteed_transfers2;

	CREATE VIEW guaranteed_transfers2 AS
	SELECT
		a.*,
		b.alpop_value_adjusted,
		CASE WHEN b.alpop_value_adjusted IS NULL THEN a.value
		ELSE b.alpop_value_adjusted END AS value_adjusted
	FROM
	    guaranteed_transfers1a AS a
	LEFT JOIN guaranteed_transfers1d AS b ON a.id = b.id


	DROP VIEW IF EXISTS guaranteed_transfers3;

	CREATE VIEW guaranteed_transfers3 AS
	SELECT
		*,
		(value_adjusted - damage_value - early_termination_value) AS rental_cond_iptu_value_guaranteed
	FROM
	    guaranteed_transfers2
	WHERE guaranteed IS NULL or guaranteed = 'true';




	/*1.2 Cria base de fluxo de caixa por contrato*/
	/*REVER: FALTA INCLUIR SEPARAÇÃO DE TRANSFERS E BILLS POR GARANTIA: DEPENDE DE RECEBIMENTO DE NOVA BASE DE DADOS*/
	DROP VIEW IF EXISTS contract_cash_flow_1;

	CREATE VIEW contract_cash_flow_1 AS
	SELECT
		contract_id,
		real_estate_agency_id,
		0 AS alpop_fee,
		0 AS alpop_setup,
		0 AS financial_revenues,
		-rental_cond_iptu_value_guaranteed AS rental_cond_iptu_value_guaranteed,
		-damage_value_guaranteed AS damage_value_guaranteed,
		-early_termination_value_guaranteed AS early_termination_value_guaranteed,
		liquidation_date,
		due_date,
		CASE WHEN liquidation_date <> '' THEN liquidation_date
		ELSE due_date END AS cash_flow_date,
		CASE
			WHEN liquidation_date <> '' AND liquidation_date <= (SELECT cutoff_date FROM params) THEN 'liquidated'
			WHEN due_date > (SELECT cutoff_date FROM params) THEN 'predicted'
			ELSE 'not liquidated'
		END AS liquidated,
	/*	value AS billed_value,
		-value AS insured_flow_value,*/
		'transfers' AS cash_flow_type
	FROM
	    guaranteed_transfers3
	UNION ALL
	SELECT
		contract_id,
		real_estate_agency_id,
		alpop_fee,
		alpop_setup,
		financial_revenues,
		rental_cond_iptu_value_guaranteed,
		damage_value_guaranteed,
		early_termination_value_guaranteed,
		payment_date AS liquidation_date,
		due_date,
		CASE WHEN payment_date <> '' THEN payment_date
		ELSE due_date END AS cash_flow_date,
		CASE
			WHEN payment_date <> '' AND payment_date <= (SELECT cutoff_date FROM params) THEN 'liquidated'
			WHEN due_date > (SELECT cutoff_date FROM params) THEN 'predicted'
			ELSE 'not liquidated'
		END AS liquidated,
	/*	value AS billed_value,
		CASE
			WHEN payment_date <> '' AND due_date <= '2024-12-31' THEN (value - alpop_fee - alpop_setup - financial_revenues)
			ELSE 0
		END AS insured_flow_value,*/
		'bills' AS cash_flow_type
	FROM
	    guaranteed_bills2
	ORDER BY contract_id, cash_flow_date, cash_flow_type;


	/* Mantém apenas fluxo de caixa efetivado, isto é, lançamentos que foram liquidados */
	DROP VIEW IF EXISTS contract_cash_flow_2;

	CREATE VIEW contract_cash_flow_2 AS
	SELECT
		a.*,
		b.start_date,
		b.end_date,
		b.termination_date,
		b.damage_fee,
		b.damage_exposure,
		b.early_termination_penalty
	FROM contract_cash_flow_1 as a
	LEFT JOIN contracts_data as b ON a.contract_id = b.id
	WHERE a.liquidated = 'liquidated';

	/* 1.2 Calcula fluxo de caixa acumulado por garantia */
	DROP VIEW IF EXISTS contract_cash_flow_3;

	CREATE VIEW contract_cash_flow_3 AS
	SELECT
	    *,
	    ROW_NUMBER() OVER () AS RowAsc,
	    strftime('%Y-%m', cash_flow_date) AS cash_flow_year_month,
	    strftime('%Y-%m', start_date) AS start_month,
	    strftime('%Y-%m', end_date) AS end_month,
	    ((strftime('%Y', end_date) - strftime('%Y', start_date)) * 12 + (strftime('%m', end_date) - strftime('%m', start_date))) AS contract_duration,
	    ((strftime('%Y', termination_date) - strftime('%Y', start_date)) * 12 + (strftime('%m', termination_date) - strftime('%m', start_date))) AS termination_duration,
	    ((strftime('%Y', cash_flow_date) - strftime('%Y', start_date)) * 12 + (strftime('%m', cash_flow_date) - strftime('%m', start_date))) AS cash_flow_period_months,
	    /*FLOOR(((strftime('%Y', cash_flow_date) - strftime('%Y', start_date)) * 12 + (strftime('%m', cash_flow_date) - strftime('%m', start_date))) / 12) AS cash_flow_period_yrs,*/
	    ROUND(SUM(rental_cond_iptu_value_guaranteed) OVER (
	        PARTITION BY contract_id
	        ORDER BY cash_flow_date ), 2) AS cum_rental_cond_iptu_flow_value,
	    ROUND(SUM(damage_value_guaranteed) OVER (
	        PARTITION BY contract_id
	        ORDER BY cash_flow_date ), 2) AS cum_damage_flow_value,
	    ROUND(SUM(early_termination_value_guaranteed) OVER (
	        PARTITION BY contract_id
	        ORDER BY cash_flow_date ), 2) AS cum_early_term_flow_value,
	    ROUND(SUM(alpop_fee) OVER (
	        PARTITION BY contract_id
	        ORDER BY cash_flow_date ), 2) AS cum_fee_flow_value,
	    ROUND(SUM(alpop_setup) OVER (
	        PARTITION BY contract_id
	        ORDER BY cash_flow_date ), 2) AS cum_setup_flow_value,
	    ROUND(SUM(financial_revenues) OVER (
	        PARTITION BY contract_id
	        ORDER BY cash_flow_date ), 2) AS cum_fin_rev_flow_value
	FROM
	    contract_cash_flow_2
	ORDER BY
	    contract_id, cash_flow_date;




	/*1.3 Cria base de fluxo de caixa mensal por contrato - garantia de aluguel + condomínio + IPTU*/
	DROP VIEW IF EXISTS contract_cash_flow_4;

	CREATE VIEW contract_cash_flow_4 AS
	SELECT
	    contract_id,
	    CASE
	    	WHEN contract_duration IS NULL THEN 30
	    	ELSE contract_duration
	    END AS contract_duration_adj,
	    CASE
		    WHEN termination_duration IS NOT NULL THEN termination_duration
	    	WHEN contract_duration IS NULL THEN 30
	    	ELSE contract_duration
	    END AS contract_effective_duration_adj,
	    cash_flow_year_month,
	    cash_flow_period_months,
	    (1 + FLOOR((cash_flow_period_months - 1 )/ 12)) AS contract_year,
	    start_month,
	    end_month,
	    SUM(rental_cond_iptu_value_guaranteed) AS rental_cond_iptu_value_flow_monthly,
	    SUM(financial_revenues) AS fin_rev_value_flow_monthly,
	    SUM(damage_value_guaranteed) AS damage_value_flow_monthly,
	    SUM(early_termination_value_guaranteed) AS early_termination_value_flow_monthly,
	    SUM(alpop_fee) AS alpop_fee_flow_monthly,
	    SUM(alpop_setup) AS alpop_setup_flow_monthly
	FROM
	    contract_cash_flow_3
	GROUP BY
	    contract_id,
	    cash_flow_year_month;
```

### Comentários Explicativos

1. **Criação das Views `guaranteed_bills1` e `guaranteed_bills2`**
    * Separam os boletos (`bills_data`) que são efetivamente cobertos pela garantia, desmembrando valores de aluguel, danos, rescisão etc.
2. **Criação das Views `guaranteed_transfers1x` e `guaranteed_transfers2/3`**
    * Faz algo análogo para os repasses (`transfers`) vinculados aos mesmos contratos, tratando particularidades (p. ex.: primeiro repasse zerado para imobiliárias específicas).
3. **`contract_cash_flow_1`, `contract_cash_flow_2` e `contract_cash_flow_3`**
    * Consolida tudo em um **fluxo de caixa** por contrato.
    * Filtra apenas o que foi efetivamente liquidado (já aconteceu) para compor a base histórica.
    * Calcula o **fluxo acumulado** de aluguel+condomínio+IPTU, danos e rescisão, a cada registro de caixa.
4. **`contract_cash_flow_4`**
    * Agrega esses valores a nível **mensal**, determinando a “duração efetiva” do contrato, considerando ou não a data de término antecipada (`termination_date`).
    * Esse resultado final é uma tabela mensal por contrato, já separando os diferentes tipos de valores (rental, fees, financial revenues), servindo de insumo para análises de comportamento da carteira.

<br/>

## 3) Cálculo do Fee Teórico e seu Consumo pelo Risco

Nesta etapa, o foco está em entender **quanto de fee** (geralmente uma taxa de administração ou garantia de aluguel) é gerado ao longo de cada ano de contrato, embasando-se em dados históricos de pagamento (transferências), levando em conta que os aluguéis podem mudar com o passar dos anos.

### Código utilizado

``` sql
	/*1. Calcula fee teórico por mês = fee do contrato * valor de transfer (Garantia (Aluguel + IPTU + Cond))*/

	/*1.1 Calcula data mais antiga de transfer no fluxo - seja a data de liquidação de transfer (liquidation_date) ou data de vencimento (due_date) de transfer*/
	/*1.1.1 Calcula data mais antiga de due_date de transfer, por contrato*/
	DROP VIEW IF EXISTS min_date_by_contract_1a;

	CREATE VIEW min_date_by_contract_1a AS
	SELECT
		contract_id,
	    MIN(due_date) AS oldest_due_date
	FROM 
	    guaranteed_transfers3
	WHERE due_date <> ''
	GROUP BY 
	    contract_id;

	/*1.1.2 Calcula data mais antiga de liquidation_date de transfer, por contrato*/   
	DROP VIEW IF EXISTS min_date_by_contract_1b;

	CREATE VIEW min_date_by_contract_1b AS
	SELECT
		contract_id,
	    MIN(liquidation_date) AS oldest_liquidation_date
	/*    MIN(due_date) AS oldest_due_date*/
	FROM 
	    guaranteed_transfers3
	WHERE liquidation_date <> ''
	GROUP BY
	    contract_id;
	   
	/*1.1.3 Calcula mais antigo entre (due_date mais antigo, liquidation_date mais antigo) de transfers, por contrato*/   
	DROP VIEW IF EXISTS min_date_by_contract_2;

	CREATE VIEW min_date_by_contract_2 AS
	SELECT
		a.contract_id,
		a.oldest_due_date,
	    b.oldest_liquidation_date,
	    CASE
		    WHEN (oldest_liquidation_date < oldest_due_date AND oldest_liquidation_date <> '') THEN oldest_liquidation_date
		    ELSE oldest_due_date
		END AS min_date
	FROM 
	    min_date_by_contract_1a as a
	LEFT JOIN min_date_by_contract_1b as b ON a.contract_id = b.contract_id;



	/*1.2 Calcula fee teórico por contrato, considerando reajustes de valor de transfer*/
	/*1.2.1 Calcula diferença entre data de liquidação (liquidation_date) e data de vencimento (due_date) de transfers*/
	DROP VIEW IF EXISTS average_yearly_fee_1;

	CREATE VIEW average_yearly_fee_1 AS
	SELECT
		a.*,
		b.oldest_due_date,
		b.oldest_liquidation_date,
		b.min_date,
		(JULIANDAY(a.liquidation_date) - JULIANDAY(b.min_date)) AS liq_min_date_diff,
		(JULIANDAY(a.due_date) - JULIANDAY(b.min_date)) AS due_min_date_diff,
		(strftime('%Y', a.liquidation_date) - strftime('%Y', b.min_date)) * 12 + (strftime('%m', a.liquidation_date) - strftime('%m', b.min_date)) AS liq_min_date_diff_month,
		(strftime('%Y', a.due_date) - strftime('%Y', b.min_date)) * 12 + (strftime('%m', a.due_date) - strftime('%m', b.min_date)) AS due_min_date_diff_month
	/*	(ROUND((JULIANDAY(a.liquidation_date) - JULIANDAY(b.min_date)) / 30, 0)) AS liq_min_date_diff_month,
		(ROUND((JULIANDAY(a.due_date) - JULIANDAY(b.min_date)) / 30, 0)) AS due_min_date_diff_month*/
	FROM
	    guaranteed_transfers3 as a
	LEFT JOIN
		min_date_by_contract_2 as b ON a.contract_id = b.contract_id
	ORDER BY contract_id, due_date, liquidation_date;

	/*1.2.2 Se liquidação ocorreu antes de vencimento, considera (diferença de datas = data de liquidação - primeiro fluxo de caixa).
	 Caso contrário, (diferença de datas = data de vencimento - primeiro fluxo de caixa) */
	DROP VIEW IF EXISTS average_yearly_fee_2;

	CREATE VIEW average_yearly_fee_2 AS
	SELECT
		*,
		CASE
		    WHEN liq_min_date_diff < due_min_date_diff AND liq_min_date_diff is NOT NULL THEN liq_min_date_diff
		    ELSE due_min_date_diff
		END AS min_date_diff,
		CASE
		    WHEN liq_min_date_diff < due_min_date_diff AND liq_min_date_diff is NOT NULL THEN liq_min_date_diff_month
		    ELSE due_min_date_diff_month
		END AS min_date_diff_month

	FROM
	    average_yearly_fee_1
	ORDER BY contract_id, min_date_diff;


	/*1.2.3 Calcula diferença de datas em anos - já que reajustes costumam ser após 1, 2, 3... anos de contrato*/
	DROP VIEW IF EXISTS average_yearly_fee_3;

	CREATE VIEW average_yearly_fee_3 AS
	SELECT
		contract_id,
		min_date_diff,
		(1 + FLOOR(min_date_diff/ 365)) AS min_date_diff_yrs,
		min_date_diff_month,
		value
	FROM
	    average_yearly_fee_2
	ORDER BY contract_id ASC, min_date_diff DESC;


	/*1.2.4 Calcula quantidade de transfers e soma de valor feito de transfer para cada ano de contrato (1o ano, 2o ano, ...)*/
	DROP VIEW IF EXISTS average_yearly_fee_4;

	CREATE VIEW average_yearly_fee_4 AS
	SELECT
		contract_id,
		min_date_diff,
		min_date_diff_month,
		min_date_diff_yrs,
		MAX(min_date_diff) AS max_min_date_diff_yr_grp,
		SUM(value) AS value_sum_year,
		SUM(CASE WHEN value <> 0 THEN 1 ELSE 0 END) AS non_zero_values
	FROM
	    average_yearly_fee_3
	GROUP BY contract_id, min_date_diff_yrs
	ORDER BY contract_id ASC, min_date_diff;


	/* 1.2.4a - Conferência: conta frequência quantidade anual de transfers).
	 Investigar se casos com mais de 13 transfers por ano são plausíveis operacionalmente*/
	SELECT non_zero_values, COUNT(*) AS Frequency
	FROM average_yearly_fee_4
	GROUP BY non_zero_values


	/*1.2.5.1. Calcula estatísticas descritivas de fee (em %) - base contracts*/
	DROP VIEW IF EXISTS average_yearly_fee_5a;
	CREATE VIEW average_yearly_fee_5a AS
	SELECT MIN(rent_fee) AS MinValue, MAX(rent_fee) AS MaxValue, AVG(rent_fee) AS AvgValue, 0 AS fee FROM contracts_data;

	/*1.2.5.2. Calcula valor mensal de transfers feitos e multiplica por fee original de contrato (base contracts)*/
	DROP VIEW IF EXISTS average_yearly_fee_5b;

	CREATE VIEW average_yearly_fee_5b AS
	SELECT
		a.contract_id,
		a.max_min_date_diff_yr_grp,
		a.value_sum_year,
		a.non_zero_values,
		a.min_date_diff_yrs AS contract_year,
		b.rent_fee,
		b.damage_fee,
		b.early_termination_penalty,
		c.AvgValue AS avg_historical_rent_fee_perc,
		CASE
			WHEN a.non_zero_values <= 0 THEN (a.value_sum_year / 1)
			ELSE (a.value_sum_year / a.non_zero_values)
		END AS monthly_value
	FROM
	    average_yearly_fee_4 as a
	LEFT JOIN contracts_data as b ON a.contract_id = b.id
	LEFT JOIN average_yearly_fee_5a as c ON b.rent_fee = c.fee
	ORDER BY contract_id, min_date_diff_yrs DESC;


	/*1.2.5.3. Calcula valor mensal de transfers feitos e multiplica por fee original de contrato (base contracts)*/
	DROP VIEW IF EXISTS average_yearly_fee_6;

	CREATE VIEW average_yearly_fee_6 AS
	SELECT
		*,
		CASE
			WHEN avg_historical_rent_fee_perc IS NULL THEN rent_fee
			ELSE ROUND(avg_historical_rent_fee_perc, 0)
		END AS rent_fee_adj
	FROM
	    average_yearly_fee_5b
	ORDER BY contract_id, contract_year DESC;



	DROP VIEW IF EXISTS average_yearly_fee_7;

	CREATE VIEW average_yearly_fee_7 AS
	SELECT
		*,
		CASE
			WHEN non_zero_values <= 0 THEN ((rent_fee_adj / 100) * value_sum_year / 1)
			ELSE ((rent_fee_adj / 100) * value_sum_year / non_zero_values)
		END AS monthly_fee_reais
	FROM
	    average_yearly_fee_6
	ORDER BY contract_id, contract_year DESC;


	/* 1.2.5.2a Analisa distribuição de valor mensal de transfer (em reais) 
	 Investigar valores zerados*/
	SELECT MIN(monthly_fee_reais) AS MinValue, MAX(monthly_fee_reais) AS MaxValue FROM average_yearly_fee_7;

	SELECT
	    (FLOOR(monthly_fee_reais / 100) * 100) AS BinStart,
	    ((FLOOR(monthly_fee_reais / 100) + 1) * 100) AS BinEnd,
	    COUNT(*) AS Frequency
	FROM
	    average_yearly_fee_7
	GROUP BY
	    BinStart, BinEnd
	ORDER BY
	    BinStart;
```

### Comentários Explicativos

1. **Data inicial (min_date) de cada contrato**
    * Em `min_date_by_contract_1a` e `min_date_by_contract_1b`, identificamos a data de vencimento mais antiga e a data de liquidação mais antiga, respectivamente. Em `min_date_by_contract_2`, selecionamos qual delas efetivamente será a data inicial de referência do contrato.
2. **Diferenças de datas e organização em anos de contrato**
    * O código calcula há quantos dias cada repasse ocorre depois da data inicial, para então converter isso em “ano 1”, “ano 2” etc. Esse agrupamento reflete que muitos contratos têm reajustes anuais de aluguel.
3. **Cálculo do valor médio mensal e aplicação do fee**
    * Determina-se quanto foi efetivamente pago de aluguel (ou valor coberto) em média por mês, e multiplica pela taxa (fee) do contrato.
    * Em `average_yearly_fee_7`, chamamos esse resultado de `monthly_fee_reais`, que indica quanto, em média, foi gerado de fee a cada mês do “ano X” daquele contrato.
4. **Importância para a análise de risco**
    * Esses dados alimentam projeções futuras e ajudam a estimar **quanto a empresa efetivamente obtém de receita** (e, por consequência, qual parte do fee pode estar sendo consumida para cobrir riscos de inadimplência ou sinistros).

<br/>

## 4) Tratamento de Dados (ETL em Python)

Aqui temos um **script em Python** que faz a leitura e consolidação dos dados (contratos e transferências) para gerar uma base confiável. Filtra contratos ativos, normaliza datas, inclui campos calculados (por exemplo, número de parcelas já pagas) e salva num CSV pronto para projeções.

### Código utilizado

``` python
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
```

### Comentários Explicativos

* **Leitura e Merge das tabelas**  
    Lê os arquivos `contracts_data.csv` e `transfers_data.csv`, unifica-os (merge), garantindo nomes únicos para colunas conflitantes.
* **Filtragem**  
    Fica apenas com contratos “ativos” (sem `termination_date`) cujos repasses sejam anteriores à data de corte (`2024-12-31`) e cujas transferências estejam liquidadas.
* **Campos calculados**
    * `contract_original_duration` e `contract_current_duration` (em meses)
    * `transfer_parcel_cycle`, que marca o ciclo de pagamento (1º ano, 2º ano, etc.)
    * `transfer_real_rental_value`, descontando valores de danos e rescisão
    * `median_rental_value`, que mede a mediana do valor efetivo pago em cada ciclo (útil para projeções)
* **Exportação**  
    Ao final, salva tudo em `transfers_projection_ETL_result.csv`, que será a base confiável para as próximas fases de projeção e análise de risco.

<br/>

## 5) Criação da Tabela de Inflação (Projeções de Índices)

Aqui, geramos os **valores mensais de IGP-M, IPCA e INPC**, com base em dados já realizados e projeções anuais (p. ex., do Boletim Focus e IPEA). O código faz uma **interpolação linear** para produzir valores mensais até 2040.

### Código utilizado

``` python
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
```

### Comentários Explicativos

* **Dados realizados x Projeções anuais**  
    Carrega-se o valor de inflação já **realizado** em dezembro/2024 e algumas projeções para anos futuros (2025, 2026, 2027 etc.).
* **Interpolação Mensal**  
    Para cada ano projetado, faz-se uma interpolação linear entre o índice do final de um ano e o índice do final do ano seguinte, gerando 12 valores para cada intervalo. Assim se obtém uma **série mensal** até 2040.
* **Fonte**  
    Boletim Focus (IGP-M, IPCA) e IPEA (INPC), neste exemplo hipotético.
* **Saída**  
    Salva em `index_table.csv` uma planilha com (index_name, month, year, value, source = ‘previsto’ ou ‘realizado’).

<br/>

## 6) Projeção de Transfers Reajustados pela Inflaçãoe Aplicação da Análise na Base Futura


Este código **cria parcelas futuras** (uma “linha do tempo” prospectiva) para cada contrato que ainda está “ativo” ou em “holdover”, aplicando o índice de reajuste (IPCA, IGP-M, etc.) conforme o mês.

#### Código utilizado

``` python
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
```

#### Comentários Explicativos

* **Detecção do status**  
    Se o contrato está em “holdover” (prazo formal venceu, mas a ocupação continua), gera-se apenas **uma parcela adicional** a cada mês seguinte, com um eventual reajuste no valor médio.
* **Contratos ainda em vigência**  
    Se o contrato ainda está “dentro do prazo” (`in_term`), projetam-se as parcelas faltantes até atingir a **duração original** do contrato, aplicando reajustes a cada novo ciclo anual.
* **Reajuste**  
    Para cada nova parcela, lê-se a tabela de índices (`index_table.csv`) e aplica-se a variação `index_value` para reajustar o valor de aluguel mediano.
* **Saída**  
    Gera linhas futuras, atribuindo ID para a parcela projetada (`transfer_id`), definindo valores zerados para `transfer_total_value` (pois ainda não foram realizados) e salvando tudo no arquivo `transfer_projection_result.csv`.

<br/>

## 7) Aplicação da Análise na Base de Transfers Projetados

Por fim, há um bloco extenso de consultas SQL que **lê a base projetada** (histórico + futuro) e calcula distribuições de valores, soma de aluguéis + condomínio + IPTU ao longo do tempo do contrato, taxas de desconto para trazer valores a valor presente e assim por diante.

Essas operações fornecem **indicadores de risco**, como a quantidade total de receita projetada, a distribuição temporal de fluxos, a exposição a danos, a parcela que seria coberta por penalidades de rescisão etc. A depender da modelagem de sinistros e inadimplência, cruzam-se essas projeções para dimensionar a **reserva técnica**.

#### Código utilizado

``` sql
	DROP VIEW IF EXISTS past_contract_cash_flow_rent_cond_iptu;

	CREATE VIEW past_contract_cash_flow_rent_cond_iptu AS
	SELECT
	    a.*,
	    b.damage_fee,
	    b.early_termination_penalty,
	    b.rent_fee_adj,
	    b.monthly_fee_reais,
	    c.median_rental_value,
	    CASE
		    WHEN b.damage_fee = 0 THEN 0
		    ELSE c.median_rental_value * 3
		END AS theoretical_damage_exposure,
	    CASE
		    WHEN b.early_termination_penalty = 'false' THEN 0
		    ELSE c.median_rental_value
		END AS theoretical_early_term_exposure/*,
		((CAST(strftime('%Y', end_month) AS INTEGER) - CAST(strftime('%Y', start_month) AS INTEGER)) * 12 + 
			(CAST(strftime('%m', end_month) AS INTEGER) - CAST(strftime('%m', start_month) AS INTEGER))) AS month_diff*/
	FROM
	    contract_cash_flow_4 AS a
	LEFT JOIN average_yearly_fee_7 AS b ON a.contract_id = b.contract_id AND a.contract_year = b.contract_year
	LEFT JOIN early_median_result_liq_date_no_parameters AS c ON a.contract_id = c.contract_id/* AND a.contract_year = c.transfer_parcel_cycle*/
	WHERE a.start_month <= '2022-06' AND a.contract_duration_adj = 30
	ORDER BY
	    contract_id, cash_flow_year_month;


	/* Calcula distribuição de quantidade de meses efetiva de contrato - contratos de 30 meses*/
	SELECT
		contract_effective_duration_adj,
		COUNT(contract_id) AS contract_effective_duration_frequency
	FROM
	    past_contract_cash_flow_rent_cond_iptu
	GROUP BY contract_effective_duration_adj;


	/*Análise: resultado por Safra*/
	SELECT
		start_month,
	    SUM(ROUND(monthly_fee_reais, 2)) AS theoretical_fee_flow,
	    SUM(ROUND(rental_cond_iptu_value_flow_monthly, 2)) AS rental_cond_iptu_flow,
	    SUM(ROUND(fin_rev_value_flow_monthly, 2)) AS fin_rev_flow
	FROM
	    past_contract_cash_flow_rent_cond_iptu
	GROUP BY start_month;


	/*Calcula relação entre fees e despesas com transfer descobertos e receita financeira*/
	DROP VIEW IF EXISTS expected_value_rental_cond_iptu_fin_rev;

	CREATE VIEW expected_value_rental_cond_iptu_fin_rev AS
	SELECT
	    SUM(ROUND(monthly_fee_reais, 2)) AS theoretical_fee_flow,
	    SUM(ROUND(rental_cond_iptu_value_flow_monthly, 2)) AS rental_cond_iptu_flow,
	    SUM(ROUND(fin_rev_value_flow_monthly, 2)) AS fin_rev_flow,
	    SUM(ROUND(rental_cond_iptu_value_flow_monthly, 2)) / SUM(ROUND(monthly_fee_reais, 2)) AS expected_rental_cond_iptu_fee,
	    SUM(ROUND(fin_rev_value_flow_monthly, 2)) / SUM(ROUND(monthly_fee_reais, 2)) AS expected_fin_rev_fee
	FROM
	    past_contract_cash_flow_rent_cond_iptu;


	/* Calcula distribuição no tempo de despesas com rental_cond_iptu - contratos de 30 meses*/
	DROP TABLE IF EXISTS expected_distr_rental_cond_iptu_fin_rev0;

	CREATE TABLE expected_distr_rental_cond_iptu_fin_rev0 AS
	SELECT
		cash_flow_period_months,
		ROUND(100 * cash_flow_period_months / 30, 0) AS contract_duration_perc,
	    /*SUM(ROUND(monthly_fee_reais, 2)) AS theoretical_fee_distr,*/
	    SUM(ROUND(rental_cond_iptu_value_flow_monthly, 2)) AS rental_cond_iptu_distr,
	    SUM(ROUND(fin_rev_value_flow_monthly, 2)) AS fin_rev_flow_distr,
	    ROUND(
	        SUM(rental_cond_iptu_value_flow_monthly) /
	        (SELECT SUM(rental_cond_iptu_value_flow_monthly) FROM past_contract_cash_flow_rent_cond_iptu),
	        4
	    ) AS expected_distr_rental_cond_iptu_fee_raw,
	    ROUND(
	        SUM(fin_rev_value_flow_monthly) /
	        (SELECT SUM(fin_rev_value_flow_monthly) FROM past_contract_cash_flow_rent_cond_iptu),
	        4
	    ) AS expected_distr_fin_rev_fee_raw,
	    ROUND(
	        SUM(rental_cond_iptu_value_flow_monthly) /
	        (SELECT SUM(rental_cond_iptu_value_flow_monthly) FROM past_contract_cash_flow_rent_cond_iptu),
	        4
	    ) AS expected_distr_rental_cond_iptu_fee,
	    ROUND(
	        SUM(rental_cond_iptu_value_flow_monthly) /
	        (SELECT SUM(rental_cond_iptu_value_flow_monthly) FROM past_contract_cash_flow_rent_cond_iptu),
	        4
	    ) AS cum_distr_rental_cond_iptu_fee_aux,
	    ROUND(
	        SUM(fin_rev_value_flow_monthly) /
	        (SELECT SUM(fin_rev_value_flow_monthly) FROM past_contract_cash_flow_rent_cond_iptu),
	        4
	    ) AS expected_distr_fin_rev_fee,
	    ROUND(
	        SUM(fin_rev_value_flow_monthly) /
	        (SELECT SUM(fin_rev_value_flow_monthly) FROM past_contract_cash_flow_rent_cond_iptu),
	        4
	    ) AS cum_distr_fin_rev_fee_aux
	/*    SUM(ROUND(rental_cond_iptu_value_flow_monthly, 2)) / SUM(ROUND(monthly_fee_reais, 2)) AS expected_distr_rental_cond_iptu_fee,
	    SUM(ROUND(fin_rev_value_flow_monthly, 2)) / SUM(ROUND(monthly_fee_reais, 2)) AS expected_distr_fin_rev_fee*/
	FROM
	    past_contract_cash_flow_rent_cond_iptu
	GROUP BY cash_flow_period_months;


	UPDATE expected_distr_rental_cond_iptu_fin_rev0
	SET expected_distr_rental_cond_iptu_fee = (
	    SELECT SUM(expected_distr_rental_cond_iptu_fee_raw)
	    FROM expected_distr_rental_cond_iptu_fin_rev0
	    WHERE cash_flow_period_months BETWEEN -1 AND 2
	),
	    cum_distr_rental_cond_iptu_fee_aux = (
	    SELECT SUM(expected_distr_rental_cond_iptu_fee_raw)
	    FROM expected_distr_rental_cond_iptu_fin_rev0
	    WHERE cash_flow_period_months BETWEEN -1 AND 2
	),
		expected_distr_fin_rev_fee = (
	    SELECT SUM(expected_distr_fin_rev_fee_raw)
	    FROM expected_distr_rental_cond_iptu_fin_rev0
	    WHERE cash_flow_period_months BETWEEN -1 AND 2
	),
		cum_distr_fin_rev_fee_aux = (
	    SELECT SUM(expected_distr_fin_rev_fee_raw)
	    FROM expected_distr_rental_cond_iptu_fin_rev0
	    WHERE cash_flow_period_months BETWEEN -1 AND 2
	)
	WHERE cash_flow_period_months = 2;

	-- Rule (ii): Update the value on the "30" line
	UPDATE expected_distr_rental_cond_iptu_fin_rev0
	SET expected_distr_rental_cond_iptu_fee = 1 - (
	    SELECT SUM(expected_distr_rental_cond_iptu_fee_raw)
	    FROM expected_distr_rental_cond_iptu_fin_rev0
	    WHERE cash_flow_period_months < 30
	),
		expected_distr_fin_rev_fee = 1 - (
	    SELECT SUM(expected_distr_fin_rev_fee_raw)
	    FROM expected_distr_rental_cond_iptu_fin_rev0
	    WHERE cash_flow_period_months < 30
	),
		cum_distr_rental_cond_iptu_fee_aux = 1,
		cum_distr_fin_rev_fee_aux = 1
	WHERE cash_flow_period_months = 30;


	/**/
	UPDATE expected_distr_rental_cond_iptu_fin_rev0
	SET expected_distr_rental_cond_iptu_fee = 0,
	    expected_distr_fin_rev_fee = 0,
	    cum_distr_rental_cond_iptu_fee_aux = 0,
	    cum_distr_fin_rev_fee_aux = 0
	WHERE cash_flow_period_months < 2;

	UPDATE expected_distr_rental_cond_iptu_fin_rev0
	SET expected_distr_rental_cond_iptu_fee = 0,
	    expected_distr_fin_rev_fee = 0,
	    cum_distr_rental_cond_iptu_fee_aux = 1,
	    cum_distr_fin_rev_fee_aux = 1
	WHERE cash_flow_period_months > 30;

	DELETE FROM expected_distr_rental_cond_iptu_fin_rev0
	WHERE cash_flow_period_months NOT BETWEEN 2 AND 30;


	DROP TABLE IF EXISTS expected_distr_rental_cond_iptu_fin_rev1a;

	DROP TABLE IF EXISTS expected_distr_rental_cond_iptu_fin_rev1b;

	DROP TABLE IF EXISTS expected_distr_rental_cond_iptu_fin_rev2;

	DROP TABLE IF EXISTS expected_distr_rental_cond_iptu_fin_rev3;


	CREATE TABLE expected_distr_rental_cond_iptu_fin_rev1a AS
	SELECT
		cash_flow_period_months,
		contract_duration_perc,
		expected_distr_rental_cond_iptu_fee,
		expected_distr_fin_rev_fee,
		CASE
			WHEN cash_flow_period_months = 2 THEN cum_distr_rental_cond_iptu_fee_aux
			WHEN cash_flow_period_months = 30 THEN cum_distr_rental_cond_iptu_fee_aux
			ELSE ROUND(SUM(expected_distr_rental_cond_iptu_fee) OVER (
	    ORDER BY cash_flow_period_months ), 4)
	    END AS cum_distr_rental_cond_iptu_fee,
	    CASE
			WHEN cash_flow_period_months = 2 THEN cum_distr_fin_rev_fee_aux
			WHEN cash_flow_period_months = 30 THEN cum_distr_fin_rev_fee_aux
			ELSE ROUND(SUM(expected_distr_fin_rev_fee) OVER (
	    ORDER BY cash_flow_period_months ), 4)
	    END AS cum_distr_fin_rev_fee
	FROM expected_distr_rental_cond_iptu_fin_rev0;

	CREATE TABLE expected_distr_rental_cond_iptu_fin_rev1b (
	    contract_duration_perc INTEGER
	);

	WITH RECURSIVE sequence(num) AS (
	    SELECT 0
	    UNION ALL
	    SELECT num + 1
	    FROM sequence
	    WHERE num < 100
	)
	INSERT INTO expected_distr_rental_cond_iptu_fin_rev1b (contract_duration_perc)
	SELECT num
	FROM sequence;

	CREATE TABLE expected_distr_rental_cond_iptu_fin_rev2 AS
	SELECT
		b.contract_duration_perc,
		ROUND(a.expected_distr_rental_cond_iptu_fee, 4) AS expected_distr_rental_cond_iptu_fee,
		ROUND(a.expected_distr_fin_rev_fee, 4) AS expected_distr_fin_rev_fee
	FROM expected_distr_rental_cond_iptu_fin_rev1b as b
	LEFT JOIN expected_distr_rental_cond_iptu_fin_rev1a as a on b.contract_duration_perc = a.contract_duration_perc;

	UPDATE expected_distr_rental_cond_iptu_fin_rev2
	SET expected_distr_rental_cond_iptu_fee = 0
	WHERE expected_distr_rental_cond_iptu_fee IS NULL;

	UPDATE expected_distr_rental_cond_iptu_fin_rev2
	SET expected_distr_fin_rev_fee = 0
	WHERE expected_distr_fin_rev_fee IS NULL;



	CREATE TABLE expected_distr_rental_cond_iptu_fin_rev3 AS
	SELECT
		contract_duration_perc,
		expected_distr_rental_cond_iptu_fee,
		expected_distr_fin_rev_fee,
		SUM(expected_distr_rental_cond_iptu_fee) OVER (ORDER BY contract_duration_perc) AS cum_distr_rental_cond_iptu_fee,
		SUM(expected_distr_fin_rev_fee) OVER (ORDER BY contract_duration_perc) AS cum_distr_fin_rev_fee
	FROM expected_distr_rental_cond_iptu_fin_rev2;





	/*Calcula relação entre fees e despesas com transfer descobertos e receita financeira*/
	DROP VIEW IF EXISTS transfer_projection_result2;

	DROP VIEW IF EXISTS transfer_projection_result3;

	DROP VIEW IF EXISTS transfer_projection_result4;

	DROP VIEW IF EXISTS transfer_projection_result5;

	DROP VIEW IF EXISTS transfer_projection_result6;

	CREATE VIEW transfer_projection_result2 AS
	WITH params AS (
	    SELECT 12.25 AS discount_rate
	)
	SELECT
		*,
		CASE
			WHEN rent_fee_value = '' THEN ROUND(transfer_real_rental_value * (rent_fee / 100), 2)
			ELSE ROUND(rent_fee_value, 2)
		END AS rent_fee_value_adj,
		ROUND((SELECT expected_rental_cond_iptu_fee FROM expected_value_rental_cond_iptu_fin_rev), 4) AS expected_rental_cond_iptu_fee,
		ROUND((SELECT expected_fin_rev_fee FROM expected_value_rental_cond_iptu_fin_rev), 4) AS expected_fin_rev_fee,
		(ROUND(POWER((1 + ((SELECT discount_rate FROM params) / 100)), 0.08333333), 6) - 1) AS discount_rate,
		strftime('%Y-%m', transfer_due_date) AS transfer_due_month,
		((strftime('%Y', transfer_due_date) - strftime('%Y', contract_start_date)) * 12 + (strftime('%m', transfer_due_date) - strftime('%m', contract_start_date))) AS cash_flow_period_months
	FROM
	    transfer_projection_result;



	CREATE VIEW transfer_projection_result3 AS
	SELECT
		*,
		CASE
			WHEN transfer_due_date > '2024-12-31' THEN ROUND((POWER(1 + discount_rate, cash_flow_period_months - contract_current_duration)) - 1, 6)
			ELSE 0
		END AS period_discount_rate,
		SUM(transfer_real_rental_value) OVER (PARTITION BY contract_id) AS total_contract_real_rental_value,
		SUM(rent_fee_value_adj) OVER (PARTITION BY contract_id) AS total_rent_fee_value_adj,
		(100 * cash_flow_period_months / contract_original_duration) AS contract_duration_perc
	FROM
	    transfer_projection_result2;


	CREATE VIEW transfer_projection_result4 AS
	SELECT
		a.contract_id,
		a.contract_start_date,
		a.contract_end_date,
		a.contract_status,
		a.contract_original_duration,
		a.contract_current_duration,
		a.contract_duration_status,
		a.contract_readjustment_index,
		a.transfer_parcel_cycle,
		a.transfer_id,
		a.transfer_due_date,
		a.transfer_real_rental_value,
		a.median_rental_value,
		a.source,
		a.transfer_total_value,
		a.rent_fee,
		a.damage_fee,
		a.rent_fee_value,
		a.rent_fee_value_adj,
		a.expected_rental_cond_iptu_fee,
		a.expected_fin_rev_fee,
		a.transfer_due_month,
		a.cash_flow_period_months,
		a.total_contract_real_rental_value,
		a.total_rent_fee_value_adj,
		a.period_discount_rate,
		ROUND(total_rent_fee_value_adj * expected_rental_cond_iptu_fee, 2) AS expected_total_flow_rental_cond_iptu,
		ROUND(total_rent_fee_value_adj * expected_fin_rev_fee, 2) AS expected_total_flow_fin_rev,
		a.contract_duration_perc,
		CASE
			WHEN ROW_NUMBER() OVER (
	            PARTITION BY a.contract_id, b.cum_distr_rental_cond_iptu_fee
	            ORDER BY a.contract_duration_perc, a.transfer_due_date) > 1 THEN 0
	        ELSE ROUND(b.cum_distr_rental_cond_iptu_fee, 4)
	    END AS distr_rental_cond_iptu_fee_aux,
	    CASE
			WHEN ROW_NUMBER() OVER (
	            PARTITION BY a.contract_id, b.cum_distr_fin_rev_fee
	            ORDER BY a.contract_duration_perc, a.transfer_due_date) > 1 THEN 0
	        ELSE ROUND(b.cum_distr_fin_rev_fee, 4)
	    END AS distr_fin_rev_fee_aux
	/*	ROUND(b.cum_distr_rental_cond_iptu_fee, 4) AS cum_distr_rental_cond_iptu_fee,
		ROUND(b.cum_distr_fin_rev_fee, 4) AS cum_distr_fin_rev_fee*/
	/*	CASE
			WHEN ROW_NUMBER() OVER (
	            PARTITION BY a.contract_id, a.contract_duration_perc
	            ORDER BY a.contract_duration_perc) > 1 THEN 0
	        ELSE ROUND(b.cum_distr_rental_cond_iptu_fee, 4)
	    END AS cum_distr_rental_cond_iptu_fee,
	    CASE
			WHEN ROW_NUMBER() OVER (
	            PARTITION BY a.contract_id, a.contract_duration_perc
	            ORDER BY a.contract_duration_perc) > 1 THEN 0
	        ELSE ROUND(b.cum_distr_fin_rev_fee, 4)
	    END AS distr_flow_perc_fin_rev_fee*/
	FROM
	    transfer_projection_result3 as a
	LEFT JOIN expected_distr_rental_cond_iptu_fin_rev3 as b ON a.contract_duration_perc = b.contract_duration_perc;


	CREATE VIEW transfer_projection_result5 AS
	SELECT
		contract_id,
		contract_start_date,
		contract_end_date,
		contract_status,
		contract_original_duration,
		contract_current_duration,
		contract_duration_status,
		contract_readjustment_index,
		transfer_parcel_cycle,
		transfer_id,
		transfer_due_date,
		transfer_real_rental_value,
		median_rental_value,
		source,
		transfer_total_value,
		rent_fee,
		damage_fee,
		rent_fee_value,
		rent_fee_value_adj,
		expected_rental_cond_iptu_fee,
		expected_fin_rev_fee,
		transfer_due_month,
		cash_flow_period_months,
		total_contract_real_rental_value,
		total_rent_fee_value_adj,
		period_discount_rate,
		expected_total_flow_rental_cond_iptu,
		expected_total_flow_fin_rev,
		CASE
			WHEN distr_rental_cond_iptu_fee_aux = 0 THEN 0
			ELSE distr_rental_cond_iptu_fee_aux - (MAX(distr_rental_cond_iptu_fee_aux) OVER (
	    	PARTITION BY contract_id 
	        ORDER BY contract_duration_perc, transfer_due_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
	        ))
	     END AS distr_rental_cond_iptu_fee,
	     CASE
			WHEN distr_fin_rev_fee_aux = 0 THEN 0
			ELSE distr_fin_rev_fee_aux - (MAX(distr_fin_rev_fee_aux) OVER (
	    	PARTITION BY contract_id 
	        ORDER BY contract_duration_perc, transfer_due_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
	        ))
	     END AS distr_fin_rev_fee
	FROM transfer_projection_result4
	ORDER BY contract_id, contract_duration_perc, transfer_due_date;


	CREATE VIEW transfer_projection_result6 AS
	SELECT
		contract_id,
		contract_start_date,
		contract_end_date,
		transfer_due_date,
		transfer_due_month,
		contract_current_duration,
		cash_flow_period_months,
		transfer_real_rental_value,
		median_rental_value,
		rent_fee_value_adj,
		total_contract_real_rental_value,
		total_rent_fee_value_adj,
		expected_total_flow_rental_cond_iptu,
		expected_total_flow_fin_rev,
		ROUND((expected_total_flow_rental_cond_iptu * distr_rental_cond_iptu_fee), 2) AS nominal_predicted_flow_rental_cond_iptu,
		ROUND((expected_total_flow_rental_cond_iptu * distr_rental_cond_iptu_fee) / (1 + period_discount_rate), 2) AS discounted_predicted_flow_rental_cond_iptu,
		ROUND((expected_total_flow_fin_rev * distr_fin_rev_fee), 2) AS nominal_predicted_flow_fin_rev,
		ROUND((expected_total_flow_fin_rev * distr_fin_rev_fee) / (1 + period_discount_rate), 2) AS discounted_predicted_flow_fin_rev
	FROM transfer_projection_result5


	SELECT
		transfer_due_month,
		SUM(transfer_real_rental_value) AS transfer_real_rental_value,
		SUM(nominal_predicted_flow_rental_cond_iptu) AS nominal_predicted_flow_rental_cond_iptu,
		SUM(discounted_predicted_flow_rental_cond_iptu) AS discounted_predicted_flow_rental_cond_iptu,
		SUM(nominal_predicted_flow_fin_rev) AS nominal_predicted_flow_fin_rev,
		SUM(discounted_predicted_flow_fin_rev) AS discounted_predicted_flow_fin_rev
	FROM transfer_projection_result6	
	GROUP BY transfer_due_month;



	DROP VIEW IF EXISTS test1;

	CREATE VIEW test1 AS
	SELECT
		*
	FROM transfer_projection_result
	GROUP BY contract_id;


	CREATE VIEW test2 AS
	SELECT
		a.*,
		b.contract_duration_status
	FROM transfer_projection_result6 as a
	LEFT JOIN test1 as b on a.contract_id = b.contract_id

	SELECT
		transfer_due_month,
		contract_duration_status,
		COUNT(DISTINCT contract_id) AS contract_count
	FROM test2
	GROUP BY transfer_due_month, contract_duration_status;




	SELECT
		transfer_due_date,
		contract_duration_status,
		COUNT(DISTINCT contract_id) AS contract_count
	FROM transfer_projection_result
	WHERE contract_status = 'active'
	GROUP BY transfer_due_date, contract_duration_status;

	/*UPDATE transfer_projection_result4
	SET 
	    CASE
	        WHEN id_duration_repetitions > 1 THEN 0
	        ELSE distr_rental_cond_iptu_fee_aux
	    END AS distr_rental_cond_iptu_fee,
	    CASE
	        WHEN id_duration_repetitions > 1 THEN 0
	        ELSE distr_rental_fin_rev_fee_aux
	    END AS distr_rental_fin_rev_fee;
	*/
	/*WITH RankedData AS (
	    SELECT
	        contract_id_duration_key,
	        distr_rental_cond_iptu_fee_aux,
	        ROW_NUMBER() OVER (
	            PARTITION BY contract_id, contract_duration_perc
	            ORDER BY distr_rental_cond_iptu_fee_aux
	        ) AS rank
	    FROM transfer_projection_result4
	)*/
	/*UPDATE transfer_projection_result4
	SET distr_rental_cond_iptu_fee = CASE
	    WHEN (SELECT COUNT(*) 
	          FROM transfer_projection_result4 AS t2 
	          WHERE t2.contract_id_duration_key = transfer_projection_result4.contract_id_duration_key
	    ) = 1 THEN distr_rental_cond_iptu_fee_aux -- Unique values
	    WHEN (SELECT rank FROM RankedData AS r
	          WHERE r.contract_id = transfer_projection_result4.contract_id 
	          AND r.contract_id_duration_key = transfer_projection_result4.contract_id_duration_key
	    ) = 1 THEN distr_rental_cond_iptu_fee_aux -- First repeated value
	    ELSE 0 -- All other repeated values
	END;*/








	    

	CREATE VIEW transfer_projection_result3 AS
	SELECT
		*,
		(1 + period_discount_rate) AS discounted
	FROM
	    transfer_projection_result2;
	FROM
	    transfer_projection_result3;

	DROP VIEW IF EXISTS reserve_projection_rental_fin_by_contract;

	CREATE VIEW reserve_projection_rental_fin_by_contract AS
	SELECT
		*,
		AS predicted_nominal_cash_flow
		AS predicted_discounted_cash_flow
		(100 * cash_flow_period_months / contract_original_duration) AS contract_duration_perc
	FROM
	    transfer_projection_result3;





















	/* Análise Preliminar - Cria tabela para análise de momento de cancelamento de contratos com duração de 30 meses */
	/*
	SELECT
	    (FLOOR(vigencia_dias / 30) * 30) AS BinStart,
	    ((FLOOR(vigencia_dias / 30) + 1) * 30) AS BinEnd,
	    COUNT(*) AS Frequency
	FROM
	    duracao_vigencia_contratos
	GROUP BY
	    BinStart, BinEnd
	ORDER BY
	    BinStart;
	SELECT , COUNT(id) AS Frequency
	FROM duracao_vigencia_contratos
	GROUP BY vigencia_dias


	DROP VIEW IF EXISTS past_contract_cash_flow_damage;

	CREATE VIEW past_contract_cash_flow_damage AS
	SELECT
	    a.*,
	    b.damage_fee,
	    b.early_termination_penalty,
	    b.rent_fee_adj,
	    b.monthly_fee_reais,
	    c.median_rental_value,
	    CASE
		    WHEN b.damage_fee = 0 THEN 0
		    ELSE c.median_rental_value * 3
		END AS theoretical_damage_exposure,
	    CASE
		    WHEN b.early_termination_penalty = 'false' THEN 0
		    ELSE c.median_rental_value
		END AS theoretical_early_term_exposure/*,
		((CAST(strftime('%Y', end_month) AS INTEGER) - CAST(strftime('%Y', start_month) AS INTEGER)) * 12 + 
			(CAST(strftime('%m', end_month) AS INTEGER) - CAST(strftime('%m', start_month) AS INTEGER))) AS month_diff
	FROM
	    contract_cash_flow_4 AS a
	LEFT JOIN average_yearly_fee_7 AS b ON a.contract_id = b.contract_id AND a.contract_year = b.contract_year
	LEFT JOIN early_median_result_liq_date_no_parameters AS c ON a.contract_id = c.contract_id/* AND a.contract_year = c.transfer_parcel_cycle
	WHERE a.start_month <= '2021-12' AND a.contract_duration_adj = 30
	ORDER BY
	    contract_id, cash_flow_year_month;


	SELECT
	    SUM(ROUND(theoretical_damage_exposure, 2)) AS damage_exposure_flow,
	    SUM(ROUND(past_contract_cash_flow_damage, 2)) AS damage_flow
	FROM
	    past_contract_cash_flow_damage;




	CREATE VIEW past_contract_cash_flow_early_term AS
	SELECT
	    a.*,
	    b.damage_fee,
	    b.early_termination_penalty,
	    b.rent_fee_adj,
	    b.monthly_fee_reais,
	    c.median_rental_value,
	    CASE
		    WHEN b.damage_fee = 0 THEN 0
		    ELSE c.median_rental_value * 3
		END AS theoretical_damage_exposure,
	    CASE
		    WHEN b.early_termination_penalty = 'false' THEN 0
		    ELSE c.median_rental_value
		END AS theoretical_early_term_exposure,
		((CAST(strftime('%Y', end_month) AS INTEGER) - CAST(strftime('%Y', start_month) AS INTEGER)) * 12 + 
			(CAST(strftime('%m', end_month) AS INTEGER) - CAST(strftime('%m', start_month) AS INTEGER))) AS month_diff
	FROM
	    contract_cash_flow_4 AS a
	LEFT JOIN average_yearly_fee_7 AS b ON a.contract_id = b.contract_id AND a.contract_year = b.contract_year
	LEFT JOIN early_median_result_liq_date_no_parameters AS c ON a.contract_id = c.contract_id/* AND a.contract_year = c.transfer_parcel_cycle
	WHERE a.start_month <= '2023-12'
	ORDER BY
	    contract_id, cash_flow_year_month;



	SELECT
	    SUM(ROUND(theoretical_damage_exposure, 2)) AS damage_exposure_flow,
	    SUM(ROUND(past_contract_cash_flow_damage, 2)) AS damage_flow
	FROM
	    past_contract_cash_flow_damage;
```

#### Comentários Explicativos

* **Criação de Views “past_contract_cash_flow_*”**  
    Integram dados históricos (ou bases consolidadas) para observar valores de danos, fluxo de aluguel, taxas etc.
* **Distribuição no tempo e aplicação de taxas de desconto**
    * Em `transfer_projection_result2`, define-se a taxa de desconto anual (p. ex. 12,25%) e converte-se para um fator mensal aproximado.
    * Em `transfer_projection_result3`, aplicam-se esses fatores para calcular o “período de desconto”.
    * Em `transfer_projection_result4`, `transfer_projection_result5` e `transfer_projection_result6`, faz-se a **alocação** (ou “distribuição”) de cada parte do fluxo ao longo da vigência do contrato, e calcula-se o valor presente líquido do aluguel+condomínio+IPTU e da receita financeira projetada.
* **Consulta por safra, contagem de contratos e consolidação**  
    Ao final, há exemplos de consultas que somam valores por mês, contam quantos contratos seguem ativos etc., permitindo medir o total de fluxo nominal e descontado em cada período.

<br/>

# 💰 Conclusão e Encadeamento das Etapas

De forma abrangente:

1. **(Etapa 1) Análise Preliminar**
    
    * Gera-se o **histograma** de duração para corrigir inconsistências e entender a base de contratos.
2. **(Etapa 2) Fluxo de Caixa Passado**
    
    * Identificam-se, para cada contrato, os **valores efetivamente recebidos** (bills) e pagos (transfers), separando componentes garantidos, taxas, danos etc.
3. **(Etapa 3) Cálculo do Fee Teórico**
    
    * Reconstrói-se como o fee se comporta ao longo do tempo (1º ano, 2º ano, …), ajudando na **modelagem de receitas** e custo de risco.
4. **(Etapa 4) Tratamento de Dados (ETL)**
    
    * Com scripts Python, gera-se uma base confiável e padronizada, **limpando e filtrando** registros apenas dos contratos e parcelas relevantes, marcando mediana de aluguel etc.
5. **(Etapa 5) Criação dos Índices de Inflação**
    
    * Monta-se a **tabela mensal** de projeções inflacionárias (IGP-M, IPCA, INPC), essencial para os reajustes automáticos dos valores de aluguel e taxa.
6. **(Etapa 6) Projeção de Repasses Reajustados**
    
    * Utilizando os dados anteriores, **projeta-se** o fluxo futuro de cada contrato, adicionando parcelas inexistentes, aplicando reajustes de inflação.

7. **(Etapa 7) Aplicação da Análise**
    * Em seguida, com consultas SQL, faz-se a **distribuição dos fluxos no tempo**, aplica-se taxa de desconto e obtêm-se relatórios que mostram quanto se espera receber (ou ter de desembolsar) ao longo da vida dos contratos. Esses resultados servem de base para dimensionar o **risco de inadimplência**, estimar **provisões** para sinistros e calcular o montante de **reserva técnica** necessário.  

<br/>

Cada etapa, portanto, adiciona uma peça fundamental à construção do modelo de análise de risco e do fluxo financeiro projetado. O alto nível de detalhe no código — a despeito de ser denso — garante a rastreabilidade completa de como cada variável é formada, permitindo eventuais auditorias ou recalibrações de parâmetros (por exemplo, alterando a taxa de desconto, os índices inflacionários, datas de corte ou a forma de agrupar as parcelas em ciclos anuais, por exemplo).

Assim, esta documentação completa as **7 etapas** do trabalho, oferecendo uma visão passo a passo dos objetivos e resultados de cada bloco de instruções (tanto em SQL quanto em Python). O resultado final é uma **base unificada** de projeções financeiras (com histórico e futuro) pronta para que a empresa possa avaliar o comportamento de sua carteira, mensurar riscos e provisionar adequadamente eventuais sinistros ou inadimplências.