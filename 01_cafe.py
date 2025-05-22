"""
# CAFE

Esta é uma DAG que faz o processo de carga do fluxo de DW da cafeteria.

"""

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import snowflake.connector as sc
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os

minha_conexao = "snowflake_conn"
meu_arquivo = "CAFE/coffee_shop_sales*"

diretorio_raiz = os.environ.get("AIRFLOW_HOME")

with DAG(
    dag_id="01_cafe",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 10, tz="UTC"),
    tags=["cafe", "dw"],
    doc_md=__doc__,
    description="esta é a descrição da DAG",
):

    # Obtém a conexão do banco de dados do Airflow
    def obter_credenciais_conexao(conn_id):
        conexao = BaseHook.get_connection(f"{conn_id}")

        if conexao:
            login = conexao.login
            senha = conexao.password
            conta = conexao.extra.split('"account": "')[1].split('"')[0]
            # regiao = conexao.extra.split('"region": "')[1].split('"')[0]
            regiao = ""
            return login, senha, conta, regiao
        else:
            print(f"A conexão com o ID '{conn_id}' não foi encontrada.")
            return None, None

    def envia_arquivo(arquivo):
        login, senha, conta, regiao = obter_credenciais_conexao(minha_conexao)
        conn = sc.connect(user=login, password=senha, account=f"{conta}")
        conn.cursor().execute(
            f"""
PUT file://dags/{arquivo} @IMPACTA.RAW.STG_RAW AUTO_COMPRESS=FALSE OVERWRITE = TRUE;
"""
        )
        return f"arquivo {arquivo} enviado para a STAGE @IMPACTA.RAW.STG_RAW"

    start = EmptyOperator(task_id="start")
    envia_arquivo_para_nuvem = PythonOperator(
        task_id="envia_arquivo_para_nuvem",
        python_callable=envia_arquivo,
        op_args=[f"{meu_arquivo}"],
        doc_md="Task que envia o arquivo csv para a nuvem do Snowflake",
    )
    # Executar a consulta usando SnowflakeOperator
    sql1 = """
CREATE or replace TABLE "IMPACTA"."RAW"."CAFE" ( transaction_id NUMBER(38, 0) , transaction_date DATE , transaction_time TIME , store_id NUMBER(38, 0) , store_location VARCHAR , product_id NUMBER(38, 0) , transaction_qty NUMBER(38, 0) , unit_price NUMBER(38, 1) , product_category VARCHAR , product_type VARCHAR , product_detail VARCHAR , Size VARCHAR , Total_bill NUMBER(38, 1) , Month_Name VARCHAR , Day_Name VARCHAR , Hour NUMBER(38, 0) , Day_of_Week NUMBER(38, 0) , Month NUMBER(38, 0) ); 

CREATE TEMP FILE FORMAT "IMPACTA"."RAW"."temp_file_format_cafe"
	TYPE=CSV
    SKIP_HEADER=1
    FIELD_DELIMITER=';'
    TRIM_SPACE=TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT='DD/MM/YYYY'
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO; 

COPY INTO "IMPACTA"."RAW"."CAFE" 
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
	FROM '@"IMPACTA"."RAW"."STG_RAW"') 
PATTERN = '.*coffee_shop_sales_.*csv'
FILE_FORMAT = '"IMPACTA"."RAW"."temp_file_format_cafe"' 
ON_ERROR=ABORT_STATEMENT 
;
"""
    # copy_file = SnowflakeOperator(
    #     task_id="copy_file",
    #     sql=sql1,
    #     snowflake_conn_id=minha_conexao,  # Nome da conexão configurada no Airflow para o Snowflake
    #     autocommit=True,
    #     split_statements=True,
    # )


    copy_file = SQLExecuteQueryOperator(
        task_id="copy_file",
        sql=sql1,
        conn_id=minha_conexao,  # Nome da conexão configurada no Airflow para o Snowflake
        autocommit=True
    )

### código inserido

    sql_dim_produto = """
MERGE INTO IMPACTA.CAFE.DIM_PRODUTO AS destino
USING (
    SELECT DISTINCT
        PRODUCT_ID,
        PRODUCT_CATEGORY,
        PRODUCT_TYPE,
        PRODUCT_DETAIL,
        SIZE
    FROM IMPACTA.RAW.CAFE
) AS origem
ON destino.PRODUCT_ID = origem.PRODUCT_ID
WHEN MATCHED THEN
    UPDATE SET
        destino.PRODUCT_CATEGORY = origem.PRODUCT_CATEGORY,
        destino.PRODUCT_TYPE = origem.PRODUCT_TYPE,
        destino.PRODUCT_DETAIL = origem.PRODUCT_DETAIL,
        destino.SIZE = origem.SIZE
WHEN NOT MATCHED THEN
    INSERT (PRODUCT_ID, PRODUCT_CATEGORY, PRODUCT_TYPE, PRODUCT_DETAIL, SIZE) 
    VALUES (origem.PRODUCT_ID, origem.PRODUCT_CATEGORY, origem.PRODUCT_TYPE, origem.PRODUCT_DETAIL, origem.SIZE);
"""

    # Task: carregar dimensão produto
    # merge_dim_produto = SnowflakeOperator(
    #     task_id="merge_dim_produto",
    #     sql=sql_dim_produto,
    #     snowflake_conn_id=minha_conexao,
    #     autocommit=True,
    #     split_statements=True,
    # )


    dim_produto = SQLExecuteQueryOperator(
        task_id="dim_produto",
        sql=sql_dim_produto,
        conn_id=minha_conexao,  # Nome da conexão configurada no Airflow para o Snowflake
        autocommit=True
    )


    sql_dim_loja = """
MERGE INTO IMPACTA.CAFE.DIM_LOJA AS destino
USING (
    SELECT DISTINCT
        STORE_ID,
        STORE_LOCATION
    FROM IMPACTA.RAW.CAFE
) AS origem
ON destino.STORE_ID = origem.STORE_ID
WHEN MATCHED THEN
    UPDATE SET
        destino.STORE_LOCATION = origem.STORE_LOCATION
WHEN NOT MATCHED THEN
    INSERT (STORE_ID, STORE_LOCATION) 
    VALUES (origem.STORE_ID, origem.STORE_LOCATION);
"""


    # Task: carregar dimensão loja
    # merge_dim_loja = SnowflakeOperator(
    #     task_id="merge_dim_loja",
    #     sql=sql_dim_loja,
    #     snowflake_conn_id=minha_conexao,
    #     autocommit=True,
    #     split_statements=True,
    # )

    dim_loja = SQLExecuteQueryOperator(
        task_id="dim_loja",
        sql=sql_dim_loja,
        conn_id=minha_conexao,  # Nome da conexão configurada no Airflow para o Snowflake
        autocommit=True
    )


    sql_fato_vendas = """
MERGE INTO IMPACTA.CAFE.FATO_VENDAS AS destino
USING (
    SELECT DISTINCT
        TRANSACTION_ID,
        TRANSACTION_DATE,
        PRODUCT_ID,
        STORE_ID,
        TRANSACTION_QTY,
        UNIT_PRICE,
        TOTAL_BILL,
        MONTH,
        MONTH_NAME,
        HOUR,
        DAY_NAME
    FROM IMPACTA.RAW.CAFE
) AS origem
ON destino.TRANSACTION_ID = origem.TRANSACTION_ID
WHEN MATCHED THEN
    UPDATE SET
        destino.TRANSACTION_DATE = origem.TRANSACTION_DATE,
        destino.PRODUCT_ID = origem.PRODUCT_ID,
        destino.STORE_ID = origem.STORE_ID,
        destino.TRANSACTION_QTY = origem.TRANSACTION_QTY,
        destino.UNIT_PRICE = origem.UNIT_PRICE,
        destino.TOTAL_BILL = origem.TOTAL_BILL,
        destino.MONTH = origem.MONTH,
        destino.MONTH_NAME = origem.MONTH_NAME,
        destino.HOUR = origem.HOUR,
        destino.DAY_NAME = origem.DAY_NAME
WHEN NOT MATCHED THEN
    INSERT (
        TRANSACTION_ID, TRANSACTION_DATE, PRODUCT_ID, STORE_ID,
        TRANSACTION_QTY, UNIT_PRICE, TOTAL_BILL, MONTH,
        MONTH_NAME, HOUR, DAY_NAME
    )
    VALUES (
        origem.TRANSACTION_ID, origem.TRANSACTION_DATE, origem.PRODUCT_ID, origem.STORE_ID,
        origem.TRANSACTION_QTY, origem.UNIT_PRICE, origem.TOTAL_BILL, origem.MONTH,
        origem.MONTH_NAME, origem.HOUR, origem.DAY_NAME
    );
"""


    # Task: carregar fato vendas
    # merge_fato_vendas = SnowflakeOperator(
    #     task_id="merge_fato_vendas",
    #     sql=sql_fato_vendas,
    #     snowflake_conn_id=minha_conexao,
    #     autocommit=True,
    #     split_statements=True,
    # )


    fato_vendas = SQLExecuteQueryOperator(
        task_id="fato_vendas",
        sql=sql_fato_vendas,
        conn_id=minha_conexao,  # Nome da conexão configurada no Airflow para o Snowflake
        autocommit=True
    )

### código inserido

    sql_remove = """
REMOVE '@"IMPACTA"."RAW"."STG_RAW"/' PATTERN = '.*coffee_shop_sales_.*csv'
"""
    # cafe_remove_arquivo = SnowflakeOperator(
    #     task_id="cafe_remove_arquivo",
    #     sql=sql_remove,
    #     snowflake_conn_id=minha_conexao,
    #     autocommit=True,
    #     split_statements=True,
    # )

    cafe_remove_arquivo = SQLExecuteQueryOperator(
        task_id="cafe_remove_arquivo",
        sql=sql_remove,
        conn_id=minha_conexao,  # Nome da conexão configurada no Airflow para o Snowflake
        autocommit=True
    )


    cafe_move_arquivo_processado = BashOperator(
        task_id="cafe_move_arquivo_processado",
        bash_command=f"mv {diretorio_raiz}/dags/CAFE/coffee_shop_sales_*.csv {diretorio_raiz}/dags/CAFE/processado/",
    )
    end = EmptyOperator(task_id="end")


(
    start
    >> envia_arquivo_para_nuvem
    >> copy_file
    >> (dim_produto, dim_loja)
    >> fato_vendas
    >> (cafe_remove_arquivo, cafe_move_arquivo_processado)
    >> end
)
