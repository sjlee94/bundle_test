# -*- coding: utf-8 -*-

import logging
import sys
import traceback

from pyspark.sql import SparkSession

# Spark 세션 가져오기 (spark_python_task에서는 명시적으로 필요)
spark = SparkSession.builder.getOrCreate()

# 로깅 설정 (INFO 레벨 출력 활성화)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#/**************************************************/
#/* version 1.0                                    */
#/* Copyright 2021 <NAME>                          */
#/*                                                */
#/* Licensed under the {} License, Version 2.0     */
#/**************************************************/
#
#/**************************************************/
#/* Procedure Name     : PEC_TABLE_TXN2_1          */
#/*                      상품명 컬럼 추가          */
#/**************************************************/

TARGET_CATALOG = "bidw_catalog"
TARGET_SCHEMA  = "bidw_schema"
TARGET_TABLE   = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.table_txn"

try:
    logging.info("=== Job Start ===")

    #--------------------------------------------
    #--1. prod_name 컬럼 추가 (없을 때만)
    #--------------------------------------------
    # 컬럼 존재 여부 확인
    columns = [row["col_name"] for row in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]

    if "prod_name" not in columns:
        spark.sql(f"""
        ALTER TABLE {TARGET_TABLE}
        ADD COLUMN prod_name STRING
        """)
        logging.info("Column added: prod_name")
    else:
        logging.info("Column already exists: prod_name")

    #--------------------------------------------
    #--2. prod_name 값 임의 적재
    #--------------------------------------------
    # mobile, internet, iptv 중 하나를 id 기반으로 매핑
    spark.sql(f"""
    UPDATE {TARGET_TABLE}
    SET prod_name = CASE CAST(id % 3 AS INT)
        WHEN 0 THEN 'mobile'
        WHEN 1 THEN 'internet'
        WHEN 2 THEN 'iptv'
    END
    WHERE prod_name IS NULL
    """)
    logging.info("prod_name updated for NULL rows")

    #--------------------------------------------
    #--3. 결과 확인
    #--------------------------------------------
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info(f"Total rows in table: {count}")

    # prod_name 분포 출력
    logging.info("=== prod_name distribution ===")
    distribution = spark.sql(f"""
        SELECT prod_name, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY prod_name
        ORDER BY prod_name
    """).collect()
    for row in distribution:
        logging.info(f"  {row['prod_name']}: {row['cnt']}")

    logging.info("=== Job Success ===")

except Exception as e:
    logging.error("=== Job Failed ===")
    logging.error(str(e))
    logging.error(traceback.format_exc())
    sys.exit(1)

# 260701 주석 추가(테스트)
# 260703 주석 추가(테스트)
finally:
    logging.info("=== Job End ===")