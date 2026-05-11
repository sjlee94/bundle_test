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
#/* Procedure Name     : PEC_TABLE_TXN_1           */
#/*                      테이블내역                */
#/**************************************************/

TARGET_CATALOG = "bidw_catalog"
TARGET_SCHEMA  = "bidw_schema"
TARGET_TABLE   = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.table_txn"

try:
    logging.info("=== Job Start ===")

    #--------------------------------------------
    #--0. 카탈로그, 스키마 생성 (권한 있을 때만)
    #--------------------------------------------
    # ⚠️ CREATE CATALOG 권한이 없으면 이 라인 주석 처리하고
    #    이미 존재하는 카탈로그를 TARGET_CATALOG에 지정하세요
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

    #--------------------------------------------
    #--1. 테이블 생성
    #--------------------------------------------
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id BIGINT,
        name STRING,
        created_at TIMESTAMP
    )
    """)
    logging.info(f"Table ready: {TARGET_TABLE}")

    #--------------------------------------------
    #--2. 데이터 적재
    #--------------------------------------------
    spark.sql(f"""
    INSERT INTO {TARGET_TABLE}
    SELECT
        monotonically_increasing_id() AS id,
        concat('table_', cast(rand()*100 as int)) AS name,
        current_timestamp() AS created_at
    FROM range(10)
    """)
    logging.info("10 rows inserted")

    # 결과 확인
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info(f"Total rows in table: {count}")

    logging.info("=== Job Success ===")

except Exception as e:
    logging.error("=== Job Failed ===")
    logging.error(str(e))
    logging.error(traceback.format_exc())
    sys.exit(1)

finally:
    logging.info("=== Job End ===")