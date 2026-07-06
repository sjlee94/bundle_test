# -*- coding: utf-8 -*-
import logging,sys,traceback
from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

TARGET_CATALOG="bidw_catalog"
TARGET_SCHEMA="bidw_schema"
TARGET_TABLE=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.table_txn_164"

try:
    logging.info("=== Job Start ===")
PROCEDURE_NAME="PEC_TABLE_TXN2_164"


    # STEP 001
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_001" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_001 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_001='VALUE_001' WHERE col_001 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 001 rows=%s",cnt)

    # STEP 002
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_002" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_002 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_002='VALUE_002' WHERE col_002 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 002 rows=%s",cnt)

    # STEP 003
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_003" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_003 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_003='VALUE_003' WHERE col_003 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 003 rows=%s",cnt)

    # STEP 004
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_004" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_004 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_004='VALUE_004' WHERE col_004 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 004 rows=%s",cnt)

    # STEP 005
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_005" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_005 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_005='VALUE_005' WHERE col_005 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 005 rows=%s",cnt)

    # STEP 006
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_006" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_006 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_006='VALUE_006' WHERE col_006 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 006 rows=%s",cnt)

    # STEP 007
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_007" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_007 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_007='VALUE_007' WHERE col_007 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 007 rows=%s",cnt)

    # STEP 008
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_008" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_008 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_008='VALUE_008' WHERE col_008 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 008 rows=%s",cnt)

    # STEP 009
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_009" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_009 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_009='VALUE_009' WHERE col_009 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 009 rows=%s",cnt)

    # STEP 010
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_010" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_010 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_010='VALUE_010' WHERE col_010 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 010 rows=%s",cnt)

    # STEP 011
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_011" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_011 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_011='VALUE_011' WHERE col_011 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 011 rows=%s",cnt)

    # STEP 012
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_012" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_012 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_012='VALUE_012' WHERE col_012 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 012 rows=%s",cnt)

    # STEP 013
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_013" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_013 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_013='VALUE_013' WHERE col_013 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 013 rows=%s",cnt)

    # STEP 014
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_014" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_014 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_014='VALUE_014' WHERE col_014 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 014 rows=%s",cnt)

    # STEP 015
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_015" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_015 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_015='VALUE_015' WHERE col_015 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 015 rows=%s",cnt)

    # STEP 016
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_016" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_016 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_016='VALUE_016' WHERE col_016 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 016 rows=%s",cnt)

    # STEP 017
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_017" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_017 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_017='VALUE_017' WHERE col_017 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 017 rows=%s",cnt)

    # STEP 018
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_018" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_018 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_018='VALUE_018' WHERE col_018 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 018 rows=%s",cnt)

    # STEP 019
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_019" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_019 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_019='VALUE_019' WHERE col_019 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 019 rows=%s",cnt)

    # STEP 020
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_020" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_020 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_020='VALUE_020' WHERE col_020 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 020 rows=%s",cnt)

    # STEP 021
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_021" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_021 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_021='VALUE_021' WHERE col_021 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 021 rows=%s",cnt)

    # STEP 022
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_022" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_022 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_022='VALUE_022' WHERE col_022 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 022 rows=%s",cnt)

    # STEP 023
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_023" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_023 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_023='VALUE_023' WHERE col_023 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 023 rows=%s",cnt)

    # STEP 024
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_024" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_024 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_024='VALUE_024' WHERE col_024 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 024 rows=%s",cnt)

    # STEP 025
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_025" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_025 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_025='VALUE_025' WHERE col_025 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 025 rows=%s",cnt)

    # STEP 026
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_026" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_026 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_026='VALUE_026' WHERE col_026 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 026 rows=%s",cnt)

    # STEP 027
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_027" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_027 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_027='VALUE_027' WHERE col_027 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 027 rows=%s",cnt)

    # STEP 028
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_028" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_028 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_028='VALUE_028' WHERE col_028 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 028 rows=%s",cnt)

    # STEP 029
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_029" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_029 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_029='VALUE_029' WHERE col_029 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 029 rows=%s",cnt)

    # STEP 030
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_030" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_030 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_030='VALUE_030' WHERE col_030 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 030 rows=%s",cnt)

    # STEP 031
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_031" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_031 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_031='VALUE_031' WHERE col_031 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 031 rows=%s",cnt)

    # STEP 032
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_032" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_032 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_032='VALUE_032' WHERE col_032 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 032 rows=%s",cnt)

    # STEP 033
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_033" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_033 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_033='VALUE_033' WHERE col_033 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 033 rows=%s",cnt)

    # STEP 034
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_034" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_034 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_034='VALUE_034' WHERE col_034 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 034 rows=%s",cnt)

    # STEP 035
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_035" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_035 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_035='VALUE_035' WHERE col_035 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 035 rows=%s",cnt)

    # STEP 036
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_036" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_036 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_036='VALUE_036' WHERE col_036 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 036 rows=%s",cnt)

    # STEP 037
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_037" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_037 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_037='VALUE_037' WHERE col_037 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 037 rows=%s",cnt)

    # STEP 038
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_038" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_038 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_038='VALUE_038' WHERE col_038 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 038 rows=%s",cnt)

    # STEP 039
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_039" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_039 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_039='VALUE_039' WHERE col_039 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 039 rows=%s",cnt)

    # STEP 040
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_040" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_040 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_040='VALUE_040' WHERE col_040 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 040 rows=%s",cnt)

    # STEP 041
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_041" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_041 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_041='VALUE_041' WHERE col_041 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 041 rows=%s",cnt)

    # STEP 042
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_042" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_042 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_042='VALUE_042' WHERE col_042 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 042 rows=%s",cnt)

    # STEP 043
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_043" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_043 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_043='VALUE_043' WHERE col_043 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 043 rows=%s",cnt)

    # STEP 044
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_044" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_044 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_044='VALUE_044' WHERE col_044 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 044 rows=%s",cnt)

    # STEP 045
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_045" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_045 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_045='VALUE_045' WHERE col_045 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 045 rows=%s",cnt)

    # STEP 046
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_046" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_046 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_046='VALUE_046' WHERE col_046 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 046 rows=%s",cnt)

    # STEP 047
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_047" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_047 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_047='VALUE_047' WHERE col_047 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 047 rows=%s",cnt)

    # STEP 048
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_048" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_048 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_048='VALUE_048' WHERE col_048 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 048 rows=%s",cnt)

    # STEP 049
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_049" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_049 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_049='VALUE_049' WHERE col_049 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 049 rows=%s",cnt)

    # STEP 050
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_050" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_050 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_050='VALUE_050' WHERE col_050 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 050 rows=%s",cnt)

    # STEP 051
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_051" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_051 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_051='VALUE_051' WHERE col_051 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 051 rows=%s",cnt)

    # STEP 052
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_052" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_052 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_052='VALUE_052' WHERE col_052 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 052 rows=%s",cnt)

    # STEP 053
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_053" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_053 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_053='VALUE_053' WHERE col_053 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 053 rows=%s",cnt)

    # STEP 054
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_054" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_054 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_054='VALUE_054' WHERE col_054 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 054 rows=%s",cnt)

    # STEP 055
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_055" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_055 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_055='VALUE_055' WHERE col_055 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 055 rows=%s",cnt)

    # STEP 056
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_056" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_056 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_056='VALUE_056' WHERE col_056 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 056 rows=%s",cnt)

    # STEP 057
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_057" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_057 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_057='VALUE_057' WHERE col_057 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 057 rows=%s",cnt)

    # STEP 058
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_058" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_058 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_058='VALUE_058' WHERE col_058 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 058 rows=%s",cnt)

    # STEP 059
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_059" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_059 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_059='VALUE_059' WHERE col_059 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 059 rows=%s",cnt)

    # STEP 060
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_060" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_060 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_060='VALUE_060' WHERE col_060 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 060 rows=%s",cnt)

    # STEP 061
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_061" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_061 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_061='VALUE_061' WHERE col_061 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 061 rows=%s",cnt)

    # STEP 062
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_062" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_062 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_062='VALUE_062' WHERE col_062 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 062 rows=%s",cnt)

    # STEP 063
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_063" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_063 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_063='VALUE_063' WHERE col_063 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 063 rows=%s",cnt)

    # STEP 064
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_064" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_064 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_064='VALUE_064' WHERE col_064 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 064 rows=%s",cnt)

    # STEP 065
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_065" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_065 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_065='VALUE_065' WHERE col_065 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 065 rows=%s",cnt)

    # STEP 066
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_066" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_066 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_066='VALUE_066' WHERE col_066 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 066 rows=%s",cnt)

    # STEP 067
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_067" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_067 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_067='VALUE_067' WHERE col_067 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 067 rows=%s",cnt)

    # STEP 068
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_068" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_068 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_068='VALUE_068' WHERE col_068 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 068 rows=%s",cnt)

    # STEP 069
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_069" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_069 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_069='VALUE_069' WHERE col_069 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 069 rows=%s",cnt)

    # STEP 070
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_070" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_070 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_070='VALUE_070' WHERE col_070 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 070 rows=%s",cnt)

    # STEP 071
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_071" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_071 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_071='VALUE_071' WHERE col_071 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 071 rows=%s",cnt)

    # STEP 072
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_072" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_072 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_072='VALUE_072' WHERE col_072 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 072 rows=%s",cnt)

    # STEP 073
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_073" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_073 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_073='VALUE_073' WHERE col_073 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 073 rows=%s",cnt)

    # STEP 074
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_074" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_074 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_074='VALUE_074' WHERE col_074 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 074 rows=%s",cnt)

    # STEP 075
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_075" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_075 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_075='VALUE_075' WHERE col_075 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 075 rows=%s",cnt)

    # STEP 076
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_076" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_076 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_076='VALUE_076' WHERE col_076 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 076 rows=%s",cnt)

    # STEP 077
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_077" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_077 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_077='VALUE_077' WHERE col_077 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 077 rows=%s",cnt)

    # STEP 078
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_078" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_078 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_078='VALUE_078' WHERE col_078 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 078 rows=%s",cnt)

    # STEP 079
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_079" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_079 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_079='VALUE_079' WHERE col_079 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 079 rows=%s",cnt)

    # STEP 080
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_080" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_080 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_080='VALUE_080' WHERE col_080 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 080 rows=%s",cnt)

    # STEP 081
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_081" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_081 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_081='VALUE_081' WHERE col_081 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 081 rows=%s",cnt)

    # STEP 082
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_082" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_082 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_082='VALUE_082' WHERE col_082 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 082 rows=%s",cnt)

    # STEP 083
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_083" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_083 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_083='VALUE_083' WHERE col_083 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 083 rows=%s",cnt)

    # STEP 084
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_084" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_084 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_084='VALUE_084' WHERE col_084 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 084 rows=%s",cnt)

    # STEP 085
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_085" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_085 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_085='VALUE_085' WHERE col_085 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 085 rows=%s",cnt)

    # STEP 086
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_086" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_086 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_086='VALUE_086' WHERE col_086 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 086 rows=%s",cnt)

    # STEP 087
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_087" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_087 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_087='VALUE_087' WHERE col_087 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 087 rows=%s",cnt)

    # STEP 088
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_088" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_088 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_088='VALUE_088' WHERE col_088 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 088 rows=%s",cnt)

    # STEP 089
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_089" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_089 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_089='VALUE_089' WHERE col_089 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 089 rows=%s",cnt)

    # STEP 090
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_090" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_090 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_090='VALUE_090' WHERE col_090 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 090 rows=%s",cnt)

    # STEP 091
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_091" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_091 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_091='VALUE_091' WHERE col_091 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 091 rows=%s",cnt)

    # STEP 092
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_092" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_092 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_092='VALUE_092' WHERE col_092 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 092 rows=%s",cnt)

    # STEP 093
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_093" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_093 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_093='VALUE_093' WHERE col_093 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 093 rows=%s",cnt)

    # STEP 094
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_094" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_094 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_094='VALUE_094' WHERE col_094 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 094 rows=%s",cnt)

    # STEP 095
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_095" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_095 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_095='VALUE_095' WHERE col_095 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 095 rows=%s",cnt)

    # STEP 096
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_096" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_096 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_096='VALUE_096' WHERE col_096 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 096 rows=%s",cnt)

    # STEP 097
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_097" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_097 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_097='VALUE_097' WHERE col_097 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 097 rows=%s",cnt)

    # STEP 098
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_098" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_098 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_098='VALUE_098' WHERE col_098 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 098 rows=%s",cnt)

    # STEP 099
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_099" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_099 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_099='VALUE_099' WHERE col_099 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 099 rows=%s",cnt)

    # STEP 100
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_100" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_100 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_100='VALUE_100' WHERE col_100 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 100 rows=%s",cnt)

    # STEP 101
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_101" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_101 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_101='VALUE_101' WHERE col_101 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 101 rows=%s",cnt)

    # STEP 102
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_102" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_102 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_102='VALUE_102' WHERE col_102 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 102 rows=%s",cnt)

    # STEP 103
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_103" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_103 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_103='VALUE_103' WHERE col_103 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 103 rows=%s",cnt)

    # STEP 104
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_104" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_104 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_104='VALUE_104' WHERE col_104 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 104 rows=%s",cnt)

    # STEP 105
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_105" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_105 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_105='VALUE_105' WHERE col_105 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 105 rows=%s",cnt)

    # STEP 106
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_106" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_106 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_106='VALUE_106' WHERE col_106 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 106 rows=%s",cnt)

    # STEP 107
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_107" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_107 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_107='VALUE_107' WHERE col_107 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 107 rows=%s",cnt)

    # STEP 108
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_108" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_108 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_108='VALUE_108' WHERE col_108 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 108 rows=%s",cnt)

    # STEP 109
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_109" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_109 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_109='VALUE_109' WHERE col_109 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 109 rows=%s",cnt)

    # STEP 110
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_110" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_110 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_110='VALUE_110' WHERE col_110 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 110 rows=%s",cnt)

    # STEP 111
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_111" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_111 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_111='VALUE_111' WHERE col_111 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 111 rows=%s",cnt)

    # STEP 112
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_112" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_112 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_112='VALUE_112' WHERE col_112 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 112 rows=%s",cnt)

    # STEP 113
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_113" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_113 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_113='VALUE_113' WHERE col_113 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 113 rows=%s",cnt)

    # STEP 114
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_114" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_114 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_114='VALUE_114' WHERE col_114 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 114 rows=%s",cnt)

    # STEP 115
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_115" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_115 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_115='VALUE_115' WHERE col_115 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 115 rows=%s",cnt)

    # STEP 116
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_116" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_116 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_116='VALUE_116' WHERE col_116 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 116 rows=%s",cnt)

    # STEP 117
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_117" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_117 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_117='VALUE_117' WHERE col_117 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 117 rows=%s",cnt)

    # STEP 118
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_118" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_118 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_118='VALUE_118' WHERE col_118 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 118 rows=%s",cnt)

    # STEP 119
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_119" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_119 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_119='VALUE_119' WHERE col_119 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 119 rows=%s",cnt)

    # STEP 120
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_120" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_120 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_120='VALUE_120' WHERE col_120 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 120 rows=%s",cnt)

    # STEP 121
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_121" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_121 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_121='VALUE_121' WHERE col_121 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 121 rows=%s",cnt)

    # STEP 122
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_122" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_122 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_122='VALUE_122' WHERE col_122 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 122 rows=%s",cnt)

    # STEP 123
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_123" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_123 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_123='VALUE_123' WHERE col_123 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 123 rows=%s",cnt)

    # STEP 124
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_124" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_124 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_124='VALUE_124' WHERE col_124 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 124 rows=%s",cnt)

    # STEP 125
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_125" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_125 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_125='VALUE_125' WHERE col_125 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 125 rows=%s",cnt)

    # STEP 126
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_126" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_126 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_126='VALUE_126' WHERE col_126 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 126 rows=%s",cnt)

    # STEP 127
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_127" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_127 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_127='VALUE_127' WHERE col_127 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 127 rows=%s",cnt)

    # STEP 128
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_128" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_128 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_128='VALUE_128' WHERE col_128 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 128 rows=%s",cnt)

    # STEP 129
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_129" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_129 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_129='VALUE_129' WHERE col_129 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 129 rows=%s",cnt)

    # STEP 130
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_130" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_130 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_130='VALUE_130' WHERE col_130 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 130 rows=%s",cnt)

    # STEP 131
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_131" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_131 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_131='VALUE_131' WHERE col_131 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 131 rows=%s",cnt)

    # STEP 132
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_132" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_132 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_132='VALUE_132' WHERE col_132 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 132 rows=%s",cnt)

    # STEP 133
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_133" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_133 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_133='VALUE_133' WHERE col_133 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 133 rows=%s",cnt)

    # STEP 134
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_134" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_134 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_134='VALUE_134' WHERE col_134 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 134 rows=%s",cnt)

    # STEP 135
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_135" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_135 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_135='VALUE_135' WHERE col_135 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 135 rows=%s",cnt)

    # STEP 136
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_136" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_136 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_136='VALUE_136' WHERE col_136 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 136 rows=%s",cnt)

    # STEP 137
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_137" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_137 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_137='VALUE_137' WHERE col_137 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 137 rows=%s",cnt)

    # STEP 138
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_138" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_138 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_138='VALUE_138' WHERE col_138 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 138 rows=%s",cnt)

    # STEP 139
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_139" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_139 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_139='VALUE_139' WHERE col_139 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 139 rows=%s",cnt)

    # STEP 140
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_140" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_140 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_140='VALUE_140' WHERE col_140 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 140 rows=%s",cnt)

    # STEP 141
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_141" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_141 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_141='VALUE_141' WHERE col_141 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 141 rows=%s",cnt)

    # STEP 142
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_142" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_142 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_142='VALUE_142' WHERE col_142 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 142 rows=%s",cnt)

    # STEP 143
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_143" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_143 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_143='VALUE_143' WHERE col_143 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 143 rows=%s",cnt)

    # STEP 144
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_144" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_144 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_144='VALUE_144' WHERE col_144 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 144 rows=%s",cnt)

    # STEP 145
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_145" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_145 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_145='VALUE_145' WHERE col_145 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 145 rows=%s",cnt)

    # STEP 146
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_146" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_146 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_146='VALUE_146' WHERE col_146 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 146 rows=%s",cnt)

    # STEP 147
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_147" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_147 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_147='VALUE_147' WHERE col_147 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 147 rows=%s",cnt)

    # STEP 148
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_148" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_148 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_148='VALUE_148' WHERE col_148 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 148 rows=%s",cnt)

    # STEP 149
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_149" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_149 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_149='VALUE_149' WHERE col_149 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 149 rows=%s",cnt)

    # STEP 150
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_150" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_150 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_150='VALUE_150' WHERE col_150 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 150 rows=%s",cnt)

    # STEP 151
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_151" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_151 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_151='VALUE_151' WHERE col_151 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 151 rows=%s",cnt)

    # STEP 152
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_152" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_152 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_152='VALUE_152' WHERE col_152 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 152 rows=%s",cnt)

    # STEP 153
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_153" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_153 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_153='VALUE_153' WHERE col_153 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 153 rows=%s",cnt)

    # STEP 154
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_154" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_154 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_154='VALUE_154' WHERE col_154 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 154 rows=%s",cnt)

    # STEP 155
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_155" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_155 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_155='VALUE_155' WHERE col_155 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 155 rows=%s",cnt)

    # STEP 156
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_156" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_156 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_156='VALUE_156' WHERE col_156 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 156 rows=%s",cnt)

    # STEP 157
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_157" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_157 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_157='VALUE_157' WHERE col_157 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 157 rows=%s",cnt)

    # STEP 158
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_158" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_158 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_158='VALUE_158' WHERE col_158 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 158 rows=%s",cnt)

    # STEP 159
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_159" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_159 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_159='VALUE_159' WHERE col_159 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 159 rows=%s",cnt)

    # STEP 160
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_160" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_160 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_160='VALUE_160' WHERE col_160 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 160 rows=%s",cnt)

    # STEP 161
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_161" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_161 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_161='VALUE_161' WHERE col_161 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 161 rows=%s",cnt)

    # STEP 162
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_162" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_162 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_162='VALUE_162' WHERE col_162 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 162 rows=%s",cnt)

    # STEP 163
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_163" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_163 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_163='VALUE_163' WHERE col_163 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 163 rows=%s",cnt)

    # STEP 164
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_164" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_164 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_164='VALUE_164' WHERE col_164 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 164 rows=%s",cnt)

    # STEP 165
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_165" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_165 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_165='VALUE_165' WHERE col_165 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 165 rows=%s",cnt)

    # STEP 166
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_166" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_166 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_166='VALUE_166' WHERE col_166 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 166 rows=%s",cnt)

    # STEP 167
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_167" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_167 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_167='VALUE_167' WHERE col_167 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 167 rows=%s",cnt)

    # STEP 168
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_168" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_168 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_168='VALUE_168' WHERE col_168 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 168 rows=%s",cnt)

    # STEP 169
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_169" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_169 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_169='VALUE_169' WHERE col_169 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 169 rows=%s",cnt)

    # STEP 170
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_170" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_170 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_170='VALUE_170' WHERE col_170 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 170 rows=%s",cnt)

    # STEP 171
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_171" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_171 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_171='VALUE_171' WHERE col_171 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 171 rows=%s",cnt)

    # STEP 172
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_172" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_172 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_172='VALUE_172' WHERE col_172 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 172 rows=%s",cnt)

    # STEP 173
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_173" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_173 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_173='VALUE_173' WHERE col_173 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 173 rows=%s",cnt)

    # STEP 174
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_174" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_174 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_174='VALUE_174' WHERE col_174 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 174 rows=%s",cnt)

    # STEP 175
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_175" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_175 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_175='VALUE_175' WHERE col_175 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 175 rows=%s",cnt)

    # STEP 176
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_176" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_176 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_176='VALUE_176' WHERE col_176 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 176 rows=%s",cnt)

    # STEP 177
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_177" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_177 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_177='VALUE_177' WHERE col_177 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 177 rows=%s",cnt)

    # STEP 178
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_178" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_178 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_178='VALUE_178' WHERE col_178 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 178 rows=%s",cnt)

    # STEP 179
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_179" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_179 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_179='VALUE_179' WHERE col_179 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 179 rows=%s",cnt)

    # STEP 180
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_180" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_180 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_180='VALUE_180' WHERE col_180 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 180 rows=%s",cnt)

    # STEP 181
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_181" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_181 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_181='VALUE_181' WHERE col_181 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 181 rows=%s",cnt)

    # STEP 182
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_182" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_182 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_182='VALUE_182' WHERE col_182 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 182 rows=%s",cnt)

    # STEP 183
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_183" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_183 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_183='VALUE_183' WHERE col_183 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 183 rows=%s",cnt)

    # STEP 184
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_184" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_184 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_184='VALUE_184' WHERE col_184 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 184 rows=%s",cnt)

    # STEP 185
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_185" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_185 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_185='VALUE_185' WHERE col_185 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 185 rows=%s",cnt)

    # STEP 186
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_186" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_186 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_186='VALUE_186' WHERE col_186 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 186 rows=%s",cnt)

    # STEP 187
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_187" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_187 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_187='VALUE_187' WHERE col_187 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 187 rows=%s",cnt)

    # STEP 188
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_188" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_188 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_188='VALUE_188' WHERE col_188 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 188 rows=%s",cnt)

    # STEP 189
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_189" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_189 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_189='VALUE_189' WHERE col_189 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 189 rows=%s",cnt)

    # STEP 190
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_190" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_190 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_190='VALUE_190' WHERE col_190 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 190 rows=%s",cnt)

    # STEP 191
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_191" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_191 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_191='VALUE_191' WHERE col_191 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 191 rows=%s",cnt)

    # STEP 192
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_192" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_192 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_192='VALUE_192' WHERE col_192 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 192 rows=%s",cnt)

    # STEP 193
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_193" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_193 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_193='VALUE_193' WHERE col_193 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 193 rows=%s",cnt)

    # STEP 194
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_194" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_194 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_194='VALUE_194' WHERE col_194 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 194 rows=%s",cnt)

    # STEP 195
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_195" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_195 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_195='VALUE_195' WHERE col_195 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 195 rows=%s",cnt)

    # STEP 196
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_196" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_196 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_196='VALUE_196' WHERE col_196 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 196 rows=%s",cnt)

    # STEP 197
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_197" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_197 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_197='VALUE_197' WHERE col_197 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 197 rows=%s",cnt)

    # STEP 198
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_198" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_198 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_198='VALUE_198' WHERE col_198 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 198 rows=%s",cnt)

    # STEP 199
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_199" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_199 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_199='VALUE_199' WHERE col_199 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 199 rows=%s",cnt)

    # STEP 200
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_200" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_200 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_200='VALUE_200' WHERE col_200 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 200 rows=%s",cnt)

    # STEP 201
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_201" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_201 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_201='VALUE_201' WHERE col_201 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 201 rows=%s",cnt)

    # STEP 202
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_202" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_202 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_202='VALUE_202' WHERE col_202 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 202 rows=%s",cnt)

    # STEP 203
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_203" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_203 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_203='VALUE_203' WHERE col_203 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 203 rows=%s",cnt)

    # STEP 204
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_204" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_204 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_204='VALUE_204' WHERE col_204 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 204 rows=%s",cnt)

    # STEP 205
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_205" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_205 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_205='VALUE_205' WHERE col_205 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 205 rows=%s",cnt)

    # STEP 206
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_206" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_206 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_206='VALUE_206' WHERE col_206 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 206 rows=%s",cnt)

    # STEP 207
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_207" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_207 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_207='VALUE_207' WHERE col_207 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 207 rows=%s",cnt)

    # STEP 208
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_208" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_208 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_208='VALUE_208' WHERE col_208 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 208 rows=%s",cnt)

    # STEP 209
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_209" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_209 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_209='VALUE_209' WHERE col_209 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 209 rows=%s",cnt)

    # STEP 210
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_210" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_210 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_210='VALUE_210' WHERE col_210 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 210 rows=%s",cnt)

    # STEP 211
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_211" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_211 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_211='VALUE_211' WHERE col_211 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 211 rows=%s",cnt)

    # STEP 212
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_212" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_212 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_212='VALUE_212' WHERE col_212 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 212 rows=%s",cnt)

    # STEP 213
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_213" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_213 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_213='VALUE_213' WHERE col_213 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 213 rows=%s",cnt)

    # STEP 214
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_214" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_214 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_214='VALUE_214' WHERE col_214 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 214 rows=%s",cnt)

    # STEP 215
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_215" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_215 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_215='VALUE_215' WHERE col_215 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 215 rows=%s",cnt)

    # STEP 216
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_216" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_216 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_216='VALUE_216' WHERE col_216 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 216 rows=%s",cnt)

    # STEP 217
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_217" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_217 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_217='VALUE_217' WHERE col_217 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 217 rows=%s",cnt)

    # STEP 218
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_218" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_218 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_218='VALUE_218' WHERE col_218 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 218 rows=%s",cnt)

    # STEP 219
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_219" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_219 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_219='VALUE_219' WHERE col_219 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 219 rows=%s",cnt)

    # STEP 220
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_220" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_220 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_220='VALUE_220' WHERE col_220 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 220 rows=%s",cnt)

    # STEP 221
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_221" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_221 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_221='VALUE_221' WHERE col_221 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 221 rows=%s",cnt)

    # STEP 222
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_222" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_222 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_222='VALUE_222' WHERE col_222 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 222 rows=%s",cnt)

    # STEP 223
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_223" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_223 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_223='VALUE_223' WHERE col_223 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 223 rows=%s",cnt)

    # STEP 224
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_224" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_224 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_224='VALUE_224' WHERE col_224 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 224 rows=%s",cnt)

    # STEP 225
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_225" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_225 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_225='VALUE_225' WHERE col_225 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 225 rows=%s",cnt)

    # STEP 226
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_226" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_226 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_226='VALUE_226' WHERE col_226 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 226 rows=%s",cnt)

    # STEP 227
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_227" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_227 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_227='VALUE_227' WHERE col_227 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 227 rows=%s",cnt)

    # STEP 228
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_228" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_228 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_228='VALUE_228' WHERE col_228 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 228 rows=%s",cnt)

    # STEP 229
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_229" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_229 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_229='VALUE_229' WHERE col_229 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 229 rows=%s",cnt)

    # STEP 230
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_230" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_230 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_230='VALUE_230' WHERE col_230 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 230 rows=%s",cnt)

    # STEP 231
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_231" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_231 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_231='VALUE_231' WHERE col_231 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 231 rows=%s",cnt)

    # STEP 232
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_232" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_232 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_232='VALUE_232' WHERE col_232 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 232 rows=%s",cnt)

    # STEP 233
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_233" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_233 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_233='VALUE_233' WHERE col_233 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 233 rows=%s",cnt)

    # STEP 234
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_234" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_234 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_234='VALUE_234' WHERE col_234 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 234 rows=%s",cnt)

    # STEP 235
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_235" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_235 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_235='VALUE_235' WHERE col_235 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 235 rows=%s",cnt)

    # STEP 236
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_236" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_236 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_236='VALUE_236' WHERE col_236 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 236 rows=%s",cnt)

    # STEP 237
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_237" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_237 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_237='VALUE_237' WHERE col_237 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 237 rows=%s",cnt)

    # STEP 238
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_238" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_238 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_238='VALUE_238' WHERE col_238 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 238 rows=%s",cnt)

    # STEP 239
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_239" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_239 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_239='VALUE_239' WHERE col_239 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 239 rows=%s",cnt)

    # STEP 240
    cols=[r["col_name"] for r in spark.sql(f"DESCRIBE {TARGET_TABLE}").collect()]
    if "col_240" not in cols:
        spark.sql(f"""ALTER TABLE {TARGET_TABLE} ADD COLUMN col_240 STRING""")
    spark.sql(f"""UPDATE {TARGET_TABLE} SET col_240='VALUE_240' WHERE col_240 IS NULL""")
    cnt=spark.sql(f"SELECT COUNT(*) c FROM {TARGET_TABLE}").collect()[0]["c"]
    logging.info("STEP 240 rows=%s",cnt)

    logging.info("=== Job Success ===")
except Exception as e:
    logging.error(str(e))
    logging.error(traceback.format_exc())
    sys.exit(1)
finally:
    logging.info("=== Job End ===")
