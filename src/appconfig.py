from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, TimestampType

NUM_CORES = 1
NUM_EXECUTORS = 3
FILE_PATH_READ_CLAIM = "data/input/claim.csv"
FILE_PATH_READ_CONTRACT = "data/input/contract.csv"
FILE_PATH_WRITE_TRN = "data/output/TRANSACTIONS"

CLAIM_SCHEMA = StructType([
    StructField("SOURCE_SYSTEM", StringType(), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), False),
    StructField("CONTRACT_ID", IntegerType(), False),
    StructField("CLAIM_TYPE", StringType(), True),
    StructField("DATE_OF_LOSS", StringType(), True),
    StructField("AMOUNT", DecimalType(16,5), True),
    StructField("CREATION_DATE", StringType(), True)
])

CLAIM_OUTPUT_SCHEMA = StructType([
    StructField("SOURCE_SYSTEM", StringType(), False),
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), False),
    StructField("CONTRACT_ID", IntegerType(), False),
    StructField("CONFORMED_VALUE", DecimalType(16,5), True),    
    StructField("CREATION_DATE", StringType(), True),  
    StructField("SOURCE_SYSTEM_ID", StringType(), True),    
    StructField("TRANSACTION_TYPE", StringType(), False),
    StructField("TRANSACTION_DIRECTION", StringType(), True),    
    StructField("BUSINESS_DATE", StringType(), True),
    StructField("SYSTEM_TIMESTAMP", TimestampType(), False),      
    StructField("NSE_ID", StringType(), True)
])

CONTRACT_SCHEMA = StructType([
    StructField("SOURCE_SYSTEM", StringType(), False),
    StructField("CONTRACT_ID", IntegerType(), False),
    StructField("CONTRACT_TYPE", StringType(), True),
    StructField("INSURED_PERIOD_FROM", StringType(), True),
    StructField("INSURED_PERIOD_TO", StringType(), True),
    StructField("CREATION_DATE", StringType(), True)
])

CONTRACT_OUTPUT_SCHEMA = StructType([
    StructField("CONTRACT_ID", IntegerType(), False)
])

TRN_OUTPUT_SCHEMA = StructType([
    StructField('CONTRACT_SOURCE_SYSTEM', StringType(), False),
    StructField('SOURCE_SYSTEM_ID', IntegerType(), False),
    StructField('CONTRACT_SOURCE_SYSTEM_ID', IntegerType(), False),
    StructField('SOURCE_SYSTEM_ID', StringType(), True),
    StructField('TRANSACTION_TYPE', StringType(), False),
    StructField('TRANSACTION_DIRECTION', StringType(), True),
    StructField('CONFORMED_VALUE', DecimalType(16,5), True),
    StructField('BUSINESS_DATE', StringType(), True),
    StructField('CREATION_DATE', StringType(), True),
    StructField('SYSTEM_TIMESTAMP', TimestampType(), False),
    StructField('NSE_ID', StringType(), True)     
])