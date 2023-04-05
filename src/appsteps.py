from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
from pyspark import StorageLevel
import logging

import src.external_api as api


def start_spark(executors: int, cores: int) -> SparkSession:
    spark = SparkSession.builder \
        .appName("MakeTransactions") \
        .config("spark.executor.instances", executors) \
        .config("spark.executor.cores", cores) \
        .getOrCreate()  
    return spark


def read_claim(spark: SparkSession, 
                  executors: int, 
                  cores: int,  
                  path: str,
                  csv_schema: StructType) -> DataFrame:
    logging.info("Start loading Claims")
    df_claim = spark \
                .read \
                .csv(path, header=True, schema=csv_schema) \
                .repartition(executors*cores) \
                .persist(StorageLevel.MEMORY_ONLY) 
    
    logging.info("Finish loading Claims")    
    return df_claim


def process_claim(df_claim: DataFrame) -> DataFrame:
    logging.info("Start processing Claims")
    get_API_udf = F.udf(api.getAPIdata, StringType())
    
    df_claim_processed = df_claim \
                            .withColumn("SOURCE_SYSTEM_ID", 
                                        F.split(df_claim.CLAIM_ID, "_")
                                        .getItem(1)
                                        ) \
                            .withColumn("TRANSACTION_TYPE", 
                                        F.when(df_claim.CLAIM_TYPE == "2",F.lit("Corporate"))
                                        .when(df_claim.CLAIM_TYPE == "1",F.lit("Private"))
                                        .otherwise(F.lit("Unknown"))
                                        ) \
                            .withColumn("TRANSACTION_DIRECTION", 
                                        F.when(df_claim.CLAIM_ID.startswith("CL"),F.lit("COINSURANCE"))
                                       .when(df_claim.CLAIM_ID.startswith("RX"),F.lit("REINSURANCE"))
                                        ) \
                            .withColumnRenamed("AMOUNT", "CONFORMED_VALUE") \
                            .withColumn("BUSINESS_DATE",
                                        F.date_format(F.to_date(
                                            df_claim.DATE_OF_LOSS, "dd.MM.yyyy"), 
                                                      "yyyy-MM-dd")
                                        ) \
                            .withColumn("CREATION_DATE",
                                        F.date_format(F.to_timestamp(
                                            df_claim.CREATION_DATE, "dd.MM.yyyy HH:mm"), 
                                                      "yyyy-MM-dd HH:mm:ss")
                                        ) \
                            .withColumn("SYSTEM_TIMESTAMP", 
                                        F.from_utc_timestamp(F.current_timestamp(), "GMT")) \
                            .withColumn("NSE_ID", get_API_udf(F.col("CLAIM_ID"))) \
                            .drop("CLAIM_ID", "CLAIM_TYPE", "DATE_OF_LOSS") 
    
    logging.info("Finish processing Claims")
    return df_claim_processed
    

def read_contract(spark: SparkSession,
                     path: str,
                     csv_schema: StructType) -> DataFrame: 
    logging.info("Start loading Contracts")    
    df_contract = spark \
                .read \
                .csv(path, header=True, schema=csv_schema) \
                .persist(StorageLevel.MEMORY_ONLY) 
    
    logging.info("Finish loading Contracts")        
    return df_contract
    
    
def process_contract(df_contract: DataFrame) -> DataFrame:                    
    logging.info("Start processing Contracts")    
    df_contract_processed = df_contract \
                .select(F.col("CONTRACT_ID")) 
                     
    df_contract_broadcasted = F.broadcast(df_contract_processed)
    
    logging.info("Finish processing Contracts")        
    return df_contract_broadcasted


def join_claim_contract(df_claim_processed: DataFrame, 
                        df_contract_broadcasted: DataFrame) -> DataFrame:
    logging.info("Start joining Claims and Contracts")        
    df_result = df_claim_processed \
        .join(df_contract_broadcasted, ["CONTRACT_ID"]) \
        .withColumn("CONTRACT_SOURCE_SYSTEM",F.lit("Europe 3")) \
        .withColumnRenamed("CONTRACT_ID", "CONTRACT_SOURCE_SYSTEM_ID") \
        .select([F.col("CONTRACT_SOURCE_SYSTEM"), 
             F.col("CONTRACT_SOURCE_SYSTEM_ID"), 
             F.col("SOURCE_SYSTEM_ID"),
             F.col("TRANSACTION_TYPE"),
             F.col("TRANSACTION_DIRECTION"),
             F.col("CONFORMED_VALUE"),
             F.col("BUSINESS_DATE"),
             F.col("CREATION_DATE"),
             F.col("SYSTEM_TIMESTAMP"),
             F.col("NSE_ID")
            ]) 
    logging.info("Finish joining Claims and Contracts")            
    return df_result
    
def write_transactions(df_result: DataFrame, 
                       path: str) -> None:  
    logging.info("Start writing transactions") 
    df_result \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .parquet(path)    
    
    logging.info("Finish writing transactions") 
    return df_result

