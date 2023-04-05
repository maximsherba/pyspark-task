import appconfig as c
from appsteps import * 
import logging

  
if __name__ == "__main__":
    logging.info("Start Make Transactions")
    spark = start_spark(c.NUM_EXECUTORS, c.NUM_CORES)
    df_claim = read_claim(spark, c.NUM_EXECUTORS, c.NUM_CORES, c.FILE_PATH_READ_CLAIM, c.CLAIM_SCHEMA)
    df_claim_processed = process_claim(df_claim)
    df_contract = read_contract(spark, c.FILE_PATH_READ_CONTRACT, c.CONTRACT_SCHEMA)    
    df_contract_broadcasted = process_contract(df_contract) 
    df_result = join_claim_contract(df_claim_processed, df_contract_broadcasted)
    write_transactions(df_result,c.FILE_PATH_WRITE_TRN)
    logging.info("End Make Transactions")
