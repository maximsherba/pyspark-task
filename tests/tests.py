from unittest.mock import patch
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from decimal import Decimal
from datetime import datetime as dt
from chispa.dataframe_comparer import assert_df_equality

from src.appsteps import process_claim, process_contract, join_claim_contract
from src.appconfig import CLAIM_SCHEMA, CLAIM_OUTPUT_SCHEMA, CONTRACT_SCHEMA, CONTRACT_OUTPUT_SCHEMA, TRN_OUTPUT_SCHEMA


MOCK_API_RETURN = "3be76cd7f9646dd9d79132c46b3dd603"
MOCK_SYSTIMESTAMP_RETURN = dt.strptime("2023-01-01 11:11:00","%Y-%m-%d %H:%M:%S")
ORDER_BY_COL_CLAIM = "SOURCE_SYSTEM_ID"
ORDER_BY_COL_CONTRACT = "CONTRACT_ID"
ORDER_BY_COL_TRN = "SOURCE_SYSTEM_ID"

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("pytest-spark").getOrCreate()
               
@patch('pyspark.sql.functions.from_utc_timestamp')
@patch('src.external_api.getAPIdata')
def test_process_claim(mock_getAPIdata, mock_current_timestamp, spark):
    mock_getAPIdata.return_value = MOCK_API_RETURN
    mock_current_timestamp.return_value = F.lit(MOCK_SYSTIMESTAMP_RETURN)
    # Test data
    input_data = [
    ("Claim_SR_Europa_3","CL_685451230","Contract_SR_Europa_3",97563756,"2","14.02.2021",Decimal(523.21),"17.01.2022 14:45"),
    ("Claim_SR_Europa_3","CL_962234","Contract_SR_Europa_4",408124123,"1","30.01.2021",Decimal(52369.0),"17.01.2022 14:46"),
    ("Claim_SR_Europa_3","CL_895168","Contract_SR_Europa_3",13767503,"","02.09.2020",Decimal(98465),"17.01.2022 14:45"),
    ("Claim_SR_Europa_3","CX_12066501","Contract_SR_Europa_3",656948536,"2","04.01.2022",Decimal(9000),"17.01.2022 14:45"),
    ("Claim_SR_Europa_3","RX_9845163","Contract_SR_Europa_3",656948536,"2","04.06.2015",Decimal(11000),"17.01.2022 14:45"),
    ("Claim_SR_Europa_3","CL_39904634","Contract_SR_Europa_3",656948536,"2","04.11.2020",Decimal(11000),"17.01.2022 14:46"),
    ("Claim_SR_Europa_3","U_7065313","Contract_SR_Europa_3",46589516,"1","29.09.2021",Decimal(11000),"17.01.2022 14:46")
    ]
    
    output_data = [
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",656948536,Decimal(11000.00000),"2022-01-17 14:46:00",39904634,"Corporate", "COINSURANCE","2020-11-04",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",97563756,Decimal(523.21000),"2022-01-17 14:45:00",685451230,"Corporate", "COINSURANCE","2021-02-14",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_4",408124123,Decimal(52369.00000),"2022-01-17 14:46:00",962234,"Private", "COINSURANCE","2021-01-30",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",13767503,Decimal(98465.00000),"2022-01-17 14:45:00",895168,"Unknown", "COINSURANCE","2020-09-02",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",656948536,Decimal(9000.00000),"2022-01-17 14:45:00",12066501,"Corporate", None,"2022-01-04",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",656948536,Decimal(11000.00000),"2022-01-17 14:45:00",9845163,"Corporate", "REINSURANCE","2015-06-04",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",46589516,Decimal(11000.00000),"2022-01-17 14:46:00",7065313,"Private", None,"2021-09-29",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN)
    ]
    df = spark.createDataFrame(input_data, CLAIM_SCHEMA)    
    df_expected = spark.createDataFrame(output_data, CLAIM_OUTPUT_SCHEMA).orderBy(ORDER_BY_COL_CLAIM)
    df_processed = process_claim(df).orderBy(ORDER_BY_COL_CLAIM)
    assert_df_equality(df_processed, df_expected)
    
def test_process_contract(spark):    
    input_data = [
    ("Contract_SR_Europa_3",408124123,"Direct","01.01.2015","01.01.2099","17.01.2022 13:42"),
    ("Contract_SR_Europa_3",46784575,"Direct","01.01.2015","01.01.2099","17.01.2022 13:42"),
    ("Contract_SR_Europa_3",97563756,"","01.01.2015","01.01.2099","17.01.2022 13:42"),
    ("Contract_SR_Europa_3",13767503,"Reinsurance","01.01.2015","01.01.2099","17.01.2022 13:42"),
    ("Contract_SR_Europa_3",656948536,"","01.01.2015","01.01.2099","17.01.2022 13:42")
    ]
    output_data = [(13767503,),(46784575,),(97563756,),(408124123,),(656948536,)]
    df = spark.createDataFrame(input_data, CONTRACT_SCHEMA)    
    df_expected = spark.createDataFrame(output_data, CONTRACT_OUTPUT_SCHEMA).orderBy(ORDER_BY_COL_CONTRACT)
    df_processed = process_contract(df).orderBy(ORDER_BY_COL_CONTRACT)
    assert_df_equality(df_processed, df_expected)    

def test_join_claim_contract(spark):    
    input_data_claim = [
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",656948536,Decimal(11000.00000),"2022-01-17 14:46:00",39904634,"Corporate", "COINSURANCE","2020-11-04",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",97563756,Decimal(523.21000),"2022-01-17 14:45:00",685451230,"Corporate", "COINSURANCE","2021-02-14",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_4",408124123,Decimal(52369.00000),"2022-01-17 14:46:00",962234,"Private", "COINSURANCE","2021-01-30",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",13767503,Decimal(98465.00000),"2022-01-17 14:45:00",895168,"Unknown", "COINSURANCE","2020-09-02",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",656948536,Decimal(9000.00000),"2022-01-17 14:45:00",12066501,"Corporate", None,"2022-01-04",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",656948536,Decimal(11000.00000),"2022-01-17 14:45:00",9845163,"Corporate", "REINSURANCE","2015-06-04",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Claim_SR_Europa_3","Contract_SR_Europa_3",46589516,Decimal(11000.00000),"2022-01-17 14:46:00",7065313,"Private", None,"2021-09-29",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN)
    ]
    input_data_contract = [(13767503,),(46784575,),(97563756,),(408124123,),(656948536,)]
    output_data = [
    ("Europe 3",656948536,39904634,"Corporate","COINSURANCE",Decimal(11000.00000),"2020-11-04","2022-01-17 14:46:00",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Europe 3",97563756,68545123,"Corporate","COINSURANCE",Decimal(523.21000),"2021-02-14","2022-01-17 14:45:00",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Europe 3",656948536,9845163,"Corporate","REINSURANCE",Decimal(11000.00000),"2015-06-04","2022-01-17 14:45:00",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Europe 3",13767503,895168,"Unknown","COINSURANCE",Decimal(98465.00000),"2020-09-02","2022-01-17 14:45:00",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Europe 3",408124123,962234,"Private","COINSURANCE",Decimal(52369.00000),"2021-01-30","2022-01-17 14:46:00",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN),
    ("Europe 3",656948536,12066501,"Corporate",None,Decimal(9000.00000),"2022-01-04","2022-01-17 14:45:00",MOCK_SYSTIMESTAMP_RETURN, MOCK_API_RETURN)    
    ]        
    df_claim = spark.createDataFrame(input_data_claim, CLAIM_OUTPUT_SCHEMA)
    df_contract = spark.createDataFrame(input_data_contract, CONTRACT_OUTPUT_SCHEMA)
        
    df_expected = spark.createDataFrame(output_data, TRN_OUTPUT_SCHEMA).orderBy(ORDER_BY_COL_TRN)
    df_processed = join_claim_contract(df_claim, df_contract).orderBy(ORDER_BY_COL_TRN)
    assert_df_equality(df_processed, df_expected)        
    
  


