# import findspark
# findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *
import pandas as pd

import os
from datetime import datetime, timedelta

def process_file(file_path, save_path, spark):
    print('------------------------')
    print(f'Processing file: {file_path}')
    print('------------------------')
    # print('Start Spark')
    # spark = SparkSession.builder.config("spark.driver.memory","8g").getOrCreate()
    # print('------------------------')
    print('Read data from HDFS')
    # print('------------------------')
    df = spark.read.json(file_path)
    print('----------------------')
    print('Showing data structure')
    print('----------------------')
    df.printSchema()
    print('Get data from column _source')
    df = df.select('_source.*')
    print('------------------------')
    print('Showing data structure after get data from _source')
    print('------------------------')
    df.printSchema()
    print('------------------------')
    print('Transforming data')
    # print('------------------------')
    df = df.withColumn("Type",
        when(  (col("AppName") =='CHANNEL') \
             | (col("AppName") =='DSHD')    \
             | (col("AppName") =='KPLUS')   \
             | (col("AppName") =='KPlus'), "TV")
        .when( (col("AppName") == 'VOD')    \
             | (col("AppName") =='FIMS_RES')\
             | (col("AppName") =='BHD_RES') \
             | (col("AppName") =='VOD_RES') \
             | (col("AppName") =='FIMS')    \
             | (col("AppName") =='BHD')     \
             | (col("AppName") =='DANET'), "Movie")
        .when( (col("AppName") == 'RELAX'), "Relax")
        .when( (col("AppName") == 'CHILD'), "Child")
        .when( (col("AppName") == 'SPORT'), "Sport")
        .otherwise("Error"))
    df = df.select('Contract','Type','TotalDuration')
    df = df.filter(df.Contract != '0' )
    df = df.filter(df.Type != 'Error')
    df = df.groupBy('Contract','Type').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)','TotalDuration')
    print('-----------------------------')
    print('Pivoting data')
    # print('-----------------------------')
    #result = pivot_data(df)
    result =  df.groupBy('Contract').pivot('Type').sum('TotalDuration')
    result = result.withColumn('Date', lit(file_path[-13:-5]))
    print('-----------------------------')
    print('Showing result output')
    print('-----------------------------')
    result.show(10,truncate=False)
    print('-----------------------------')
    print('Saving result output')
    print('-----------------------------')
    # result.repartition(1).write.csv(f'{save_path}_{file_path[-13:-5]}.csv', header=True)
    # Đoạn code dưới các kết quả từng ngày sẽ lưu vào các forder riêng biệt
    # result.repartition(1).write.csv(f'{save_path}\\{os.path.basename(file_path)[:-5]}.csv', header=True)
    result.coalesce(1).write.mode('overwrite').csv(f'{save_path}\\{os.path.basename(file_path)[:-5]}.csv', header=True)
    print(f'Successfully processed and saved: {file_path}')
    return print('The task ran Successfully (^-^)')
	
def main_task(path, save_path, start_date, end_date):
    # Khởi động Spark
    print('Start Spark')
    print('------------------------')
    spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()
    
    current_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')
    
    while current_date <= end_date:
        file_name = current_date.strftime('%Y%m%d') + '.json'
        file_path = os.path.join(path, file_name)
        
        if os.path.exists(file_path):
            process_file(file_path, save_path, spark)
        else:
            print(f'File not found: {file_path}')
        
        # Tăng ngày lên 1
        current_date += timedelta(days=1)
    
    print('All files processed successfully.')

path =  'C:\\Users\\August\\Desktop\\Spark\\Dataset\\File json\\'
save_path = 'C:\\Users\\August\\Desktop\\Spark\\Practice\\Data Result\\Clean_Data'
start_date = '20220401'
end_date = '20220430'

main_task(path, save_path, start_date, end_date)