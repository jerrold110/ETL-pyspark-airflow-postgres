def load(job_timestamp):
    """
    Load job
    When dealing with PySpark DataFrames, it's often more efficient to write the transformed DataFrames to a persistent storage system (such as a file system or a database) and then read them back in when needed, rather than passing them directly between functions.

    Here's why:

    Persistence:

    Writing the DataFrames to a persistent storage system allows you to persist the data, making it available even after your Spark session has ended. If you pass DataFrames between functions within the same Spark session, they are retained in memory, but once the session ends, the data is lost.
    Decoupling Transformation and Loading:

    Separating the transformation and loading steps makes your ETL pipeline more modular and scalable. You can independently optimize each step without being tightly coupled to the specifics of the other.
    Parallelism and Distributed Processing:

    Persisting the data allows for parallel and distributed processing. Other Spark or non-Spark applications can read the data concurrently, and you can take advantage of distributed storage systems for improved performance.
        """
    print('Load starting')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]") \
                        .appName('Data ETL') \
                        .getOrCreate()
    
    # Load transformed data
    prefix_file_path = f'./transformed_data/{job_timestamp}'
    #dim_customer = spark.read.csv(f'{prefix_file_path}/dim_customer', header=True, inferSchema=True)
    #dim_film = spark.read.csv(path=f'{prefix_file_path}/dim_film ', header=True, inferSchema=True)
    dim_store = spark.read.csv(path=f'{prefix_file_path}/dim_store', header=True, inferSchema=True)
    #dim_date = spark.read.csv(path=f'{prefix_file_path}/dim_date ', header=True, inferSchema=True)
    #fact_sale = spark.read.csv(path=f'{prefix_file_path}/fact_sales', header=True, inferSchema=True)

    no_partitions = 4
    dim_store.repartition(no_partitions)

    # Posgres read and write functions for each table
    # Transaction isolation level is serializable
    ###############################################################################################################

    def read_db(table):
        """
        Returns a table as a dataframe. db: alabama db_vendor: posgresql
        """
        postgres_url = 'jdbc:postgresql://localhost:5432/alabama'
        properties = {
            "user": "my_user",
            "password": "my_user_1",
            "driver": "org.postgresql.Driver",
            "isolationLevel": "SERIALIZABLE"
        }
        
        return spark.read.jdbc(url=postgres_url, table=table, properties=properties)

    def write_df(df, table, partitions):
        """
        Write appends a dataframe as a table. db: alabama db_vendor: posgresql
        """
        postgres_url = 'jdbc:postgresql://localhost:5432/alabama'
        properties = {
            "user": "my_user",
            "password": "my_user_1",
            "driver": "org.postgresql.Driver",
            "numPartitions": str(partitions), # equal to or lesser than the no. partitions of the DF
            "isolationLevel": "SERIALIZABLE"
        }
        df.write.jdbc(url=postgres_url, table=table, mode="append", properties=properties)
        
    def filter_load_dim_df(df, table, partitions, idx_col):
        """
        Function for filtering out existing rows in the dimension tables before updating the data.
        """
        
        # Load unique identifiers already in the destination database
        print(f'Reading {table}')
        existing_table = read_db(table)
        # Filter out entries from dimension tables that are already there
        existing_ids = existing_table.select(idx_col).collect()
        non_existing_rows = df.filter(~df[idx_col].isin(existing_ids))
        # Load data into table
        print(f'Loading {table}')
        write_df(non_existing_rows, table, partitions)
        
        print(f'Load for {table} done')
    ###############################################################################################################
    filter_load_dim_df(dim_store, 'dim_store', no_partitions, 'store_id')

if __name__ == '__main__':
	from datetime import datetime
	load(datetime(2011, 1, 1, 0, 0, 0))
    