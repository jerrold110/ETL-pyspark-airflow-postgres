## ETL-pyspark-airflow-postgres
This project involves creating a scalable ETL data pipeline and modelling a data warehouse, with the source being an S3 bucket and the destination being a PostgreSQL datebase, orchestrated with Airflow. The data is obtained from a sample DVD rental database dataset. This pipeline persists snapshots of the data processed in every batch, and logging of batch successes and errors

We assume we are working with large amounts of data in each table. Spark is used for parallel and distributed processing of the T and L jobs. Database is optimised for analytics queries by using relevant indexes and partitions on the tables (and primary/foreign keys). 

### Pipeline diagram
![My Image](/Resources/Diagram.jpg "Pipeline diagram")
### Warehouse schema
![My Image](/Resources/Warehouse.png "Warehouse schema")
### Airflow DAG
![My Image](/Resources/Grid.png "Airflow DAG")

### Extract
Data objects downloaded from an AWS data lake with the AWS SDK (python) at each batch and stored locally

### Transform
Reads the extracted data into dataframes and defines the schema for each file. Dataframes are cached if reused, and large dataframes are repartitioned for improved performance of the transform operations. Data is transformed using SQL in pyspark, creating the fact and dimension tables. The transformed data is writted to persistent storage instead of passing between functions for reasons explained in transform.py documentation. The transformed data is repartitioned for parallel writing to store a snapshot of the transformed data, and parallel reading later on. 

### Load
The data is read into dataframes and repartitioned for parallel writing. Read and write isolation level is serializable. Fact tables are directly appended to the database table, dimension tables (eg: customer/store data) involve a slightly different process of querying the unique identifiers of records already existing in the target table, filtering out these rows from the dataframes, and appending the rows that are not already present (identified by the unique identifier) to ensure there are no duplicate records in the warehouse.
