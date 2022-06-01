# The-Journey-of-CDC-to-SCD-in-Delta-Lake

## What is Change Data Capture (CDC)?

CDC (Change Data Capture) is a collection of software design patterns used to detect any data change in the database. It triggers the event associated with data so that a particular action will be taken for any Change Data Capture.

## How does CDC work?

 CDC tracks row-level changes in database-source tables categorized as update, insert, and delete events- and then make those change notifications available to any other services or systems that depend on the same data. The change notifications are sent in the same order as they were generated in the original database. This way, CDC ensures that all the interested parties of a given dataset are precisely informed of the change and can react accordingly, by either refreshing their own version of the data or by triggering business processes
 
## Benefits of CDC

CDC Generates more Savings<br/>
CDC Generates more Revenue<br/>
CDC Protects Business Assets<br/>
CDC gets rid of Opportunity Costs<br/>


## Implementation Techniques for Change Data Capture (CDC)

1) **Timestamp Based Technique**<br/>

This technique depends on a timestamp field in the source to identify and extract the changed data sets.

2) **Triggers Based Technique**<br/>

This technique requires the creation of database triggers to identify the changes that have occurred in the source system and then capture those changes into the target database.
The implementation of this technique is specific to the database on which the triggers need to be created.

3) **Snapshot Based Technique**<br/>

This technique involves creating a complete extract of data from the source table in the target staging area.
Therefore, the next time the incremental data needs to be loaded, a second version or snapshot of the source table is compared to the original one for spotting the changes.

4) **Log Based Technique**<br/>

Almost all Database Management Systems have a transaction log file that records all changes and modifications in the database made by each transaction. 
In general, every DML operation such as CREATE, UPDATE, DELETE is captured in a log file in the database, along with the timestamp or a database-specific unique identifier indicating when each of these operations was incurred.
This log-based technique depends on this log information to spot the changes and perform CDC operations

## Use Cases for Change Data Capture in ETL

1) **Transaction Analysis**<br/>

 - **Fraud detection**<br/>
You want to analyze transactions in some sort of batch manner to see if credit cards are being used from multiple locations at the same time.
 - **Kafka pipeline**<br/>
You want some sort of analysis done on a transaction level and not an aggregated level.

2) **Data Duplication**<br/>
 - **Kafka pipeline**<br/>
Database mirroring is a strategy used in High Availability (HA) and Disaster Recovery (DR) database deployments. It involves two or three SQL Server instances where one acts as a primary instance (principal), the other as a mirrored instance (mirror), while the third instance acts as the witness.
 - **Database replication**<br/>
Database replication is the process of copying data from a database in one server to a database in another server so that all users share the same level of information without any inconsistency. It can be a one-time operation or an ongoing process.

## Slowly Changing Dimensions (SCDs)

SCDs refer to a data warehousing concept where dimensions contain both current AND historical data. This is a common approach to maintaining historical details in a data warehouse but their use will depend on both the business needs and purpose of the data warehouse.

At a basic level, the dimension will change slowly over time rather than at a scheduled interval — there is no fixed pattern in the frequency of the dimension changes.

This raises the question as to whether the data warehouse will store current or historical data or both. The decision will likely be dictated by business need and will be implemented and managed by a Data Architect or Engineer, but understanding the type of dimensions used and the rationale will be critical to the Data Analyst’s ability to respond to business questions and perform their role effectively.

## Different types of slowly changing dimensions

- [Type 0: Always retains original](https://github.com/PAVAN-221/Delta-Lake/edit/main/README.md#type-0-always-retains-original)<br/>
- [Type 1 : Keeps latest data, old data is overwritten](https://github.com/PAVAN-221/Delta-Lake/edit/main/README.md#type-1-keeps-latest-data-old-data-is-overwritten)<br/>
- [Type 2 : Keeps the history of old data by adding new row](https://github.com/PAVAN-221/Delta-Lake/edit/main/README.md#type-2-keeps-the-history-of-old-data-by-adding-new-row)<br/>
- [Type 3 : Adds new attribute to store changed value](https://github.com/PAVAN-221/Delta-Lake/edit/main/README.md#type-3-adds-new-attribute-to-store-changed-value)<br/>
- [Type 4 : Uses separate history table](https://github.com/PAVAN-221/Delta-Lake/edit/main/README.md#type-4-uses-separate-history-table)<br/>
- [Type 6 : combination of type 1, 2 and 3](https://github.com/PAVAN-221/Delta-Lake/edit/main/README.md#type-6-combination-of-type-1-2-and-3)<br/>



## Type 0: Always retains original

The type 0 is passive method. And therefore, perform no actions on dimension changes. Values remain as they were at the time the dimension table record was first inserted, others may be overwritten.



## Type 1: Keeps latest data, old data is overwritten

This method overwrites the old data, hence doesn’t keep historical data.

Consider the patient table


| SK | Pat_ID | Name | Cellphone |
| :--- | :--- | :--- | :--- |	
| 100	| 1	| ABC	| 1234567890 |


In the above table, pat_ID is the natural key and SK is surrogate key column. Technically, SK column is not required as natural key uniquely identifies the records. Join tables using this key column and this column is unique key.

If the patient changes his/her cellphone number, overwrite the old data.

| SK | Pat_ID | Name | Cellphone |
| :--- | :--- | :--- | :--- |	
| 100	| 1	| ABC	| 9876543210 |


One of the Type 1 method disadvantages is, there is no historical data in data warehouse. However, Type 1 maintenance is very easy, and advantage is reduced the data warehouse size.

## Type 2: Keeps the history of old data by adding new row

This method keeps track of historical data by adding multiple rows of given key columns in dimension table. Some SCD type 2 implementations use effective from and to date with flag indicating latest record.

| SK	| Pat_ID |	Name | Cellphone |	EFF_FR_DT |	EFF_TO_DT	| flag |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 100 |	1 |	ABC |	1234567890 |	2016-06-01	| 2016-06-10 |	0 |
| 100	| 1 |	ABC	| 9876543210	| 2016-06-10	| NULL	| 1 |

Flag = 1 and EFF_TO_DT = NULL indicates current or latest tuple versions. In some implementation, data modeller uses future date (9999-12-31) as effective to date.  In case if you want to track the historical records, you have to select all records which are non-null and flag = 0 values.

Adding new records in changes to the dimensional model in type 2 could be very expensive database operation so it is not recommended to use it in dimensions where a new attribute/columns could be added to table in the future.

## Type 3: Adds new attribute to store changed value

This method of slowly changing dimensions keeps the limited history of the changed data in form of new column. The history preservation is limited to the number of columns designed for storing historical data. The original table structure in type 1 and 2 is same , but type 3 adds new column to store current data.

For example, in following example, add additional column to deal with cell phone number changes.

| SK	| Pat_ID |	Name |	Previous_Cellphone |	Current_Cellphone |
| :--- | :--- | :--- | :--- | :--- |
| 100 |	1 |	ABC |	1234567890 |	9876543210 |

The disadvantage with this method is that it keeps limited history about changed data. In above table, new columns are added to hold new and changed data i.e. current_cellphone and previous_cellphones respectively.

## Type 4: Uses separate history table

Type 4 usually uses the separate table to hold history data, where as original table keeps the current data, and create other table to store the changed data. Add surrogate key to both tables to and is used in fact table to identify original and history data.

Patient table

| SK	| Pat_ID |	Name |	Cellphone |
| :--- | :--- | :--- | :--- |
| 100 |	1 |	ABC |	9876543210 |

Patient History table.

| SK	| Pat_ID |	Name	| Cellphone |	CRT_DT |
| :--- | :--- | :--- | :--- | :--- |
| 100 |	1	| ABC |	1234567890 |	2016-06-01 |
| 100 |	1 |	ABC	| 9876543210	| 2016-06-10 |



## Type 6: Combination of type 1, 2 and 3

Finally, the Type 6 method combines the approaches of types 1, 2 and 3. Therefore, this type is called hybrid method. In this type, below are the changes to dimension table:

- Current_record – keeping the current data
- Historical_record – keeping historical or changed data
- Effective_from_date – keeping start date of record, null for new records
- Effective_to_date –keeping end of expired record
- Curr_flag – indicates current records


Attribute changes are captured in this type of SCD, a new table record is added as in SCD type 2. Overwrite the old information with the new data as in type 1. And preserve history in a historical_record as in type 3.

Below is type 6 table example

| Pat_ID |	Name |	Current_Cellphone |	Historical_Cellphone |	EFF_FR_DT |	EFF_TO_DT |	flag |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1	| ABC |	9876543210 |	2345678901 |	2015-01-01 |	2016-01-20 |	0 |
| 1	| ABC	| 9876543210 |	1234567890 | 2016-01-21 |	Null |	1 |

## Simplifying Change Data Capture with Databricks Delta

A common use case that we are seeing today is the customers looking to perform change data capture (CDC) from one or many sources into a set of Databricks Delta tables. These sources may be on-premises or in the cloud, operational transactional stores, or data warehouses. The common glue that binds them all is they have change sets generated:
-	using an ETL tool like Oracle GoldenGate or Informatica PowerExchange,
-	from vendor-supplied change tables (e.g., Oracle Change Data Capture), or
-	user-maintained database tables that capture change sets using insert/update/delete triggers

and they wish to merge these change sets into Databricks Delta.


## CDC before Databricks Delta

Consider below scenario 

Informatica => Oracle => Spark Nightly Batch Job => Databricks.

In this scenario, Informatica pushes change sets from over 30 different data sources and consolidates them in an Oracle data warehouse. Approximately once a day, Databricks jobs retrieve these change sets from Oracle, via JDBC, and refresh tables in Databricks. While this scheme was successfully productionized, it had two major drawbacks:

1.	It added load to an already overloaded Oracle instance, which resulted in constraints on when and how these ETL jobs could run, and
2.	The refresh rates were at best nightly, due to concurrency limitations of vanilla Parquet tables (prior to Databricks Delta).

## CDC with Databricks Delta

With Databricks Delta, the CDC pipeline is now streamlined and can be refreshed more frequently: Informatica => S3 => Spark Hourly Batch Job => Delta. In this scenario, Informatica writes change sets directly to S3 using Informatica’s Parquet writer. Databricks jobs run at the desired sub-nightly refresh rate (e.g., every 15 min, hourly, every 3 hours, etc.) to read these change sets and update the target Databricks Delta table.

With minor changes, this pipeline has also been adapted to read CDC records from Kafka, so the pipeline there would look like Kafka => Spark => Delta. In the rest of this section, we elaborate on this process, and how we use Databricks Delta as a sink for their CDC workflows.

## Using Insert Overwrite

The basic idea behind this approach is to maintain a staging table that accumulates all updates for a given recordset and a final table that contains the current up-to-date snapshot that users can query.

For every refresh period, a Spark job will run two INSERT statements.

- Insert (Insert 1): Read the change sets from S3 or Kafka in this refresh period, and INSERT those changes into the staging table.
-	Insert Overwrite (Insert 2): Get the current version of every record set from the staging table and overwrite those records in the final table.

A familiar classification scheme to CDC practitioners is the different Types of handling updates ala slowly changing dimensions (SCDs). Our staging table maps closest to an SCD Type 2 scheme whereas our final table maps closest to an SCD Type 1 scheme.

## Implementation

Let’s dive deeper into the two steps, starting with the first insert.

```
%scala
val changeSets = Array(file1, file2, …)
spark.read.parquet(changeSets :_*).createOrReplaceTempView("incremental")

%sql
INSERT INTO T_STAGING
PARTITION(CREATE_DATE_YEAR)
SELECT ID, VALUE, CDC_TIMESTAMP
 FROM INCREMENTAL
```

Here, the first cell defines a temporary view over the change sets which is fed to the INSERT INTO in the second cell. The INSERT INTO is fairly straightforward with the exception of the PARTITION clause, so let’s take a moment to unwrap that one.


Next, we look at the second insert.

```
%sql
INSERT OVERWRITE TABLE T_FINAL
   PARTITION(CREATE_DATE_YEAR)
   SELECT ID, VALUE, CDC_TIMESTAMP  
   FROM (
      SELECT A.*,
             RANK() OVER (PARTITION BY ID ORDER BY CDC_TIMESTAMP DESC) AS RNK
        FROM T_STAGING A.*
       WHERE CREATE_DATE_YEAR IN (2018, 2016, 2015)
   ) B
WHERE B.RNK = 1 AND B.FLAG < > 'D'
```

