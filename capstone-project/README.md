## 1 Project Summary
This project is primarily scoped to illustrate the fitness of modern “Data Engineering Technologies”, such as park, Apache Airflow, Redshift, S3 and Cloud infrastructure, in context of Large-scale Data Transformation (BigData ETL, Data lake). 

One of the primary challange of BigData solution is  - Scale to Demand and Adapt to growing needs of Data-scientist (data-exploration, pattern-recognizition, connecting-data for experiments) and Business ( almost realtime information flow, multiple aspects of data, decision making capability etc.). 
       
In this project we will briefly touch multiple phases and aspects of solution-cycle, such as Data-gathering, Data-assessment/understanding, Data-modeling, BigData Solution stack and Design & Build. Our end deliverable will be end-to-end working-model, ready to be deployed on Cloud, AWS. We will keep design simple and modular, easy to understand and extend.

Although evaluation of “Data Engineering Technologies” is primary driver of this project, but selection of appropriate data-set (data-quality & data-size) is very important factor for success of this project. We will use data set provided by Udacity. Udacity data set is free for its students and is of high-quality & high-volume (more than three million records). 

 

<hr>

## 2 Primary Objective
Project will focus to answer below insights/query and will guide design of LDM (Logical data model) of OLAP (Fact – Dimension model).

       a) Insight on visitor type (Student, Tourism, Skilled worker, Business)
       b) Country of visitor
       c) Destination of visitor (US cities)
       d) Climate/temperature of US cities 


<hr>

## 3 Data modeling
### 3.1 Data set
#### Datasets
       a) I94 Immigration Data: This data comes from the US National Tourism and Trade Office. This is where the data comes from. 

       b) World Temperature Data: This dataset came from Kaggle. You can read more about it here.

       c) U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it here.

       d) Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from here.


#### 3.2 Data assessment 

<b>I94 Immigration Data </b>

* Overall assessment

![Alt text](images/I94ImmigrationData_Overall.png?raw=true "Data assessment")

* Column level assessment 

![Alt text](images/I94ImmigrationData_fieldlevel.png?raw=true "Data assessment")

<b> World Temperature Data </b>

* Overall assessment

![Alt text](images/WorldTemperature_Overall.png?raw=true "Data assessment  View")

* Column level assessment

![Alt text](images/WorldTemperature_fieldlevel.png?raw=true "Data assessment  View")

<b> U.S. City Demographic Data </b>

* Overall assessment

![Alt text](images/USCityDemography_overall.png?raw=true "Data assessment  View")

* Column level assessment

![Alt text](images/USCityDemography_fieldlevel.png?raw=true "Data assessment  View")

<b> Airport Code Table </b>

* Overall assessment

![Alt text](images/AirportCode_overall.png?raw=true "Data assessment  View")


* Column level assessment

![Alt text](images/AirportCode_fieldLevel.png?raw=true "Data assessment  View")





#### 3.3 Dictionary Data

Dictionary data is provided by Udacity 

       a) i94cntyl.txt: Country and the corresponding codes. 
       b) i94addrl.txt:  USA state name and code.
       c) i94model.txt:  i94Mode. 
       d) i94prtl.txt:  USA port codes map to city, state. 
       e) i94visa.txt: Visa types.

                        

#### 3.4 Logical data model

This project will use “Fact & Dimension” LDM model (Star schema) for OLAP cubes.

![Alt text](images/LogicalDataModel.png?raw=true "LDM")



<hr>

## 4 Descriptions of artifacts

              ├───airflow
              │   ├───dags
              │   │   ├───lib
              │   │   └───__pycache__
              │   └───plugins
              │       ├───operators
              │       │   └───__pycache__
              │       └───__pycache__
              ├───dictionary_data
              ├───images
              ├───output_data
              │   ├───airport_table.parquet
              │   ├───arrival_port_table.parquet
              │   ├───country_table.parquet
              │   ├───dem_table.parquet
              │   ├───person_table.parquet
              │   ├───status_table.parquet
              │   └───time_table.parquet
              └───sas_data

The <b>airflow</b> folder is base folder. <b>airflow</b> contains <b>dag</b> and <b>plugins</b>   folder.  <b>dag</b> folder contains collection of DAGs and <b>plugins</b> folder contains operators and helper/utility clasess, SQLs.


<hr>

## 5 Design overview - DAGS and Operators

### 5.1 Operators
Airflow DAGs will be used to Orchestrate end to end pipeline¶

At high level we will use below Operators to build end to end flow.

       a) Airflow Operator to Create EMR cluster
       b) Airflow Operator EMR cluster status/sensor check
       c) Airflow Operator EMR cluster terminate
       d) Airflow Operator Load and Transfrom data ( Using Spark and S3)
       e) Airflow Operator Data Quality check

### 5.2 Data Quality Operator
Data Quality Operator is meant for data quality checks. This operator can be extended to add any bussiness or data quality check operation to improve quality and confidence in data.

Brief on Data Quality Checks

       a)Data Dictionary will used to check relevence of data.
       b)Duplicate data will be removed
       c)Missing data will be ignored
       d)Intrgrity constraint will be maintained by Primary keys


### 5.5 Graph View of DAG
![Alt text](images/DAG_capstone.png?raw=true "DAG Graph View")





<hr>

### 6 Rationale for the choice of tools and technologies for the project.
#### 6.1 Choice of solution stack and tools
* This project will use EC2, S3, EMR cluster (Spark), Apache Airflow - these are core components of solution stack.
* S3 – S3 Storage is elastic and scales up/shrinks down effectivly as per demand. S3 sotrage is distributed, highly availabel and support high speed I/O. Hence it is a very good choice of Big Data workloads.

* EMR – Spark EMR cluster can scale up/shrink-down as per workload demand. Spark is best at parallel/distributed computing. Hence it is a very good choice of Big Data workloads.

* Apache Airflow – Data pipeline and orchestration. Airflow can manages end to end orchestration of workflow as well as monitor status of each task/job. Airflow has many useful Hooks and Operator already built in for connecting and managing Cloud services. Hence it is a very good choice of Big Data workloads.

2) Propose how often the data should be updated and why.

As per current usecase requirement, data can be update once a day. However, if usecase needs change, data load frequency can be configured to hourly.

#### 6.2 How you would approach the problem differently under the following scenarios.

* 6.2.1) The data was increased by 100x.
a) In case data volume increase by 100%?
       
       we need to scale Up EMR Spark cluster and S3 capacity. Since Spark is distributed in nature and can increase parallel/distributed computing based on number of worker nodes, increasing EMR capacity (worker nodes) should help to accomodate 100x increase.
       
       S3 storage can easily scale up to compliment Spark processing and provide distributed storage for 100x data increase.
       
       Airflow only orchestrates flow/pipeline and does not process data, Airflow capacity increase is not required.

* 6.2.2) The data populates a dashboard that must be updated on a daily basis by 7am every day?

       For Such requirement, we can extend our Data lake by Adding RedShift OLAP tables. Spark can populate RedShift OLAP cubes with transformed data in advance. This will help to manage SLA for "Data being ready by 7 AM every day" for consumption. 

* 6.2.3) The database needed to be accessed by 100+ people?

       For concurrent data consumption by Applications/Consumer, it will be good idea to Add RedShift OLAP tables (Staging and OLAP). This will ensure data-consumtion by Applications/Consumer is rendered by RedShift. RedShift is distributed and MPP database, which can scale up to petabyte data and is good solution-block in Big Data landscape.



<hr>

## 7 How do I setup and execute program?
#### Follow below steps in order
 a. Logon to AWS.
 
 b. Configure (airflow.cfg) Airflow to point to <b>airflow</b> folder. 
     
       dags_folder = /airflow/dags
       
 c. Configure AWS credentials(<b>aws_credentials</b>), "region" variable, "s3-bucket" conection in AirFlow.
 
 d. Execute script <b>upload_dataset</b> to upload data in S3.
 
 d. Execute dag <b>udac_create_emr_cluster_dag</b> to create EMR cluster. 
 
 e. Execute dag <b>udac_immigration_etl_dag</b> to execute ETL pipeline.
 
       i) udac_create_emr_cluster_dag : This DAG creates EMR cluster. Schduler for this DAG is set as None, this means this DAG requires external trigger to execute.
       ii) udac_immigration_etl_dag : This DAG performs end to end ETL task/jobs. It can be scheduled to run repeatedly.
       iii) udac_terminate_emr_dag : This DAG terminates EMR cluster instance. Schduler for this DAG is set as None, this means this DAG requires external trigger to execute.
