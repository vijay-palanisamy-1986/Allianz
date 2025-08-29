Welcome !

## Pre-requistis 
- If you are running this dbt-project with Apple silicon i.e., Macbook M1 or higher, Run the terminal using Rosetta [More info here](https://support.apple.com/en-us/102527)
- python3 installation
- dbt-core installation
- dbt-snowflake connector: python3 -m pip install dbt-core dbt-snowflake
- Snowflake database: database created for this assignment connection details in [profile file](3_Design/profiles.yml)


## About this project:
- This is dbt assignment created for Alliaze Interview evaluation purpose.
- Requirement of this assignment provided by Allianze is in [Allianz_DBT_Assignmnet_medior.doc](1_Requirements/Allianz_DBT_Assignmnet_medior.docx) 


## Design:
- 3 Layer approached has been choosen (RAW, Stage and Datavault)
- There no Dataware house and consumption related requirment. Hence these 2 layers are skipped (Out of scope :))


## Development$:
### RAW data is downloaded from www.kaggle.com
- This RAW layer holds the exact data that is coming from source either in files or in tables
- Loading Mechanism: Trucate and Load, means will have only last version of the data received
- Datasets
    - Customer - https://www.kaggle.com/datasets/shrutimechlearn/customer-data
    - Product - https://www.kaggle.com/datasets/sujaykapadnis/products-datasets
    - Order data - Cooked up by Vijay based on above Customer and Product data
    - All above RAW data is loaded into Snowflake database's Source schema. Mimicking datalake RAW layer
    - For reference, the source files also kept in ./Assignment/2 Source_data folder

### STAGE Layer: 
- This Stage layer is more or less equal to RAW layer but with little (technical) transformation i.e., data and field properties are prep for next Data vault layer.
- Loading Mechanism: Trucate and Load, means will have only last version of the data received
- Transformations:
    - Technical clean up
    - Field name standarazation
    - Data type conversion 
    - Metadata attachment with one to one data object mapping to Source data object
    - etc

### DATAVALUT layer:
Model the data using DataVault 2.0 model
- Loading Mechanism: Incremental with historic changes retention
- Customer:
    - Built customer satellite table to hold base customer data attributes (with holding historic changes)
    - Built customer profession satellite table to hold professional related attributes (with holding historic changes)
    - Built customer spending satellite table to hold customer spending related attributes (with historic changes capture)
    - Built hub table for Customer

- Product:
    - Built product satellite table to hold base product data attributes (with holding historic changes)
    - Built product profession satellite table to hold category related attributes (with holding historic changes)
    - Built product spending satellite table to hold discount related attributes (with historic changes capture)
    - Built hub table for Product

- Orders:
    - Built order satellite table to hold base product data attributes

- Link
    - Built orders link table to link Orders satellite table with Customer hub and Product hub tables

### Validation: 
2 type of validation are done: Technical & Business validaitons
1. Technical Validation: Technical validation are done via dbt model contracts. Definition for the model contracts are supplied via schema .yaml file of each model
2. Unit data test validation: Leveraged dbt unit test feature to do basic technical data validaiton
3. Business data valiation: 
    - Business or functional data validaiton are done with custom SQLs
    - Build re-usable modules with macro, so this business validaiton test can be scalled to N no of business or function tests
    - Outputs are store in VALIDATION schema
        - Each rule will have their own output object, which will have last run results
        - But common table VALIDATION.datatest_business_validation_outputs table will persis all execution validation results or output

## Documention:
- dbt offered documentation is heavily used to document  the code, component and logic
- All tables, fileld details are caputured in schema ymal file
- This schema yaml file can be futher used to build catalog, which give good amount of documentation about this project
- Care has been take while building the project, so that project data lineage also generated with dbt documentation

Steps to build/view the dbt documentation:
- Step 1: Build catalog buy running the command `dbt docs generate`
- Step 2: Run command `dbt docs serve --port 8001`
- Step 3: Open up the document in browser. Most probably http://localhost:8001/#!/overview 


## Excecution:
All the dbt model in this project can be excuted in different ways....
1. Individual models using command:  `dbt run --select modelName`
2. Whole project: 
    - Method 1: Using command:  `dbt run`
    - Method 2: Tags are configured [project.yaml](4_dbt_project/dbt_project.yml) file, so that whole project can be executed using tag: `dbt run --select tag:allianz_dbt_assignment`
3. Partially: Tags are configured [project.yaml](4_dbt_project/dbt_project.yml)file, so that each layer can be executed using tag: `dbt run --select tag:stage` or  `dbt run --select tag:datavault`
4. Care has been take to reference or putting dependiences during model development. Hence you can also execute dbt models flow with prefix or suffix using '+' operator
    - Example 1: `dbt run --select modelName+`
    - Example 2: `dbt run --select +modelName`
    - Example 3: `dbt run --select +modelName+`