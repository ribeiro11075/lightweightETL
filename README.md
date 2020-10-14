# lightweight-etl
A lightweight python library to to perform ETL (Extract, Transform, Load)

![ETL](https://www.blastanalytics.com/wp-content/uploads/extract-transform-load-icons-800x279.png)


## Getting Started
The repository includes example folders in different directories to provide an example of configuration that can be used as a reference

1. Follow the installation guide outlined below
1. Follow the code outlined in the `script` folder for additional guidance


## Installation
The installation steps include optional items that are dependent on requirements, setup, and user preferences

1. Setup prerequisites
    - [ ] **Required**: Install python3 by following the instructions [here](https://realpython.com/installing-python)
    - [ ] **Required**: Install pip by following the instructions [here](https://howchoo.com/g/mze4ntbknjk/install-pip-python)
    - [ ] **Optional**: Install Oracle Client by following the instructions [here](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html)

1. Create a virtual environment and install dependencies
    - [ ] **Optional**: Install the virtualenv python package `pip install virtualenv`
    - [ ] **Optional**: Create a folder to manage virtual environments `mkdir <environment_directory>`
    - [ ] **Optional**: Create a virtual environment `virtualenv <environment_directory>/<environment_name>`
    - [ ] **Optional**: Activate the virtual environment
        - Windows: `.\<environment_directory>/<environment_name>\Scripts\activate`
        - MacOS: `source <environment_directory>/<environment_name>\bin\activate`
    - [ ] **Required**: Navigate to the root directory of the repository `cd <repository_directory>`
    - [ ] **Required**: Install python dependencies in virtual environment `pip install -r requirements.txt`
    - [ ] **Optional**: Deactivate the virtual environment when finished `deactivate`

1. Define configuration
    - [ ] **Required**: Open `configuration/environment/database.yaml`
        - `database_alias` (**Required**): Used to get configuration throughout scripts (string)
        - `database_type` (**Required**): Database installation options [oracle, mysql, postgresql] (string)
        - `database_user` (**Required**): Database user credential (string)
        - `database_password` (**Required**): Database password credential (string)
    - [ ] **Required**: Create `configuration/environment/runtime.yaml` (COMING SOON)
    - [ ] **Required**: Create `configuration/job/<fileName>.yaml`
        - `workers` (**Required**): Number of cores to concurrently run with multiprocessing (number)
        - `jobName` (**Required**): Job alias that will be used for logging (string)
        - `active` (**Required**): Job run status with options of [true, false] (boolean)
        - `refresh` (**Required**): Number of seconds before rerunning the job (number)
        - `predecessors` (**Optional**): Jobs that should run before (list -> string)
        - `sourceDatabase` (**Required**): Source database alias where data is extracted from (string)
        - `targetDatabase` (**Required**): Target database alias where data is loaded to (string)
        - `insertStrategy` (**Required**): Strategy for inserting data with options of [swap, upsert] (string)
            - `swap`: Inserts data into a staging table and renames staging table to final table
            - `upsert`: Upserts data from memory or staging table to target table
        - `chunkSize` (**Required**): Number of rows in each insert chunk for performance (number)
        - `targetTableStage` (**Optional**): Staging table in target database (string)
            - Required when `insertStrategy=swap`
            - Used by default when `insertStrategy=upsert`
        - `targetTableFinal` (**Required**): Target table in target database (string)
        - `columnTransforms` (**Optional**): Transformations on data currently limited to currency (list -> string)
        - `preTargetAdhocQueries` (**Optional**): Queries run on target database pre data load (list -> string)
        - `postTargetAdhocQueries` (**Optional**): Queries run on target database post data load(list -> string)
        - `sourceQuery` (**Required**): Query to extract data from source database (string)
