# Airflow DAGs for copying tables from one DB to another

## Quick start

* Create a folder inside the project directory named `artifacts`. Download the MySQL odbc drivers. Note that the connector binary must match the one specified in the `Dockerfile`.

* Deploy the databases. It supports MS-SQL and MongoDB targets (It probably does not support other ODBC targets because of MS-SQL's rather unique UPSERT mechanism). It can source most ODBC sources. Again, modify the Dockerfile to include your favorite ODBC drivers (MySQL is what I tested this on).

* Run `docker-compose up airflow-init`. Once it finishes run `docker compose up`.

* You should be able to browse the Airflow admin page. Make sure the `sql_transfer_middleware` and `mongo_transfer_middleware` DAGs are loaded.

* Change variables.json as necessary. Upload it in Admin -> Variables -> Browse. Click import. It should show two variable keys, `AACC_MIDDLEWARE_MAPPING` and `MONGODB_MIDDLEWARE_MAPPING`. Check if the configuration is correct.

* Add your connections `odbc-core-source`, `odbc-core-target`, and `mongo-core-target` in Admin -> Connections. Make sure to adjust your variables as necessary.

* Now you should be able to run the two DAGs in the DAGs page.

## Quirks

* Do note that it creates and destroys a table named temp-<target> in the MS-SQL target database, and hence could cause data loss if such a table already exists and is being used for something.

* For MySQL `schema`s and `database`s are the same thing. This allows us to control the database (dot operator, e.g: `for * in schema1.table1`) we are querying in the mapping itself.

* For MS-SQL `schema`s and `database`s are different. By default the 'dbo' schema is selected.
  - Note that in the connection settings schema actually means database. We can use the dot operator (e.g: `for * in schema1.table1`) for specifying schema.

* Avoid using simplistic identifiers, they have a tendency to get reserved. We cannot guarantee proper handling of any quotation characters over ODBC. MS-SQL for example does not support \` quotation.

