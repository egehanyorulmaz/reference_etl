# Developing reliable and flexible ETL pipelines using Apache Airflow
This repository is created for the Medium Article, "Developing reliable and flexible ETL pipelines using Apache Airflow" and consists the steps of creating an ETL pipeline from scratch using reference tables.

You need certain applications to follow the steps of the tutorial. We will be using Docker, Docker Compose, Python \
Docker Desktop : https://docs.docker.com/get-docker/ \
Docker Compose : https://docs.docker.com/compose/install/ \
Python: https://www.python.org/downloads/

After successfully downloading Docker and Docker Compose, you have certain options to install PostgreSQL and MySQL, but the easiest way is to use docker-compose. I have designed the docker-compose.yml file to install both of them for the sake of this tutorial along with Apache Airflow to use.

Please follow these steps to install PostgreSQL, MySQL and Apache Airflow as docker container: \
1- Navigate to the path of the .yml file using terminal \
2- Make sure docker is up and running.\
3- Run `docker-compose -f etl_databases.yml up -d` in the terminal to install Postgresql and MySQL databases. \
4- Run `docker-compose -f apache-airflow.yaml up -d` in the terminal to install Apache Airflow and required dependent services.


To access the database, you can use tools like DataGrip. I highly recommend using DataGrip if you are working with multiple databases at the same time. It allows migrating data from one database to another and it will be a useful feature if you are not concerned about time restrictions. But, I have to say it will be inefficient.

