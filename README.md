Svenska versionen
Köksglädje ETL projekt

Köksglädje är ett ETL projekt som visar hur rå transaktionsdata kan hämtas från en extern källa, bearbetas och lagras i en relationsdatabas för vidare analys. Projektet är utvecklat inom ramen för utbildningen Data Manager och har som syfte att visa ett komplett dataflöde från källa till analysbar struktur med fokus på datakvalitet, normalisering och tydlig arkitektur.

Projektets syfte och omfattning:
Projektets mål är att demonstrera hur ett strukturerat ETL flöde kan byggas i Python. Rå data hämtas i CSV format, rensas och transformeras till en normaliserad datamodell innan den laddas in i en SQLite databas. Fokus ligger på tydlig separation mellan extraktion, transformation och laddning samt på att skapa en databas som är enkel att analysera vidare.

Arkitektur och ETL flöde:
ETL processen är orkestrerad med Apache Airflow. Extraktionen innebär att data hämtas automatiskt från en extern källa. Under transformationssteget rensas datan, datatyper justeras och informationen delas upp i logiska tabeller med relationer. Slutligen laddas den transformerade datan in i en SQLite databas som utgör slutresultatet av ETL flödet.

Databas och datamodell:
Efter att ETL flödet har körts finns den färdiga datan lagrad i en SQLite databas. Datamodellen är normaliserad för att minska redundans och skapa tydliga relationer mellan exempelvis transaktioner och tillhörande information. Denna struktur gör databasen väl lämpad för analys och visualisering.

Projektet är organiserat enligt följande struktur:
Sökvägen airflow innehåller en dags mapp där filen transactions_dag.py ligger. Denna fil innehåller hela ETL logiken och Airflow DAG definitionen. I data mappen ligger SQLite databasen köksglädje.db som skapas och uppdateras av ETL flödet och fungerar som projektets slutliga output.

Viktigt vid körning av projektet:
För att kunna köra koden lokalt krävs att databasen finns på rätt plats enligt projektets mappstruktur. Airflow DAGen förväntar sig att SQLite databasen ligger i data mappen med korrekt sökväg. Om databasen saknas eller ligger på fel plats kommer ETL flödet inte att fungera som avsett. Vid första körning skapas databasen automatiskt, men mappstrukturen måste redan finnas.

Analys och vidare användning:
När ETL flödet har körts är databasen redo att användas för analys. Analys och visualisering sker utanför Airflow, exempelvis via Jupyter Notebook eller andra analysverktyg. Projektet är även förberett för att kunna kopplas till externa verktyg som Power BI.

Projektets roll som portföljarbete:
Köksglädje är tänkt som ett portföljprojekt och ett pedagogiskt exempel snarare än ett färdigt produktionssystem. Projektet visar förståelse för ETL processer, datamodellering, databashantering och grundläggande analys i ett realistiskt sammanhang.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

English version
Köksglädje ETL project

Köksglädje is an ETL project that demonstrates how raw transaction data can be extracted from an external source, transformed and stored in a relational database for further analysis. The project was developed as part of the Data Manager program and aims to present a complete data pipeline from source to analysis ready structure with a strong focus on data quality, normalization and clear architecture.

Project purpose and scope:
The purpose of this project is to demonstrate how a structured ETL pipeline can be built using Python. Raw data is extracted in CSV format, cleaned and transformed into a normalized data model before being loaded into a SQLite database. The main focus is on a clear separation between extraction, transformation and loading, as well as on building a database that is easy to analyze.

Architecture and ETL flow:
The ETL process is orchestrated using Apache Airflow. During the extraction step, data is automatically retrieved from an external source. In the transformation step, the data is cleaned, data types are adjusted and the information is split into logical tables with defined relationships. Finally, the transformed data is loaded into a SQLite database, which represents the final output of the ETL pipeline.

Database and data model:
After the ETL flow has been executed, the processed data is stored in a SQLite database. The data model is normalized to reduce redundancy and to create clear relationships between transactions and related entities. This structure makes the database suitable for further analysis and visualization.

Project structure:
The project follows a clear folder structure. The airflow directory contains a dags folder where the file transactions_dag.py is located. This file contains the full ETL logic and the Airflow DAG definition. The data directory contains the SQLite database köksglädje.db, which is created and updated by the ETL flow and represents the final ETL output.

Important information for running the project:
To run the code locally, the database must be located in the correct directory according to the project structure. The Airflow DAG expects the SQLite database to be placed inside the data folder using the correct path. If the database is missing or placed incorrectly, the ETL flow will not run as intended. On the first execution, the database is created automatically, but the folder structure must already exist.

Analysis and further usage:
Once the ETL flow has been executed, the database is ready for analysis. Analysis and visualization are performed outside of Airflow, for example using Jupyter Notebook or other analysis tools. The project is also prepared to be connected to external tools such as Power BI.

Project as a portfolio example:
Köksglädje is intended as a portfolio project and a learning example rather than a production ready system. It demonstrates knowledge of ETL processes, data modeling, database management and basic data analysis in a realistic context.
