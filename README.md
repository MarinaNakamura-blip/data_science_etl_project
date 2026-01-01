# data_science_etl_project
School group work where we created an etl transformation into airflow.

README version svenska
Projektöversikt

Detta projekt är ett data pipeline projekt där transaktionsdata hämtas, bearbetas, lagras och visualiseras. Syftet är att visa hela flödet från rådata till insikter genom visualiseringar, med fokus på tydlighet och datakvalitet snarare än avancerad kod.

Projektet är uppdelat i tre huvudsakliga delar
Datainsamling
Datatransformation och lagring
Datavisualisering och presentation

Datakälla

Datan består av transaktioner i CSV format som innehåller information om exempelvis
Transaktions id
Tidpunkt
Belopp
Produkter och kvantitet

Datan hämtas automatiskt via en ETL process.

Databearbetning

Efter hämtning bearbetas datan i Python.
Rådatat normaliseras och delas upp i två tabeller

transactions
Innehåller övergripande information om varje transaktion som datum, tid och totalbelopp.

transaction_details
Innehåller detaljerad information om produkter och antal per transaktion.

Detta gör datan mer strukturerad och lättare att analysera vidare.

Lagring

Den transformerade datan lagras i en SQLite databas.
Vid varje körning filtreras endast nya transaktioner in i databasen för att undvika dubbletter.

Visualisering

För att analysera och presentera datan används Seaborn och Matplotlib.
Visualiseringarna används för att identifiera mönster, spridning och samband i datan.

Exempel på visualiseringar i projektet
Fördelning av transaktionsbelopp
Spridning av belopp för att identifiera avvikande värden
Heatmap över antal transaktioner per dag och timme
Samband mellan antal produkter och totalbelopp

Visualiseringarna är valda för att vara lättförståeliga och tydliga i en presentation.

Syfte och slutsats

Projektets syfte är att visa förståelse för hela dataflödet från rådata till analys.
Resultatet visar hur data kan struktureras och visualiseras för att ge insikter om beteenden och mönster i transaktionsdata.

Projektet är främst fokuserat på datakvalitet, struktur och tydlig presentation.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

README version English
Project overview

This project is a data pipeline project where transaction data is collected, processed, stored and visualized. The main goal is to demonstrate the full workflow from raw data to insights, with a focus on clarity and data quality rather than advanced technical complexity.

The project consists of three main parts
Data ingestion
Data transformation and storage
Data visualization and presentation

Data source

The dataset consists of transaction data in CSV format containing information such as
Transaction id
Timestamp
Amount
Products and quantities

The data is automatically retrieved through an ETL process.

Data processing

After ingestion, the data is processed using Python.
The raw data is normalized and split into two tables

transactions
Contains high level information about each transaction such as date, time and total amount.

transaction_details
Contains detailed information about products and quantities per transaction.

This structure improves data consistency and makes further analysis easier.

Storage

The transformed data is stored in a SQLite database.
Each pipeline run filters and inserts only new transactions to avoid duplicates.

Visualization

Seaborn and Matplotlib are used to analyze and present the data.
The visualizations are designed to highlight patterns, distributions and relationships in the dataset.

Examples of visualizations included in the project
Distribution of transaction amounts
Spread of amounts to highlight potential outliers
Heatmap showing transaction activity by day and hour
Relationship between number of items and total transaction amount

The visualizations are intentionally kept simple and presentation friendly.

Purpose and conclusion

The purpose of this project is to demonstrate an understanding of the complete data workflow from raw data to analysis.
The results show how structured data and visualization can provide insights into transaction behavior and trends.

The project focuses on data structure, quality and clear communication of results.
