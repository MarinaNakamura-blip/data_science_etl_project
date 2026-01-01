# data_science_etl_project
School group work where we created an etl transformation into airflow.

Beskrivning
Detta är ett skolprojekt där vi har byggt en ETL pipeline som hämtar transaktionsdata,
transformerar den till en normaliserad struktur och lagrar resultatet i en SQLite databas.
Projektet innehåller även visualiseringar skapade med Seaborn.
 
ETL flöde
1 Extract: CSV data hämtas från extern källa
2 Transform: Data delas upp i transactions och transaction_details
3 Load: Data lagras i SQLite databas
 
Projektstruktur
src innehåller all python kod för ETL och visualisering
data innehåller eventuella lokala datafiler
reports innehåller sparade visualiseringar
 
Hur man kör projektet
1 Installera beroenden
   uv sync
2 Kör ETL pipeline
   uv run python src/etl.py
 
Visualiseringar
Projektet innehåller bland annat:
- fördelning av transaktionsbelopp
- spridning av transaktioner
- heatmap över transaktioner per dag och timme
