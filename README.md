# S_and_P_500_Analysis_using_apache_spark

This research initiative undertakes an exhaustive analysis and visualization of stock market data and company attributes utilizing two primary datasets, namely "stock_details_5_years.csv" and "constituentsfinal.csv". The analysis framework encapsulates a multifaceted exploration, including:
- Sector-wise risk assessment
- Market dominance investigation
- Dividend stock identification
- Correlation analysis between company age and performance
- Yearly performance evaluations across sectors
- Stock split impact assessment
- Analysis of price movement patterns
- Geographical influences on key metrics
- Seasonality patterns
- Lag analysis

The culmination of these analyses yields comprehensive insights into the intricate dynamics of stock market behavior, company operational paradigms, and prevailing industry trends. These findings serve as a pivotal resource offering invaluable guidance for devising astute investment strategies, fortifying decision-making processes, and fostering a deeper comprehension of the intricate and nuanced landscapes within the domains of finance and corporate ecosystems.

## Requirements
- Apache Spark
- Scala (version 2.12.18)
- Python (libraries: pandas, matplotlib.pyplot, numpy)
- MySql

## Folder Structure
- BDMS Spark Analysis (Scala project file)
  - built.sbt (to install required libraryDependencies)
  - src (source files for the Scala project)
    - main -> scala -> finalproject.sc (Scala code with all analyses)

- Datasets
  - constituentsfinal.csv
  - stock_details_5_years.csv

- Python Visualization
  - Contains all CSV files generated from Spark analysis
  - finalplot.ipynb (Contains visualizations)

## How to Run the Code
1. Load the dataset into MySQL.
2. Build the project in an IDE (e.g., IntelliJ) and use sbt to install required libraryDependencies like spark-core (version 3.5.0), spark-sql (version 3.5.0), and mysql-connector-java (version 8.0.33).
3. Connect MySQL and Apache Spark using JDBC.
4. Change the CSV file paths in the analysis and visualizations as necessary.


## Contact 
- aringhorui@icloud.com

