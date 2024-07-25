# Studying the Impact of a TE Premium
*Fantasy football allows users to thrive on the thrill of strategic decision-making and enriches our NFL viewing experience. However, a recurring issue in not just my league but other leagues is the underwhelming impact of tight ends on the overall scoring system. In our setup, tight ends seem to contribute minimally compared to other positions (unless you have someone like Travis Kelce), leading to less excitement and strategic depth in managing this position. This imbalance diminishes the overall fantasy football experience, as participants feel less incentive to invest time and resources in drafting or trading for tight ends. In this project, I hope to make an informed recommendation to our league commissioner on whether a "TE premium" scoring system would be beneficial.*

## Project Overview
This project focuses on working within a comprehensive data architecture to leverage the Sleeper API for extracting and analyzing fantasy football data. The primary goal is to design a robust data architecture that supports the exploratory analysis of player statistics, team performances, and league transactions. More importantly, this analysis aims to evaluate the potential benefits of implementing a TE premium, in the interest of enhancing competitive balance and enriching the overall fantasy football experience for future seasons. 

By constructing and operating within this data architecture, the project demonstrates key data engineering concepts such as data integration, ETL processes (ELT in this case), data governance, and data security. It showcases my ability to design efficient data pipelines, manage data storage, and ensure data quality and accessibility. The insights derived from the analysis not only inform strategic decisions and league set-up recommendations for those in my fantasy football league but also highlight my proficiency in handling complex data environments.

## Technologies Used
- Programming Languages: Python, SQL
- Libraries: Pandas, PySpark
- Tools: Databricks, Tableau, and other Azure Services

## The Source of our Data
The data that we used was sourced from Sleeper's API. Its various endpoints allow us to access data, such as league information, player stats, user data, and more. While no API token is necessary, it is important to be aware of the frequency of API calls. The documentation states, as a general rule, that we should stay under 1000 calls per minute to prevent risk of getting your IP blocked. During this project however, there were no scripts that required such frequency of calls.

For more information about this API, check out the [Sleeper API documentation](https://docs.sleeper.com/).

## Understanding our Pipeline
An overwhelming majority of our ELT process will be done on Azure Databricks, an Apache Spark-based analytics platform optimized for Microsoft Azure. It's an amazing environment for teams of data engineers, data analysts, and data scientists to collaborate on Big Data projects. I chose Databricks to become familiar with a Big Data analytics platform and for its substantial market share as a primary tool in organizations' data architecture.

Additionally, I familiarize myself with Microsoft Azure services, a worthwhile process as more businesses and organizations transition to a cloud ecosystem.

Our ELT process begins with the Sleeper API. We use Python's requests library to make calls to various endpoints of the API and store its JSON information into the raw container of our Azure Data Lake Storage. Leveraging PySpark, we then process our data through DataFrames. This involves ingesting our JSON files into DataFrames and general data cleaning. More importantly, we store and arrange this processed data (as parquet) in an easy-to-navigate filing system within our processed container. Finally, we make the appropriate transformations for our presentation layer. This involves sorting, aggregating, and even normalization in 3NF.

In the next phase, we put on our data analyst hat and analyze and visualize our fantasy data. We convert our parquet files from our presentation container into tables that are stored in a database in Databricks' file system. Using SQL, we filter and join relevant information in order to smoothly conduct our analysis. And finally, we use Tableau to visualize our data to communicate our findings effectively to our fellow non-technical fantasy users.

Here is a data flow diagram that summarizes our pipeline:
![SleeperProject-DFD](https://github.com/user-attachments/assets/c34e5c74-09ef-4720-913f-4282590a3352)
*This flow chart was created using [Lucidchart](https://www.lucidchart.com/pages/?).*

And for analysts, here is our database structure:
