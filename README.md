# Studying the Impact of a TE Premium
<img src="https://github.com/user-attachments/assets/5f32c890-ae2b-45a5-b36c-ed348bc59a2c" alt="travis-kelce-resized" align="right" style="margin-right: 10px;" width="200"/>

*Fantasy football allows NFL fans to thrive on the thrill of strategic decision-making and enriches our NFL viewing experience. However, a recurring issue in not just my league but other leagues is the underwhelming impact of tight ends on the overall scoring system. In our setup, tight ends seem to contribute minimally compared to other positions (unless you have someone like Travis Kelce), leading to less excitement and strategic depth in managing this position. This imbalance diminishes the overall fantasy football experience, as participants feel less incentive to invest time and resources in drafting or trading for tight ends. In this project, I hope to make an informed recommendation to our league commissioner on whether a "TE premium" scoring system would be beneficial.*
<div style="clear: both;"></div>

## Table of Contents
#### 1. [Project Overview](#project-overview)
#### 2. [Technologies Used](#technologies-used)
#### 3. [The Source of our Data](#the-source-of-our-data)
#### 4. [Understanding our Pipeline](#understanding-our-pipeline)
- [Overview of the Architecture](#overview-of-the-architecture)
- [Summary of Project Steps](#summary-of-project-steps)
#### 5. [Phase One: Project Set-Up](#phase-one-project-set-up)
- [Creating our Resources in the Azure Portal](#i-creating-our-resources-in-the-azure-portal)
- [Getting Started in Databricks: The Set-Up Folder](#ii-getting-started-in-databricks-the-set-up-folder)
#### 6. [Phase Two: Designing our Pipeline](#phase-two-designing-our-pipeline)
- [Extraction](#i-extraction)
- [Ingestion and Transformation](#ii-ingestion-and-transformation)
- [Fine Tuning and Normalizing Transactions](#iii-fine-tuning-and-normalizing-transactions)
- [Load](#iv-load)
#### 7. [Phase Three: Analyzing the Data](#phase-three-analyzing-the-data)
#### 8. [Conclusion](#conclusion)

## Project Overview
This project focuses on working within a comprehensive data architecture to leverage the Sleeper API for extracting and analyzing fantasy football data. The primary goal is to design a robust data architecture that supports the exploratory analysis of player statistics, team performances, and league transactions. More importantly, this analysis aims to evaluate the potential benefits of implementing a TE premium, in the interest of enhancing competitive balance and enriching the overall fantasy football experience for future seasons. 

In addition, this project demonstrates key data engineering concepts such as data integration, ETL processes, data governance, and data security. It showcases my ability to design efficient data pipelines, manage data storage, and ensure data quality and accessibility. The insights derived from the analysis not only inform strategic decisions and league set-up recommendations for those in my fantasy football league but also highlight my proficiency in working within a complex data environment.

## Technologies Used
<img src="https://github.com/user-attachments/assets/ee5ba8ed-1e07-48c4-9df4-6c905d83168a" alt="python-sql-tableau" align="right" style="margin-right: 10px;" width="180">

- Programming Languages: Python, SQL
- Libraries: Pandas, PySpark
- Tools: Databricks, Tableau, and other Azure Services

## The Source of our Data
<img src="https://github.com/user-attachments/assets/274d9339-cbfc-41f0-a91a-adc097cf5ea4" alt="sleeper-logo" align="left" style="margin-right: 10px;" width="150">

The data that we used was sourced from Sleeper's API. Its various endpoints allow us to access data, such as league information, player stats, user data, and more. While no API token is necessary, it is important to be aware of the frequency of API calls. The documentation states, as a general rule, that we should stay under 1000 calls per minute to prevent risk of getting your IP blocked. During this project however, there were no scripts that required such frequency of calls.

For more information about this API, check out the [Sleeper API documentation](https://docs.sleeper.com/).

## Understanding our Pipeline
### Overview of the Architecture
An overwhelming majority of our ETL process will be done on Azure Databricks, an Apache Spark-based analytics platform optimized for Microsoft Azure. It's an amazing environment for teams of data engineers, data analysts, and data scientists to collaborate on Big Data projects. I chose Databricks to become familiar with a Big Data analytics platform and for its substantial market share as a primary tool in organizations' data architecture. Additionally, I wanted to familiarize myself with Microsoft Azure services, a worthwhile venture as more businesses and organizations transition to a cloud ecosystem.

One key feature of this pipeline is the use of the "Medallion Architecture". It is a data architecture pattern designed to organize data into layers with increasing levels of refinement and quality. This architecture is particularly beneficial for managing large volumes of data, ensuring data quality, and making data accessible and usable for various analytical purposes. Our data is organized into three main layers are Bronze (raw), Silver (processed), and Gold (presentation). You can learn more about it on the databrick website [here](https://www.databricks.com/glossary/medallion-architecture).

### Summary of Project Steps
Our ETL process begins with the Sleeper API. We use Python's requests library to make calls to various endpoints of the API and store its JSON information into the raw container of our Azure Data Lake Storage. Leveraging PySpark, we then process our data through DataFrames. This involves ingesting our JSON files into DataFrames and general data cleaning. More importantly, we store and arrange this processed data (as parquet) in an easy-to-navigate filing system within our processed container. Finally, we make the appropriate transformations for our presentation layer. This involves sorting, aggregating, and even normalization in 3NF.

In this next phase, we put on our data analyst hat and analyze and visualize our fantasy data. We load our parquet files from our presentation container into tables that are stored in a database in Databricks' file system. Using SQL, we filter and join relevant information in order to smoothly conduct our analysis. And finally, we use Tableau to visualize our data to communicate our findings effectively to our fellow non-technical fantasy users.

Here is a data flow diagram that summarizes our pipeline:
![SleeperProject-DFD](https://github.com/user-attachments/assets/63760a43-4ee8-419c-9d86-53cf653312ee)
*This flow chart was created using [Lucidchart](https://www.lucidchart.com/).*

And for analysts, here is the database schema that I've created:
![sleeper_schema](https://github.com/user-attachments/assets/233554ff-177e-416d-9361-1055c4701bc1)
*This diagram was created using [dbdiagram.io](https://dbdiagram.io/)*

Later on in our project, we'll create a couple more tables (i.e. `standings` and `matchups`) for presentation.

## Phase One: Project Set-Up
### I. Creating our Resources in the Azure Portal
Our project begins in the Azure portal where I create all my instances of the necessary Azure services. I will go in detail the resources that I've created.

#### Azure Databricks
As I've described briefly before, Databricks is an integrated analytics platform that simplifies big data and AI workloads. It provides an environment for data professionals to collaborate and perform tasks, such as ingestion, transformation, and analysis.

#### Storage Account (ADLS)
A data lake is best used for structure and unstructured data and allows for diverse formats, such as files, images, videos, and more. Azure provides a data lake that is created when setting up a storage account in the Azure portal. Upon set-up, we created our raw, processed, and presentation containers. Within our Databricks workspace, we will be accessing data stored in this data lake. 

#### Service Principal
The service principal is the entity that accesses certain Azure resources. Using a service principal is ideal for organizations/projects dealing with classified or confidential data as it is more secure and provides granular permissions through role-based access control. When configuring our data lake in the Azure portal, we assigned the "Storage Blob Data Contributor" role to our Service Principal. This role grants read, write, and delete access to Azure Storage blob containers and data, including the raw, processed, and presentation containers we created. While fantasy football data doesn't require such an authentication method, I personally want to emulate working within an environment that deals with secure data. 

#### Key Vault
Azure Key Vault securely stores and manages sensitive information, such as secrets, keys, and certificates. This is where the credentials to our service principal will be found. Our Databricks workspace will access the key vault to retrieve the credentials needed to authenticate the Service Principal, which enables access to the fantasy football data in our data lake. 

Here is a relationship diagram of all the resources in our scenario (also created using [Lucidchart](https://www.lucidchart.com)):
![Azure Resource Relationship Diagram for Sleeper Project](https://github.com/user-attachments/assets/b00456f8-7d87-48ae-8bf1-d5d5452bc20d)

In summary, the Service Principal acts like a trusted person, or identity, that provides access to a highly secure storage location (the data lake). We can interact with this person from our Databricks workspace, but only if we use a secret password that can be found in a secure book of secrets (our Key Vault). I hope this simplifies the understanding of the relationship between all the resources. If you want more information on Azure resources, you can read documentation and tutorials [here](https://learn.microsoft.com/en-us/azure/?product=popular). YouTube and Udemy are also great resources.

### II. Getting Started in Databricks: The Set-Up Folder
Inside our set-up folder of our workspace, we must define any global variables and mount our ADLS containers. The only global variables we are defining are the `CURRENT_LEAGUE_ID` and `ALL_SEASONS` variables.

#### Global Variables
<img align="right" width="230" height="280" src="https://github.com/user-attachments/assets/aabca3eb-e0f2-4bc2-86de-8f71a1098c65">

It is important to note that each season of your fantasy football league will have its own separate league ID. This is because Sleeper defines a league as a unique season for the group of users participating in it. When a new season begins, a new league ID is generated. The figure on the right (from the [Sleeper API documentation](https://docs.sleeper.com/)) shows there are different endpoints that provide access to various kinds of information, but only for a specific "league" or season. Therefore, you need to provide a specific league ID when making a GET request for information such as transactions. If you need information from other seasons, you must make additional requests using their respective league IDs.

Considering what we just discussed, we're going to eventually make a lot of calls to retrieve different kinds of information for each league ID in our league's history. As you navigate through the current and past seasons of your league on the Sleeper website, you'll notice that the league ID in the URL will change. We can manually store these via copy and paste inside a Python data structure, but that would take a lot of work, especially if your league's history extends to many past seasons.

To automate this process, we use the endpoint that retrieves a specific league. The URL for it is:

`https://api.sleeper.app/v1/league/<league_id>`

By replacing `<league_id>` with our current league ID and examining the JSON response, we find a `previous_league_id` key. With code, we can iterate through these `previous_league_id` values, making multiple API calls until we encounter a `None` value, which indicates we have reached the very first season. At each iteration, we will record the year of the season, found in `season`, and the value of `league_id`. Then, use the value of `previous_league_id` to make the next call.

At the end of the task, all this information should be stored in a Python dictionary where the key is the year of the season, and the value is the corresponding league ID. The dictionary should look like this:
```
{
  2024: 1048353521130741760,
  2023: 917263521006592000,
  2022: 833585949995814912
}
```
These will be the league IDs for my Sleeper league, but yours will look different. Let's now create a new notebook in our set-up folder to create the function that does exactly what we just described. Make sure to import the `requests` library, which allows you to send HTTP requests easily.

Here is the function that will complete our tasks:
```python
def generate_league_history(current_league_id: int) -> dict:
  if not isinstance(current_league_id, int):
    raise TypeError("current_league_id must be an integer.")

  league_id = current_league_id
  league_history = {}

  try:
    # Make a HTTP request to sleeper API
    response = requests.get(f"https://api.sleeper.app/v1/league/{league_id}")
    
    # Parse the JSON response; The .json() method returns a Python dictionary
    data = response.json()
    
    # Add the current league_id to the dictionary
    league_history[int(data['season'])] = int(data['league_id'])
    
    # Keep adding league IDs to the dictionary until we reach the first season in the history
    while data['previous_league_id'] is not None:
    
      # Make another HTTP request to get previous season's information
      league_id = data['previous_league_id']
      response = requests.get(f"https://api.sleeper.app/v1/league/{league_id}")
      data = response.json()
    
      league_history[int(data['season'])] = int(data['league_id'])
  
  except Exception as e:
    print(f"An error occurred while generating league history: {e}")
    return {}
  
  return league_history
```
Then we assign our variables `CURRENT_LEAGUE_ID` and `ALL_SEASONS`.
```
CURRENT_LEAGUE_ID = 1048353521130741760
ALL_SEASONS = generate_league_history(CURRENT_LEAGUE_ID)
```
After attaching the notebook to a cluster, run the code and make sure to print `ALL_SEASONS` to check your results. Our first notebook is complete! You can check the full code [here](https://github.com/TeamPete/SleeperProject/blob/main/set-up/global_variables.ipynb). I will make sure to link more notebooks as we progress further into the project steps.

#### Mounting ADLS Containers
Databricks has a secure storage mechanism for managing sensitive information called the **secret scope**. We must create one and link it with our Key Vault, accessing the credentials. 

When creating a new notebook, our first step is to assign those credentials to our variables. We use the **dbutils** library to access our secret scope.
```python
client_id = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-client-id')
tenant_id = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-tenant-id')
client_secret = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-client-secret')
```

Afterwards, we must set up our mount point. Microsoft Learn actually shows us how to create a mount point in our notebook. Please refer [here](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts) to see how to create a mount point.

The first step is to set up the Spark configuration.
```python
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
```

Then use **dbutils** to mount the container.
```python
# Mount the storage account container
dbutils.fs.mount(
    source = f"abfss://{<container_name>}@{<storage_account>}.dfs.core.windows.net/",
    mount_point = <mount_point>,
    extra_configs = configs)
```

For example, if we mounted our "raw" container, our mount point would be "/mnt/sleeperprojectdl/raw".

In our notebook, we encapsulate this process in a function and use a for loop to iterate through a list of container names, mounting each container in turn. 
```python
def mount_adls(storage_account, container_name):
    # Get secrets from Azure Key Vault
    client_id = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-client-id')
    tenant_id = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-tenant-id')
    client_secret = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-client-secret')

    # Set Spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Create mount point
    mount_point = f"/mnt/{storage_account}/{container_name}"
    
    # Check if mount point already exists
    existing_mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]

    if mount_point in existing_mounts:
        # Unmount the existing mount point
        dbutils.fs.unmount(mount_point)
        print(f"Unmounted previously existing mount at {mount_point}")
    
    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
        mount_point = mount_point,
        extra_configs = configs)
    
    print(f"Successfully mounted abfss://{container_name}@{storage_account}.dfs.core.windows.net/ to {mount_point}")
```

In addition, I did some conditional checking to see whether the mount point already exists in **dbutils.fs.mounts()** and had messages prepared so that we knew what issues we ran into in case of an unsuccessful mount.

Now, we just need to mount each of our containers.
```python
# Mounting containers
storage_account = 'sleeperprojectdl'
containers_for_project = ['raw', 'processed', 'presentation']

for container in containers_for_project:
    mount_adls(storage_account, container)
```

We have finished! The entire notebook for this can be found [here](https://github.com/TeamPete/SleeperProject/blob/main/set-up/mount_adls_containers.ipynb).
## Phase Two: Designing our Pipeline
### I. Extraction

Our pipeline begins with extraction, which uses the **requests** library to make GET requests to Sleeper's API. We save the responses as JSON files in our "raw" container. The extraction process is divided into two notebooks: one for creating the extraction class, where each method calls a different endpoint of the Sleeper API, and another for creating an instance of this class and executing all necessary methods.

#### The `SeasonExtractor` Class
We begin by writing the __init__ method (constructor) for our `SeasonExtractor` class, which accepts `league_id` and `year` as parameters. This method also initializes additional attributes, such as `max_week` (set to 17) and `draft_id`. These attributes are crucial because they will be included in the URLs of the API endpoints we will be using.

```python
class SeasonExtractor:
    def __init__(self, league_id: int, year: int):
        current_year = datetime.now().year
        
        # Enforce the integer type of league_id.
        if not isinstance(league_id, int) or not isinstance(league_id, int) or year > current_year or year < 2017:
            raise TypeError("You must pass valid league_id and year values")

        self.league_id = league_id
        self.year = year
        self.max_week = 17
        self.draft_id = None

        try:
            url = f"https://api.sleeper.app/v1/league/{self.league_id}"
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            print(f"An error occurred while creating a SeasonExtractor instance: {e}")

        if response.json()['status'] == 'in_season':
            self.max_week = response.json()['settings']['leg'] - 1
        
        self.draft_id = response.json()['draft_id']
```

In this __init__ method, I've created checks to enforce the integer type of `league_id` and a valid `year`. Additionally, we make a call to the Sleeper API. This particular URL gives us basic information of a specific league. We will want to find the `draft_id` since we will need it.

Throughout the class notebook, I define methods for each endpoint and store the response as JSON in the "raw" container. In the example below, I am getting matchup information. In this particular URL however, I must specify which week. To get matchups for all the weeks, I use a for loop to iterate through all possible weeks, making a different call at each turn.
```python
def extract_matchups(self):
    print(f"\nExtracting {self.year} matchups...")

    for week in range(1, self.max_week + 1):
        try:
            url = f"https://api.sleeper.app/v1/league/{self.league_id}/matchups/{week}"
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            print(f"An error occurred while fetching {self.year} regular season matchups: {e}")

        json_content = json.dumps(response.json(), indent=4)
        file_path = f"/mnt/sleeperprojectdl/raw/{self.year}/matchups/week_{week}.json"
            
        dbutils.fs.put(file_path, json_content, overwrite=True)
        print(f"Successfully wrote {self.year} week {week} matchups to /mnt/sleeperprojectdl/raw")
```

As you can see, we save the file as JSON using the mount point we had created.

I won't go over every method, but the rest of the methods extract basic league information, transactions, rosters, users, and draft results. If you want to see the full code of the `SeasonExtractor` class and how I define each method, click [here](https://github.com/TeamPete/SleeperProject/blob/main/1_extraction/season_extractor_class.ipynb).

#### Executing Extraction
In this `extraction_main` notebook, we define the function `ExtractSleeperAPI()` that passes in an instance of the `SeasonExtractor` class and calls all the methods we just recently defined.
```python
def ExtractSleeperAPI(instance: SeasonExtractor):
    print(f"Initiating extraction for the {instance.year} season...")

    instance.extract_league_info()
    instance.extract_rosters()
    instance.extract_users()
    instance.extract_matchups()
    instance.extract_transactions()
    instance.extract_draft_picks()

    print(f"\nExtraction for the {instance.year} season completed.")
```
In a for loop that iterates through the `ALL_SEASONS` global variable, we will call that function for each `league_id` possible. Remember, one instance of our `SeasonExtractor` class only extracts data for one `league_id` or season.

```python
for season, league_id in ALL_SEASONS.items():
    instance = SeasonExtractor(league_id, season)
    ExtractSleeperAPI(instance)
```

It's important to remember to use the `%run` magic command in the beginning of this notebook to reference `ALL_SEASONS` and the `SeasonExtractor` class defined in other notebooks. See the full code of this notebook [here](https://github.com/TeamPete/SleeperProject/blob/main/1_extraction/extraction_main.ipynb).

#### Extracting NFL Player Data
In this Python script I wrote in VSCode, I simply retrieve, process, and filter NFL player data from the Sleeper API. I save this data in a csv file and uploaded it onto the data lake storage in the "raw" container. I wanted to do this in my Databricks environment, but for some reason I couldn't run the extraction with this particular Sleeper API endpoint. I suspect it had to do with the limitations of the cluster I had attached, so I moved this part of the process onto my IDE. Despite this challenge, I'll be glad to demonstrate my knowledge of pandas and working with a csv file format.

The function `get_playerinfo()` is defined to fetch NFL player data from the Sleeper API. The data is returned as a Python dictionary. If the API request fails, the function handles the exception and returns an empty dictionary.
```python
def get_playerinfo():
    url = "https://api.sleeper.app/v1/players/nfl"

    try:
        response = requests.get(url)
        response.raise_for_status
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"There was an error when fetching roster data from your league: {e}")
        return {}
```

The function `filter_playerinfo()` function processes the fetched data, filtering it to include only offensive players (quarterbacks, wide receivers, tight ends, and running backs). It selects relevant player attributes such as ID, name, position, team, experience, age, height, weight, college, and status. The filtered data is returned as a Pandas DataFrame.
```python
def filter_playerinfo(data):
    players = []
    offense = ['QB', 'WR', 'TE', 'RB']

    # We only want to select the relevant fields
    for key, value in data.items():
        try:
            # Only filter for fantasy relevant players
            if value.get('position') in offense:
                players.append({
                    'player_id': int(key),
                    'name': value.get('full_name'),
                    'position': value.get('position'),
                    'team': value.get('team'),
                    'exp': value.get('years_exp'),
                    'age': value.get('age'),
                    'height': value.get('height'),
                    'weight': value.get('weight'),
                    'college': value.get('college'),
                    'status': value.get('status')
                })

        except (ValueError, TypeError) as e:
            # Handle conversion error or missing value
            print(f"Error processing player {key}: {e}")

    return pd.DataFrame(players)
```

The `convert_to_inches()` function converts a player's height from feet and inches (e.g., 6'2") into total inches. It uses regular expressions to parse the height format and calculates the total height in inches. If the height is not in the expected format, an error is raised.
```python
def convert_to_inches(height):
    if "'" in height:
        # If height is in feet and inches format, create match object
        match_obj = re.match(r"(\d+)'(\d+)\"", height)

        if match_obj:
            feet = int(match_obj.group(1))
            inches = int(match_obj.group(2))
            return feet * 12 + inches
        else:
            raise ValueError("Invalid height format")
    else:
        return height
```

We bring all those functions together by calling `get_playerinfo()` to fetch the player data and then filter it using `filter_playerinfo()`. The height of each player is then converted to inches using the `convert_to_inches()` function. Finally, the processed data is saved as a CSV file named nfl_players.csv.
```python
response_as_dict = get_playerinfo()
players_df = filter_playerinfo(response_as_dict)
players_df['height'] = players_df['height'].apply(convert_to_inches)

players_df.to_csv("nfl_players.csv", index=False)
```

To access this script, you can find it [here](https://github.com/TeamPete/SleeperProject/blob/main/1_extraction/sleeper_player_info.py).

### II. Ingestion and Transformation
<img src="https://github.com/user-attachments/assets/81f4b922-f6a7-4fdc-a423-3d6a347875f5" alt="sleeper-logo" align="right" style="margin-right: 10px;" width="200">

In the ingestion folder, each notebook will be responsible for handling a file or a set of files depending on how we want to organize our data. We will finally leverage Spark to read and clean our data in this section. The basic approach will be the same across all notebooks in this folder and is as follows:
1. Import the required libraries
2. Define the schema
3. Read the file as a DataFrame
4. Clean the data
5. Save the data to our "processed" container as a parquet file

I will go over two notebooks to demonstrate this process: [draft_picks](https://github.com/TeamPete/SleeperProject/blob/main/2_ingestion/draft_picks.ipynb) and [player_performances](https://github.com/TeamPete/SleeperProject/blob/main/2_ingestion/player_performances.ipynb). For the rest of the notebooks in the ingestion folder, you can click [here](https://github.com/TeamPete/SleeperProject/tree/main/2_ingestion).

#### Ingesting Draft Picks
Let's look at the "draft_picks" notebook. To start off, we import the data types from the `pyspark.sql.types` module. This module provides classes that define the data types used in Spark DataFrames. You use these classes when you need to define or enforce a specific schema for your DataFrame, ensuring that each column has a specific data type.

Additionally, the module `pyspark.sql.functions` provides a collection of built-in functions that you can use to manipulate data within Spark DataFrames.
```
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DecimalType, FloatType, IntegerType
from pyspark.sql.functions import col, current_timestamp, lit, concat, when, explode
import json
```

The next step is to observe the "draft_picks" JSON file from my "raw" container. I'm looking at the fields I'm ingesting and what data types I need to enforce in my schema.
```
[
    {
        "round": 1,
        "roster_id": 2,
        "player_id": "9509",
        "picked_by": "450908656402165760",
        "pick_no": 1,
        "metadata": {
            "years_exp": "0",
            "team": "ATL",
            "status": "Active",
            "sport": "nfl",
            "position": "RB",
            "player_id": "9509",
            "number": "7",
            "news_updated": "1682644810114",
            "last_name": "Robinson",
            "injury_status": "",
            "first_name": "Bijan"
        },
        "is_keeper": null,
        "draft_slot": 1,
        "draft_id": "917263521006592001"
    },
    {
        "round": 1,
        "roster_id": 5,
        "player_id": "9228",
        "picked_by": "833594957678886912",
        "pick_no": 2,
        "metadata": {
            "years_exp": "0",
            "team": "CAR",
            "status": "Active",
            "sport": "nfl",
            "position": "QB",
            "player_id": "9228",
            "number": "9",
            "news_updated": "1682967935354",
            "last_name": "Young",
            "injury_status": "",
            "first_name": "Bryce"
        },
        "is_keeper": null,
        "draft_slot": 2,
        "draft_id": "917263521006592001"
    },...
```

So now I understand what my schema is, I will write my schema definition below. Look at `draft_picks_schema` so you can see how to define a schema for a Spark DataFrame:
```python
# In order use dbutils.fs.head, we must make sure our file isn't too large.
# We use 2023's draft_picks.json since the start-up draft file from 2022 would've been too large.
draft_picks_as_string = dbutils.fs.head("/mnt/sleeperprojectdl/raw/2023/draft_picks.json")
draft_picks_as_json = json.loads(draft_picks_as_string)

metadata_schema = StructType([StructField(item, StringType(), True) for item in draft_picks_as_json[0]['metadata'].keys()]
    + [StructField('amount', StringType(), True)])

draft_picks_schema = StructType([
    StructField('round', IntegerType(), True),
    StructField('roster_id', IntegerType(), True),
    StructField('player_id', StringType(), True),
    StructField('picked_by', DecimalType(38,0), True),
    StructField('pick_no', IntegerType(), True),
    StructField('metadata', metadata_schema, True),
    StructField('is_keeper', BooleanType(), True),
    StructField('draft_slot', IntegerType(), True),
    StructField('draft_id', DecimalType(38,0), True)
])
```

Notice how I have a nested dictionary of additional fields under "metadata". I decided that instead of tediously writing each field for "metadata" for `metadata_schema`, I used a list comprehension to dynamically create a list of StructField objects. This is done based on the keys found in the "metadata" field of a single element in `draft_picks_as_json`. This time-saving technique can be found in other notebooks of the ingestion folder.

The next step is to actually read the JSON file into a Spark DataFrame. In the "raw" container, it is important to remember that there are folders for each season, so I will need to read multiple draft_picks.json files. I use a for loop once again to do this process.
```python
for season in ALL_SEASONS.keys():
    # Set the mount point as our file path; use formatted string to dynamically set the season
    file_path = f"/mnt/sleeperprojectdl/raw/{season}/draft_picks.json"

    # Read the json file and apply the schema
    draft_picks_df = spark.read.json(file_path, schema=draft_picks_schema, multiLine=True)

    # Clean and write the data to the processed folder
    draft_picks_final_df = draft_picks_df.select(
            col('pick_no'),
            col('player_id'),
            col('picked_by'),
            col('metadata.amount')
        ) \
        .withColumn('season', lit(season)) \
        .withColumn('player_id', col('player_id').cast('integer')) \
        .withColumn('amount', col('amount').cast('integer')) \
        .withColumn('ingestion_date', current_timestamp())

    draft_picks_final_df.write.mode('append').parquet(f"/mnt/sleeperprojectdl/processed/draft_picks")
```

In the final DataFrame, I only select relevant columns I want in my draft picks table and did some type conversions. I also added an ingestion date to indicate when the data was processed. When I finally wrote it to the "processed" container, I made sure to set the mode to "append" because I want all my draft picks data in one single parquet file. Here is what the final DataFrame looks like once you display it:
![Screenshot 2024-08-14 124030](https://github.com/user-attachments/assets/b5a31ca1-ddc7-4a02-afb6-55924c0d2a2f)

#### Ingesting Matchups
The matchup files in our "raw" container are organized by week. While it's possible to ingest all files in a single line of code, doing so prevents us from capturing the specific week as a new column in the dataset, which is why we'll need to use a loop to ingest each file individually.

When importing the necessary libraries again, we introduce the **MapType** and **ArrayType** data types in Spark, which are used to define schemas that include dictionary-like structures and arrays, respectively. Here is how the schema is defined:
```python
matchups_schema = StructType([
    StructField("players_points", MapType(StringType(), FloatType()), True),
    StructField("starters_points", ArrayType(FloatType()), True),
    StructField("starters", ArrayType(StringType()), True),
    StructField("matchup_id", IntegerType(), True),
    StructField("custom_points", FloatType(), True),
    StructField("roster_id", IntegerType(), True),
    StructField("players", ArrayType(StringType()), True),
    StructField("points", FloatType(), True)
])
``` 
Now, let's take a look at a sample week of matchups as a DataFrame:
![Screenshot 2024-08-14 130809](https://github.com/user-attachments/assets/eae75444-b814-487f-95d4-198e53708a23)

There’s a lot to consider here! We need to determine the structure of our final DataFrame. Should we create multiple final DataFrames, or focus on just one? What data is essential, and what can we omit? I've decided that the goal should be to create a final DataFrame that captures individual player performances, including the week, season, team, and matchup ID. This approach allows us to use aggregations later on to generate a separate matchups table with each team's total points.

I began this process with a nested for loop. By using slicing, I limited the iteration to the 2023 season, as the 2024 season hasn't started yet. The nested for loop then iterates through all the weeks within that season.
```python
for season in sorted(ALL_SEASONS.keys())[:-1]:
    for week in range(1, 18):
```

I then define the file path using a formatted string to dynamically adjust for the season and week. Then, I read the respective file.
```python
file_path = f'/mnt/sleeperprojectdl/raw/{season}/matchups/week_{week}.json'
matchups_df = spark.read.json(file_path, schema=matchups_schema, multiLine=True)
```

The `player_points` column contains dictionaries, which can be cumbersome to work with. To simplify this, I used the **explode** function to transform it into multiple rows while ensuring that `roster_id`, `matchup_id`, `week_num`, and `season` were captured alongside each entry. This is all done in a DataFrame called `players_df`. In another DataFrame called `is_starters_df`, I also exploded the `starters` and added the `is_starter` boolean column to indicate whether the player was a starter (I set the values all to True since all the rows here were starters).

Finally, I used a left join to join both `players_df` and `is_starters_df` on `player_id`.
```python
players_df = matchups_df.select(
                explode('players_points').alias('player_id', 'points'),
                col('roster_id'),
                col('matchup_id')
            ) \
            .withColumn('week_num', lit(week)) \
            .withColumn('season', lit(season))

is_starters_df = matchups_df.select(
        explode('starters').alias('player_id')
    ) \
    .withColumn('is_starter', lit(True))

joined_df = players_df.join(is_starters_df, on='player_id', how='left')
```

In `final_df`, I replaced `NULL` values in the `is_starter` column with `False`. I also added an ingestion date and reordered the columns to make the DataFrame more readable.
```python
final_df = joined_df.withColumn(
                'is_starter',
                when(col('is_starter').isNull(), lit(False)) \
                    .otherwise(True)
            ) \
            .withColumn('ingestion_date', current_timestamp())

final_df = final_df.select(
        col('season'),
        col('week_num'),
        col('matchup_id'),
        col('roster_id'),
        col('player_id'),
        col('points'),
        col('is_starter'),
        col('ingestion_date')
    ) \
    .orderBy(
        col('season'), col('week_num'), col('matchup_id'), col('roster_id'), col('is_starter').desc(), col('points').desc()
        )
```

Finally, we write this DataFrame as a parquet file to our "processed" container, appending the newly processed DataFrames to the file with each iteration of the loop.
```python
final_df.write.mode('append').parquet(f'/mnt/sleeperprojectdl/processed/player_performances')
```

Here is a preview of the final DataFrame of our processed data:
![Screenshot 2024-08-14 135808](https://github.com/user-attachments/assets/e72352c7-a20a-4549-9370-4e7584fdd35a)
### III. Fine Tuning and Normalizing Transactions
<img src="https://github.com/user-attachments/assets/7ba84a85-f955-484d-bbd1-4f21dd6f5690" alt="processed-container" align="right" style="margin-right: 10px;" width="120">

This is where I perform additional transformations and an extensive normalization process on the transactions data before writing the analysis-ready DataFrames to the "presentation" container. You can refer the transformation folder (located [here](https://github.com/TeamPete/SleeperProject/tree/main/3_transformation)) for this section. Also refer to the screenshot on the right to see the parquet files we'll be working with from our "processed" container.

For notebooks like [draft_picks](https://github.com/TeamPete/SleeperProject/blob/main/3_transformation/draft_picks(1).ipynb) and [nfl_players](https://github.com/TeamPete/SleeperProject/blob/main/3_transformation/nfl%20players.ipynb) in our transformation folder, I either renamed columns, sorted the data, or in some cases, made no changes and directly wrote to the "presentation" container. 

The main challenge was normalizing the transactions data. As we open up the transactions parquet file as a DataFrame, we see a lot of map and array data types similar to the raw matchups data we processed previously:
![Screenshot 2024-08-14 143424](https://github.com/user-attachments/assets/0a7add96-787e-4a73-924b-5727e5642cce)

I broke this table down into four relational tables: 1) A central transactions table, 2) a consenters table, 3) a roster actions table, and 4) a traded picks table. This kind of normalization will hopefully simplify queries and improve data integrity. 

#### Creating the Transactions Table
In this DataFrame, I kept what I thought were the essential columns you should find in a transactions table. I couldn't imagine these columns in a separate table. These columns include `transaction_id` (our primary key), `type`, `created`, etc. I then wrote this DataFrame to our "presentation" container.
```python
final_transactions_df = transactions_transformed_df.select(
        col("transaction_id"),
        col("type"),
        col("created"),
        col("status_updated").alias("completed"),
        col("waiver_bid"),
        col("week"),
        col("season"),
        col("ingestion_date")
    ) \
    .orderBy("completed") \
    .withColumn("type", when(col("type") == "free_agent", lit("free agent")).otherwise(col("type")))

final_transactions_df.write.mode("overwrite").parquet("/mnt/sleeperprojectdl/presentation/transactions")
```

Here is a preview of the transactions table:
![Screenshot 2024-08-15 152510](https://github.com/user-attachments/assets/eac6f9d5-cfcb-4a1b-9d89-4975f0dd4dab)

#### Creating the Consenters Table
This table will identify the teams that took part in a transaction. Many transactions only had one consenter (i.e. free agent signing) and others had multiple (i.e. trades). Part of normalization is to ensure atomic values (single numbers). Since the consenters column contains arrays, we want to explode the roster ID's in each entry.
```python
consenters_df = transactions_transformed_df.select(
        col("transaction_id"),
        explode(col("roster_ids")).alias("roster_id")

consenters_df.write.mode("overwrite").parquet("/mnt/sleeperprojectdl/presentation/consenters")
```

Here is a preview of the consenters table:
![Screenshot 2024-08-15 152651](https://github.com/user-attachments/assets/c27941a7-fc22-4d13-92fd-1c4ef28e5d0e)

#### Creating the Roster Actions Table
This table will consolidate the transaction information into a single column that indicates whether a player was "added" or "dropped." In the original transactions DataFrame, the `adds` and `drops` columns are separate and contain dictionaries where the keys represent player IDs and the values represent roster IDs. We will want to explode these values and put them together in a single column.
```python
# Exploding "adds" column
adds_df = transactions_transformed_df.select(
        col("transaction_id"),
        explode(col("adds"))
    ) \
    .withColumnsRenamed({
        "key": "player_id",
        "value": "roster_id"
    }) \
    .withColumn("action", lit("add"))

rearranged_adds_df = adds_df.select(
        col("transaction_id"),
        col("action"),
        col("roster_id"),
        col("player_id")
    ) \
    .orderBy("transaction_id")

# Exploding "drops" column
drops_df = transactions_transformed_df.select(
        col("transaction_id"),
        explode(col("drops"))
    ) \
    .withColumnsRenamed({
        "key": "player_id",
        "value": "roster_id"
    }) \
    .withColumn("action", lit("drop"))

rearranged_drops_df = drops_df.select(
        col("transaction_id"),
        col("action"),
        col("roster_id"),
        col("player_id")
    ) \
    .orderBy("transaction_id")
```

In the code above, we exploded the "adds" and "drops" in separate DataFrames and then created an `action` column. We will then concatenate these two DataFrames.
```python
rearranged_adds_df.write.mode("append").parquet("/mnt/sleeperprojectdl/presentation/roster_actions")
rearranged_drops_df.write.mode("append").parquet("/mnt/sleeperprojectdl/presentation/roster_actions")
```

I could've concatenated the two DataFrames before writing, but using append should suffice. Here's a preview of the roster actions table:
![Screenshot 2024-08-15 154435](https://github.com/user-attachments/assets/1ea6eae3-ae61-409f-ae6c-e0d154460e67)

#### Creating the Traded Draft Picks Table
We explode the `draft_picks` column and find out that it is an array of picks as dictionaries. We only select the relevant columns within the dictionary, such as `previous_owner_id`, `owner_id`, and the `round` the pick belongs to.
```python
draft_picks_df = transactions_transformed_df.select(
        col("transaction_id"),
        explode(col("draft_picks"))
    )

draft_picks_final_df = draft_picks_df.select(
    col("transaction_id"),
    col("col.previous_owner_id").alias("previous_roster_id"),
    col("col.owner_id").alias("new_roster_id"),
    col("col.season"),
    col("col.round")
)

draft_picks_final_df.write.mode("overwrite").parquet("/mnt/sleeperprojectdl/presentation/traded_picks")
```

Here is a preview of the traded draft picks table:
![Screenshot 2024-08-15 155026](https://github.com/user-attachments/assets/2954f928-0c01-4952-8e03-12a57d19da1f)

We are done with normalization! For the full notebook, click [here](https://github.com/TeamPete/SleeperProject/blob/main/3_transformation/transactions(1).ipynb).

### IV. Load
In Databricks, you can create a "database", which essentially is a logical namespace within the metastore. It groups tables together under a specific name, making it easier to manage and query related tables. We will use this database as a staging area for querying data from our presentation container.

In the "Database Creation" notebook, we create SQL tables with our presentation container:
```python
for table in dbutils.fs.ls("/mnt/sleeperprojectdl/presentation"):
    temp_df = spark.read.parquet(table.path)
    temp_df.write.format('delta').mode('overwrite').saveAsTable(f'sleeper.{table.name[:-1]}')
```

We use a for loop to iterate through all the mounts linked to our presentation container files, then write the parquet to a SQL table in our database. Here is what the full notebook looks like:
![Screenshot 2024-08-19 at 10 36 40 PM](https://github.com/user-attachments/assets/fa0f5e3a-2cc5-4adb-b436-2caea83ca97a)
![Screenshot 2024-08-19 at 10 36 49 PM](https://github.com/user-attachments/assets/23d22b1a-fd71-479f-a427-f60eb0f9cece)
**I duplicated the player performances table then manually adjusted all starting tight ends' points in 2023 to what it would be with a TE premium. I named this table as "player_scores_te_premium". We use this table for much of our analysis.**
![Screenshot 2024-08-19 at 10 36 57 PM](https://github.com/user-attachments/assets/8c27d620-e61d-4314-81d3-80c6ecb77d6b)

## Phase Three: Analyzing the Data
In this section of SQL queries, I will go do some exploratory analysis and look into what the effect of the TE premium would have had on the 2023 season.

### Generating New Tables
#### Aggregation and Cleaning of the Player Performances Table
Firstly, I deleted all the bench performances. I didn't think I'd use it to conduct any meaningful analysis.
```sql
DELETE FROM
  player_performances
WHERE
  is_starter = FALSE;
```

To make my player performances table more readable, I joined the `nfl_players` and `users` tables.
```sql
CREATE TABLE player_performances_CLEAN AS
SELECT
  p.season,
  p.week_num,
  p.matchup_id,
  u.user_name AS team,
  n.name,
  n.position,
  p.points
FROM
  player_performances p
  JOIN nfl_players n ON p.player_id = n.player_id
  JOIN users u ON p.roster_id = u.roster_id
  AND p.season = u.season
ORDER BY
  p.season,
  p.week_num,
  p.matchup_id,
  u.user_name,
  p.points DESC;
```

Here is a preview of the table we aggregated and cleaned:
![Screenshot 2024-08-20 at 4 19 03 PM](https://github.com/user-attachments/assets/dcaf09db-2a20-46a8-953e-70f68bb43b9e)

#### Generating the Matchups Table
I generated a matchups table by aggregating the data in our `player_performance sCLEAN` table along with common table expressions.
```sql
CREATE TABLE regular_season_matchups AS
WITH cte AS (
  SELECT
    p.season, p.week_num, p.matchup_id, p.team,
    ROUND(SUM(p.points), 2) AS sum_points
  FROM
    player_performances_CLEAN p
  WHERE
    p.week_num < 15
  GROUP BY
    p.season, p.week_num, p.matchup_id, p.team
  ORDER BY
    p.season, p.week_num, p.matchup_id
)
SELECT
  m1.season, m1.week_num, m1.matchup_id,
  m1.team AS winner,
  m1.sum_points AS winner_points,
  m2.team AS loser,
  m2.sum_points AS loser_points
FROM
  cte m1, cte m2
WHERE
  m1.season = m2.season
  AND m1.week_num = m2.week_num
  AND m1.matchup_id = m2.matchup_id
  AND m1.team != m2.team
  AND m1.sum_points > m2.sum_points
ORDER BY
  m1.season, m1.week_num, m1.matchup_id;
```
![Screenshot 2024-08-20 at 4 06 01 PM](https://github.com/user-attachments/assets/78f30aca-e9e7-4d6d-95c9-6779073d0ba4)

#### Generating the Standings Table
I added a `pts_for` and a `pts_against` column using CTE's.
```sql
CREATE TABLE final_standings AS (
  WITH pts_against_winners AS (
    SELECT
      season, winner AS team,
      ROUND(SUM(loser_points), 2) AS points_against
    FROM regular_season_matchups
    GROUP BY season, winner
  ),
  pts_against_losers AS (
    SELECT
      season, loser AS team,
      ROUND(SUM(winner_points), 2) AS points_against
    FROM regular_season_matchups
    GROUP BY season, loser
  ),
  final_pts_for AS (
    SELECT
      season, team,
      ROUND(SUM(points), 2) AS pts_for
    FROM
      player_performances_CLEAN
    GROUP BY
      season, team
  ),
  final_pts_against AS
  (
    SELECT
      season, team, SUM(points_against) AS pts_against
    FROM (
      SELECT * FROM pts_against_winners
      UNION ALL
      SELECT * FROM pts_against_losers
    )
    GROUP BY
      season, team
  ),
  wins_and_losses AS (
    SELECT
      season,
      winner AS team,
      COUNT(winner) AS wins,
      14 - COUNT(winner) AS losses
    FROM
      regular_season_matchups
    GROUP BY
      season, winner
    ORDER BY
      season, wins DESC
  )
  SELECT
    wl.season, wl.team, wl.wins,
    wl.losses, pf.pts_for, pa.pts_against
  FROM
    wins_and_losses wl
  JOIN
    final_pts_against pa ON
    wl.season = pa.season AND
    wl.team = pa.team 
  JOIN
    final_pts_for pf ON
    wl.season = pf.season AND
    wl.team = pf.team
  ORDER BY
    wl.season, wl.wins DESC,
    pf.pts_for DESC, pa.pts_against
);
```
![Screenshot 2024-08-20 at 4 09 18 PM](https://github.com/user-attachments/assets/132d70bf-2754-404d-a363-1372f6c60cf9)

### Exploratory Analysis
Answering some questions I have about the data.

#### Who has the most regular season wins in league history?
![Screenshot 2024-08-20 at 4 29 57 PM](https://github.com/user-attachments/assets/5b0fa056-ea68-49df-860c-896e594dbc54)

#### What were the five highest scoring matchups in the regular season?
![Screenshot 2024-08-20 at 4 30 28 PM](https://github.com/user-attachments/assets/a87bc007-6ed6-498f-a75e-82bb056ef4c6)

#### What were the five biggest blowouts in regular season? What were the five closest games?
Biggest blowouts:
![Screenshot 2024-08-20 at 4 31 00 PM](https://github.com/user-attachments/assets/0ab180ca-db57-4204-b9df-7459618d0fa3)

Closest games:
![Screenshot 2024-08-20 at 4 31 27 PM](https://github.com/user-attachments/assets/6e7dd0f5-9379-4795-a4b5-0b4a2bf4fd31)

#### What is the average contribution as a percentage of total points for a player of each position for a team in a regular season game?
![Screenshot 2024-08-20 at 4 34 08 PM](https://github.com/user-attachments/assets/b8fc4494-befb-4be1-8464-363873f25e17)
![Screenshot 2024-08-20 at 4 34 30 PM](https://github.com/user-attachments/assets/3c2f03b0-3307-4ae1-ad51-c17fa819c535)

#### Which teams have the highest QB averages in regular season games?
![Screenshot 2024-08-20 at 4 50 25 PM](https://github.com/user-attachments/assets/c0aba44a-726d-4e6c-a20f-1b3674eadfbf)

### Effects of the TE Premium
#### Distribution of TE Performances
In this query, I compare the 2023 player performances of the original scoring system to the that of the TE premium.
```sql
WITH c_te AS (
  SELECT
    p.season,
    p.week_num,
    n.name,
    p.points AS pts_half_ppr,
    t.points AS pts_te_premium
  FROM
    player_performances p
    JOIN nfl_players n ON p.player_id = n.player_id
    LEFT JOIN player_scores_te_premium t ON p.player_id = t.player_id
    AND p.season = t.season
    AND p.week_num = t.week_num
  WHERE
    p.season = 2023
    AND n.position = 'TE'
  ORDER BY
    p.points DESC
)
SELECT
  week_num AS Week,
  name AS Name,
  pts_half_ppr AS `Half PPR`,
  pts_te_premium AS `With TE Premium`
FROM
  c_te
WHERE
  week_num < 15
ORDER BY
  week_num,
  pts_half_ppr DESC,
  pts_te_premium DESC;
```
![Screenshot 2024-08-20 at 5 14 38 PM](https://github.com/user-attachments/assets/3a426186-569f-4f84-a3f5-f26b7c613a46)

On Tableau, I plot these values on box plots to compare the distribution of tight end performances in half PPR and TE premium scoring systems, so we can visualize and better analyze the effect of a TE premium.
![Screenshot 2024-08-20 171934](https://github.com/user-attachments/assets/12a8e831-a3c2-4a6e-b97d-f3675c4ffe57)

Right off the bat, we can observe that with the TE premium, there are higher scores for tight ends. There is a higher median and an unpward shift in the overall distribution. Not only that, the spread of scores is larger in the TE Premium system, suggesting that there is more performance variability. The higher outliers with the TE premium can also suggest a potential to score exceptionally high on a weekly basis, which could affect the outcomes of fantasy matchups greatly.

#### Tight Ends Compared to Other Positions
In an earlier query, we revealed the average contribution as a percentage of total points for a player of each position for a team. We discovered that tight end had the least average contribution. How would this query change with the adjusted scores in a TE premium?

Without TE Premium (2022 & 2023):
![Screenshot 2024-08-20 at 5 55 45 PM](https://github.com/user-attachments/assets/2492652e-9d72-488e-85bc-f7344f25eda3)

With TE Premium (2023 only):
![Screenshot 2024-08-20 at 5 55 34 PM](https://github.com/user-attachments/assets/477649f7-ed8d-44d3-9dfc-debc0821ab83)

We can see that TE jumps to the second highest contributor in teams' points.

#### How would the standings change in 2023?
Without TE Premium:
![Screenshot 2024-08-20 at 6 14 29 PM](https://github.com/user-attachments/assets/fefe1395-ad6e-4b81-8999-84c5a43d674f)

With TE Premium:
![Screenshot 2024-08-20 at 6 16 18 PM](https://github.com/user-attachments/assets/42f69b18-62e4-4d3f-adde-16344f4879b9)

The standings have changed pretty substantially! The one seed didn't belong to ShawnDeWin anymore with kongfutranda taking his place. My team (hwinsane) dropped to the fifth seed and the_kool_guy14 jumped to the fourth seed. Clearly, there were a few matchups where the outcome was changed. With the TE premium in 2023, we would've seen a different playoff bracket and perhaps a different champion/playoff outcomes!

## Conclusion
In this study, we examined the impact of the TE premium on tight end performances in 2023. Our findings suggest that implementing a TE premium could lead to more volatile tight end performances in the future. Additionally, we observed that tight ends significantly contributed to team total points, influencing the outcomes of several matchups and altering the 2023 playoff picture.

Based on our analysis, I recommend introducing a TE premium to our league commissioner. Our exploratory analysis revealed that tight ends had the lowest average contribution to team total points. By implementing a TE premium, we can expect the value of tight ends to increase, making them more desirable compared to past seasons. This change will likely influence draft strategies, with tight ends being considered more prominently in future drafts and potentially increasing trade activity. Overall, the introduction of a TE premium will enhance the excitement of our fantasy football league and NFL viewing experience.
