# Studying the Impact of a TE Premium
<img src="https://github.com/user-attachments/assets/5f32c890-ae2b-45a5-b36c-ed348bc59a2c" alt="travis-kelce-resized" align="right" style="margin-right: 10px;" width="200"/>

*Fantasy football allows NFL fans to thrive on the thrill of strategic decision-making and enriches our NFL viewing experience. However, a recurring issue in not just my league but other leagues is the underwhelming impact of tight ends on the overall scoring system. In our setup, tight ends seem to contribute minimally compared to other positions (unless you have someone like Travis Kelce), leading to less excitement and strategic depth in managing this position. This imbalance diminishes the overall fantasy football experience, as participants feel less incentive to invest time and resources in drafting or trading for tight ends. In this project, I hope to make an informed recommendation to our league commissioner on whether a "TE premium" scoring system would be beneficial.*
<div style="clear: both;"></div>

## Project Overview
This project focuses on working within a comprehensive data architecture to leverage the Sleeper API for extracting and analyzing fantasy football data. The primary goal is to design a robust data architecture that supports the exploratory analysis of player statistics, team performances, and league transactions. More importantly, this analysis aims to evaluate the potential benefits of implementing a TE premium, in the interest of enhancing competitive balance and enriching the overall fantasy football experience for future seasons. 

In addition, this project demonstrates key data engineering concepts such as data integration, ETL processes (ELT in this case), data governance, and data security. It showcases my ability to design efficient data pipelines, manage data storage, and ensure data quality and accessibility. The insights derived from the analysis not only inform strategic decisions and league set-up recommendations for those in my fantasy football league but also highlight my proficiency in working within a complex data environment.

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
An overwhelming majority of our ELT process will be done on Azure Databricks, an Apache Spark-based analytics platform optimized for Microsoft Azure. It's an amazing environment for teams of data engineers, data analysts, and data scientists to collaborate on Big Data projects. I chose Databricks to become familiar with a Big Data analytics platform and for its substantial market share as a primary tool in organizations' data architecture. Additionally, I wanted to familiarize myself with Microsoft Azure services, a worthwhile venture as more businesses and organizations transition to a cloud ecosystem.

One key feature of this pipeline is the use of the "Medallion Architecture". It is a data architecture pattern designed to organize data into layers with increasing levels of refinement and quality. This architecture is particularly beneficial for managing large volumes of data, ensuring data quality, and making data accessible and usable for various analytical purposes. Our data is organized into three main layers are Bronze (raw), Silver (processed), and Gold (presentation). You can learn more about it on the databrick website [here](https://www.databricks.com/glossary/medallion-architecture).

### Summary of Project Steps
Our ELT process begins with the Sleeper API. We use Python's requests library to make calls to various endpoints of the API and store its JSON information into the raw container of our Azure Data Lake Storage. Leveraging PySpark, we then process our data through DataFrames. This involves ingesting our JSON files into DataFrames and general data cleaning. More importantly, we store and arrange this processed data (as parquet) in an easy-to-navigate filing system within our processed container. Finally, we make the appropriate transformations for our presentation layer. This involves sorting, aggregating, and even normalization in 3NF.

In this next phase, we put on our data analyst hat and analyze and visualize our fantasy data. We convert our parquet files from our presentation container into tables that are stored in a database in Databricks' file system. Using SQL, we filter and join relevant information in order to smoothly conduct our analysis. And finally, we use Tableau to visualize our data to communicate our findings effectively to our fellow non-technical fantasy users.

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
```
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
```
client_id = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-client-id')
tenant_id = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-tenant-id')
client_secret = dbutils.secrets.get(scope= 'sleeper-secret-scope', key= 'sleeper-project-client-secret')
```

Afterwards, we must set up our mount point. Microsoft Learn actually shows us how to create a mount point in our notebook. Please refer [here](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts) to see how to create a mount point.

The first step is to set up the Spark configuration.
```
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
```

Then use **dbutils** to mount the container.
```
# Mount the storage account container
dbutils.fs.mount(
    source = f"abfss://{<container_name>}@{<storage_account>}.dfs.core.windows.net/",
    mount_point = <mount_point>,
    extra_configs = configs)
```

For example, if we mounted our "raw" container, our mount point would be "/mnt/sleeperprojectdl/raw".

In our notebook, we encapsulate this process in a function and use a for loop to iterate through a list of container names, mounting each container in turn. 
```
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
```
# Mounting containers
storage_account = 'sleeperprojectdl'
containers_for_project = ['raw', 'processed', 'presentation']

for container in containers_for_project:
    mount_adls(storage_account, container)
```

We have finished! The entire notebook for this can be found here.
## Phase Two: Designing our Pipeline
## Phase Three: Analyzing the Data
## Our Findings and Conclusion
