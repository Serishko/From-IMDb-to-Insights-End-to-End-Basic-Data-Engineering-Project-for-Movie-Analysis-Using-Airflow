# From-IMDb-to-Insights-End-to-End-Basic-Data-Engineering-Project-for-Movie-Analysis-Using-Airflow
## Architecture

![architecture](https://github.com/Serishko/From-IMDb-to-Insights-End-to-End-Basic-Data-Engineering-Project-for-Movie-Analysis-Using-Airflow/assets/58653229/b32b1810-43d2-4c16-941a-3be68ccd5c3b)

## Tools and Technologies

* Python
* Amazon S3
* Snowflake
* Tableau
* Airflow
* Docker

## Overview

The top 250 IMDb movies are extracted from the IMDb website based on ratings using Beautiful Soup as the web scraping tool. The resulting data is stored as a JSON file, which is then uploaded to an AWS S3 bucket for storage. From the S3 bucket, the JSON file is retrieved and integrated into Snowflake, a cloud-based data warehousing platform. Utilizing SQL queries within Snowflake, the JSON data is transformed. All the above steps are done using Airflow as an orchestrator. A CSV file is then generated from Snowflake as the final result. This CSV file is subsequently downloaded to the local machine, where it can be accessed for further analysis and processing. Finally, the downloaded CSV file is imported into Tableau, a data visualization and analytics tool, enabling the creation of data-driven visualizations and interactive dashboards.

## Dashboard

![image](https://github.com/Serishko/data-enngineering-project/assets/58653229/6047cf2b-aa24-4ef4-8935-9d13796a2911)

Check out the Tableau dashboard for visualizing the data:
[Tableau Dashboard](https://public.tableau.com/views/imdb_dashboard_16861536566380/Dashboard1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link)


