<a name="readme-top"></a>
# 👨‍💻 Built with

<img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue"/> <img src="https://bentego.com/wp-content/uploads/2022/03/xx.png" width="100" height="27,5" />
<img src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white" />
<img src="https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white" />
<img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white" /> 
<img src="https://media.licdn.com/dms/image/D4D12AQGmDkishDrQ7A/article-cover_image-shrink_720_1280/0/1707514298430?e=2147483647&v=beta&t=Kez0xxjher8Bhf1sZ67ko5N1uKj81ZdkyRlHDN7l-h4" width="100" height="27,5" />
<img src="https://lh6.googleusercontent.com/2jkuIwxrds9QFLoBN5Rh1-uLgt5ukWTmGjXj_8TluJTdb9jYZ2su50b0vM_zU7cqn3y7xf5MRNjrrXUVQtZ-xetqMVgGrQBivhurxhTyM0ElzaSANRvEftTcW7edTVmb7UhJZ0Tj" width="100" height="27,5" />
<img src="https://media.licdn.com/dms/image/C5612AQFW02yzG1e_NA/article-cover_image-shrink_720_1280/0/1595330203608?e=2147483647&v=beta&t=VJk0SODBlUaxVYaCEB_wdz_gUwUAz65OHHjgdH3ACBE" width="100" height="27,5" style="background-color:white" />

<!-- ABOUT THE PROJECT -->
# ℹ️ About The Project

This project facilitates the comprehensive data processing, analysis, and visualization of a flight booking system. It harnesses the power of PySpark job executed on Google Cloud Dataproc (Serverless) for large-scale data processing. Visualizations are delivered through Apache Superset and Jupyter Notebooks, providing intuitive insights into the data.

The architecture incorporates a local database for Jupyter Notebooks and allows Spark to run locally for initial data processing. Processed aggregates are then uploaded to Google BigQuery for efficient storage and querying.

For seamless development and deployment, the project is managed through GitLab CI/CD, ensuring continuous integration, deployment, and rigorous testing, with all tests written using pytest.

# ✅  Pytest test pass

? github coverage ?

# 🧮 Created Aggregates

- Number of bookings
- Generated revenue
- Time spent midair
- Number of flights between 0:00 and 6:00
- Number of flights between 6:00 and 12:00
- Number of flights between 12:00 and 18:00
- Number of flights between 18:00 and 00:00
- Average delay per flight measured in secods
- Average occupancy of flight
- Average occupancy of flight in economy class
- Average occupancy of flight in business class
- Average occupancy of flight in comfort class


<img src="link"/>


# 🛢 Database Information

Database is available [here](https://postgrespro.com/community/demodb).<br>
I used [demo-big-en.zip (232 MB) — flight data for one year (DB size is about 2.5 GB).](https://edu.postgrespro.com/demo-big-en.zip).<br>

## 🧩 Database Schema<br>
<img src="link"/>

<br>
<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 📝 Schema Description<br>

The main entity is a booking (bookings).

One booking can include several passengers, with a separate ticket (tickets) issued to each passenger. A ticket has a unique number and includes information about the passenger. As such, the passenger is not a separate entity. Both the passenger's name and identity document number can change over time, so it is impossible to uniquely identify all the tickets of a particular person; for simplicity, we can assume that all passengers are unique.

The ticket includes one or more flight segments (ticket_flights). Several flight segments can be included into a single ticket if there are no non-stop flights between the points of departure and destination (connecting flights), or if it is a round-trip ticket. Although there is no constraint in the schema, it is assumed that all tickets in the booking have the same flight segments.

Each flight (flights) goes from one airport (airports) to another. Flights with the same flight number have the same points of departure and destination, but differ in departure date.

At flight check-in, the passenger is issued a boarding pass (boarding_passes), where the seat number is specified. The passenger can check in for the flight only if this flight is included into the ticket. The flight-seat combination must be unique to avoid issuing two boarding passes for the same seat.

The number of seats (seats) in the aircraft and their distribution between different travel classes depends on the model of the aircraft (aircrafts) performing the flight. It is assumed that every aircraft model has only one cabin configuration. Database schema does not check that seat numbers in boarding passes have the corresponding seats in the aircraft (such verification can be done using table triggers, or at the application level).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## ⚙️ Local Database Setup

Put downloaded database in database directory and name it as flights_db.sql

Move to database directory:
```
cd database
```

Build image:
```
docker build -t flights_image .
```

Run container:
```
docker run -d -p 8001:5432 --name flights_container flights_image
```
If port 8001:5432 does not work use 5432:5432 instead.
<br>
<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 📊 Apache Superset Dashboard

## ⚙️ Apache Superset Setup

Before setup make sure to fill neccessary field in files:
```
superset-init-sample.sh
superset_config-sample.py
```
MAP_BOX_API_KEY is obligatory for dynamic map to work.<br>
You can obtain it on [Mapbox Webpage](https://account.mapbox.com/)<br>
Of course afterwards rename files not to include sample word.

Move to superset directory
```
cd superset
```
Launch Apache Superset container
```
docker compose up
```
Go to login page and login using credentials from superset-init.sh file
```
http://localhost:8088/login/
```
In order to connect to Google Big Query you have to create connection in Apache superset<br>
<img src="link"/>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 📊 Apache Superset Dashboard
<img src="link"/>
<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 📓 Jupyter Notebook Data Analysis

Download [postgresql-42.5.1.jar ](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/) from repository and put it into notebook directory<br>

Move to notebook directory:
```
cd notebook
```
Build image:
```
docker build -t jupyter-notebook-spark .
```

Run container:
```
docker run -it -p 8888:8888 -v ${PWD}:/home/jovyan/work jupyter-notebook-spark
```
<img src="link"/>
<p align="right">(<a href="#readme-top">back to top</a>)</p>


# 🔑Setup

In order to have the job running on the GCP you have to do the following

**Have service account with required permissions, you need following roles:**
```
ROLE: roles/bigquery.admin

ROLE: roles/dataproc.admin

ROLE: roles/dataproc.editor

ROLE: roles/dataproc.worker

ROLE: roles/iam.serviceAccountAdmin

ROLE: roles/iam.serviceAccountUser

ROLE: roles/storage.objectAdmin

ROLE: roles/storage.objectCreator
```
**Use following commands to grant them**
```
gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/storage.bigquery.admin"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/storage.dataproc.admin"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/storage.dataproc.editor"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/storage.dataproc.worker"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/iam.serviceAccountAdmin"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding <project_id>     --member="<service_account>"     --role="roles/storage.objectCreator"

```

**Have required network and subnetwork infrastructure for your Dataproc job.**
```
gcloud compute networks create <network_name>     --subnet-mode=custom

gcloud compute networks subnets create my-new-subnet --network=<network_name> --region=us-central1 --range=10.0.0.0/24

gcloud compute firewall-rules create allow-internal-ingress --network=<network_name> --source-ranges=10.0.0.0/24 --destination-ranges=10.0.0.0/24 --direction=ingress --action=allow --rules=all

gcloud compute networks subnets create <subnet_name> --network=<network_name>

gcloud compute networks subnets update <subnet_name> --region=us-central1 --enable-private-ip-google-access
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# 🌲 Project tree
```bash
.
├─── app
│    ├── main.py
│    ├── jobs
│    │   ├── __init__.py
│    │   ├── flight_job.py
│    │   └── functions
│    │       ├── __init__.py
│    │       └── functions.py
│    └── tests        
│        ├── pytest.ini    
│        └── test_aggregates.py
├─── database
│    ├── flights_db.sql  
│    └── Dockerfile
├─── img
│    ├── img1
│    ├── img2
│    ├── gif1
│    ├── gif2
│    └── gif3
├─── notebook
│    ├── postgresql-42.5.1.jar  
│    ├── data_analysis.ipynb
│    └── Dockerfile
├─── superset
│    ├── Dockerfile  
│    ├── docker-compose.yml
│    ├── superset-init-sample.sh
│    └── superset_config-sample.py
├─── .coverageerc
├─── .gitignore
├───  README.md
└─── license.txt


```
<p align="right">(<a href="#readme-top">back to top</a>)</p>


# 📄 License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
  
