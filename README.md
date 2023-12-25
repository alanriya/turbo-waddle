### Introduction
This is a project that builds a lambda architecture streaming data application serving the batch processing, speed processing and serving layer. It has a UI that displays the streaming project

There will be 2 streaming data sources: the first source will go through the batch processing layer for big data processing using spark processing - the batch processing will be triggered via a Airflow Orchestrator. For batch processing, data will be stored as parquet files in AWS S3. The second source will go through the speed layer that aggregates data in real-time. Both speed layer and processing layer will have a database as datasink. The datasink will update in real-time, and frontend which consist of ReactJs will display a dashboard. 

User will access data insights through React Dashboard.

After all the components are finalised, they are packaged into docker images for resuability on dockerhub. A docker-compose.yml will be used at the end to host the applications.

As a second-stage enhancement, the application will be hosted on kubernetes. 

### Overall Architecture
![Alt text](resources/architecture-shot.png)