# fxavgspread-beam-pipeline
A Beam pipeline which calculates the average spread of the GPB-USD exchange rate in 10 minutes sliding windows with 1 minute steps. The pipeline should read data from Cloud Storage and write the results into BigQuery.


##How to run the job ?

The underliying code contains a beam pipeline job written in Java that reads file from public google cloud storage and calculate the rolling average and persists the data in specified Google BigQuery table. 


###Prerequisites

Install Maven & Java 1.8 is installed in the machine.

Create a Google cloud platform project and enable billing. 

Enable the Google Cloud Storage, Compute Engine, Cloud Data-flow, Stack driver Logging and BigQuery for new project, create service account key and download the json(service account key file)  file.


####Setup

Clone the project in local and try building it using command `mvn clean install`.

Create a Cloud Storage Bucket, create two folders one for holding temporary objects and the other for holding staging objects (eg : gs://aliz/temp & gs://aliz/stage).

Copy the downloaded service account key file (json) to $PROJECT_HOMR/cfg directory and create a SYSTEM variable with the name **GOOGLE_APPLICATION_CREDENTIALS** and point it to `%PROJECT_HOME%/cfg/<<service-account-key>>.json`.

`export GOOGLE_APPLICATION_CREDENTIALS=$PROJECT_HOME/cfg/<<service-account-key>>.json`

and then run the below command to run the job with default 10 minute sliding window with 1 minute steps.

`java -jar $PROJECT_HOME/target/fxavgspread-beam-pipeline-job-0.1.jar --runner=DataflowRunner --project=<project-id> --inputFile=<input file path> --bigQueryDataset=<Bigquery Output Dataset Name> --bigQueryTable=<Bigquery Output Table Name> --tempLocation=<Google storage temp location> --stagingLocation=<Google storage staging location>`      
  
    
Eg:

`java -jar $PROJECT_HOME/target/fxavgspread-beam-pipeline-job-0.1.jar --runner=DataflowRunner --project=aliz --inputFile=gs://solutions-public-assets/time-series-master/GBPUSD_2014_01.csv --bigQueryDataset=aliz-fx --bigQueryTable=FxAverageSpreadRates --tempLocation=gs://aliz-fx/temp --stagingLocation=gs://aliz-fx/stage`


We can change the sliding window at the runtime by the following option.

`java -jar $PROJECT_HOME/target/fxavgspread-beam-pipeline-job-0.1.jar --runner=DataflowRunner --project=<project-id> --inputFile=<input file path> --bigQueryDataset=<Bigquery Output Dataset Name> --bigQueryTable=<Bigquery Output Table Name> --tempLocation=<Google storage temp location> --stagingLocation=<Google storage staging location> --windowDuration=<Window Duration in mins> --windowSlideEvery=<Slide Interval in mins>`  

and if the job is slow and if we would like to scale the number of workers use the below command

`java -jar $PROJECT_HOME/target/fxavgspread-beam-pipeline-job-0.1.jar --runner=DataflowRunner --project=<project-id> --inputFile=<input file path> --bigQueryDataset=<Bigquery Output Dataset Name> --bigQueryTable=<Bigquery Output Table Name> --tempLocation=<Google storage temp location> --stagingLocation=<Google storage staging location> --windowDuration=<Window Duration in mins> --windowSlideEvery=<Slide Interval in mins> --injectorNumWorkers=<Number of workers>`  

    
    
  