This tutorial describes storing Avro [SpecificRecord](http://avro.apache.org/docs/1.8.1/api/java/index.html?org/apache/avro/specific/SpecificRecord.html) objects in [BigQuery](https://cloud.google.com/bigquery) using [Cloud Dataflow](https://cloud.google.com/dataflow) by automatically generating the table schema and transforming the input elements. This tutorial also showcases the usage of Avro-generated classes to materialize or transmit intermediate data between workers in your [Cloud Dataflow](https://cloud.google.com/dataflow) pipeline.

Please refer to the related [article](https://cloud.google.com/solutions/streaming-avro-records-into-bigquery-using-dataflow) for all the steps to follow in this tutorial.

Contents of this repository:

* `BeamAvro`: Java code for the Apache Beam pipeline deployed on [Cloud Dataflow](https://cloud.google.com/dataflow/).
* `generator`: Python code for the randomized event generator.

To run the example:
1. Update configuration by updating [env.sh](env.sh)
2. Set environment variables
    ```shell script
    source env.sh
    ```
3. Generate java beans from the avro file and Run Dataflow pipeline: 
    ```shell script
    mvn clean compile package   
   ```

   Run the pipeline
   ```shell
   java -cp target/BeamAvro-bundled-1.0-SNAPSHOT.jar \
   com.google.cloud.solutions.beamavro.AvroToBigQuery \
   --project=$GOOGLE_CLOUD_PROJECT \
   --runner=DataflowRunner \
   --stagingLocation=gs://$MY_BUCKET/stage/ \
   --tempLocation=gs://$MY_BUCKET/temp/ \
   --inputPath=projects/$GOOGLE_CLOUD_PROJECT/topics/$MY_TOPIC \
   --workerMachineType=n1-standard-1 \
   --region=$REGION \
   --dataset=$BQ_DATASET \
   --bqTable=$BQ_TABLE \
   --outputPath=$AVRO_OUT \   
   --avroSchema="$(<src/main/resources/orderdetails.avsc)"
   ```
4. Run event generation script:
   1. Create Python virtual environment
        ```shell script
        python3 -m venv ~/generator-venv
        source ~/generator-venv/bin/activate
        ```
   2. Install python dependencies
        ```shell script
        pip install -r generator/requirements.txt
        ```
   3. Run the Generator
        ```shell script
        python generator/gen.py -p $GOOGLE_CLOUD_PROJECT -t $MY_TOPIC -n 100 -f avro
        ```
      
