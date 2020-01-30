This tutorial describes storing Avro [SpecificRecord](http://avro.apache.org/docs/1.8.1/api/java/index.html?org/apache/avro/specific/SpecificRecord.html) objects in [BigQuery](https://cloud.google.com/bigquery) using [Cloud Dataflow](https://cloud.google.com/dataflow) by automatically generating the table schema and transforming the input elements. This tutorial also showcases the usage of Avro-generated classes to materialize or transmit intermediate data between workers in your [Cloud Dataflow](https://cloud.google.com/dataflow) pipeline.

Please refer to the related [article](https://cloud.google.com/solutions/streaming-avro-records-into-bigquery-using-dataflow) for all the steps to follow in this tutorial.

Contents of this repository:

* `BeamAvro`: Java code for the Apache Beam pipeline deployed on [Cloud Dataflow](https://cloud.google.com/dataflow/).
* `generator`: Python code for the randomized event generator.

To run the example:
* Set environment variables using env.sh
* Generate java beans from the avro file: `mvn generate-sources`
* Run Dataflow pipeline: mvn compile exec:java -Dexec.mainClass=com.google.cloud.solutions.beamavro.AvroToBigQuery -Dexec.cleanupDaemonThreads=false   -Dexec.args="--project=$GOOGLE_CLOUD_PROJECT --runner=DataflowRunner --stagingLocation=gs://$MY_BUCKET/stage/ --tempLocation=gs://$MY_BUCKET/temp/ --inputPath=projects/$GOOGLE_CLOUD_PROJECT/topics/$MY_TOPIC --workerMachineType=n1-standard-1 --maxNumWorkers=$NUM_WORKERS --region=$REGION --dataset=$BQ_DATASET --bqTable=$BQ_TABLE --outputPath=$AVRO_OUT"
* Run event generation script: python3 generator/gen.py -p $GOOGLE_CLOUD_PROJECT -t $MY_TOPIC -n 100 -f avro
