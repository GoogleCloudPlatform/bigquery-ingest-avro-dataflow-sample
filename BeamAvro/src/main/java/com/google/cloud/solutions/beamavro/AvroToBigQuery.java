/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.beamavro;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.beamavro.beans.OrderDetails;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

/**
 * Creates a beam Pipeline that reads JSON or Avro records, writes the Avro records to GCS and
 * BigQuery
 */
public class AvroToBigQuery {
  public enum FORMAT {
    JSON,
    AVRO
  }

  // [START read_input]
  private static PCollection<OrderDetails> getInputCollection(
      Pipeline pipeline, String inputPath, FORMAT format) {
    if (format == FORMAT.JSON) {
      // Transform JSON to Avro
      return pipeline
          .apply("Read JSON from PubSub", PubsubIO.readStrings().fromTopic(inputPath))
          .apply("To binary", ParDo.of(new JSONToAvro()));
    } else {
      // Read Avro
      return pipeline.apply(
          "Read Avro from PubSub", PubsubIO.readAvros(OrderDetails.class).fromTopic(inputPath));
    }
  }
  // [END read_input]

  public interface OrderAvroOptions extends PipelineOptions, DataflowPipelineOptions {
    @Description("Input path")
    @Validation.Required
    ValueProvider<String> getInputPath();

    void setInputPath(ValueProvider<String> path);

    @Description("Output path")
    @Validation.Required
    ValueProvider<String> getOutputPath();

    void setOutputPath(ValueProvider<String> path);

    @Description("BigQuery Dataset")
    @Validation.Required
    ValueProvider<String> getDataset();

    void setDataset(ValueProvider<String> dataset);

    @Description("BigQuery Table")
    @Validation.Required
    ValueProvider<String> getBqTable();

    void setBqTable(ValueProvider<String> bqTable);

    @Description("Input Format")
    @Default.Enum("AVRO")
    FORMAT getFormat();

    public void setFormat(FORMAT format);
  }

  private static String getBQString(OrderAvroOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append(options.getProject());
    sb.append(':');
    sb.append(options.getDataset().get());
    sb.append('.');
    sb.append(options.getBqTable().get());
    return sb.toString();
  }

  private static final SerializableFunction TABLE_ROW_PARSER =
      new SerializableFunction<SpecificRecord, TableRow>() {
        @Override
        public TableRow apply(SpecificRecord specificRecord) {
          return BigQueryAvroUtils.convertSpecificRecordToTableRow(
              specificRecord, BigQueryAvroUtils.getTableSchema(specificRecord.getSchema()));
        }
      };

  public static void main(String args[]) {
    OrderAvroOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(OrderAvroOptions.class);

    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    String bqStr = getBQString(options);
    // [START schema_setup]
    TableSchema ts = BigQueryAvroUtils.getTableSchema(OrderDetails.SCHEMA$);
    // [END schema_setup]

    // Read JSON objects from PubSub
    PCollection<OrderDetails> ods =
        getInputCollection(pipeline, options.getInputPath().get(), options.getFormat());

    // Write to GCS
    // [START gcs_write]
    ods.apply(
        "Write to GCS",
        new AvroWriter()
            .withOutputPath(options.getOutputPath())
            .withRecordType(OrderDetails.class));
    // [END gcs_write]
    // Write to BigQuery
    // [START bq_write]
    ods.apply(
        "Write to BigQuery",
        BigQueryIO.write()
            .to(bqStr)
            .withSchema(ts)
            .withWriteDisposition(WRITE_APPEND)
            .withCreateDisposition(CREATE_IF_NEEDED)
            .withFormatFunction(TABLE_ROW_PARSER));
    // [END bq_write]

    pipeline.run().waitUntilFinish();
  }
}
