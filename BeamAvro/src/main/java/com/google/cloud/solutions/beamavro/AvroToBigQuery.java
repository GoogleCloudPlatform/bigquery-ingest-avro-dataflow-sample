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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;


/**
 * Creates a beam Pipeline that reads JSON or Avro records, writes the Avro records to GCS and
 * BigQuery
 */
public class AvroToBigQuery {

  private final AvroToBigQueryOptions options;
  private final Pipeline pipeline;

  @VisibleForTesting
  AvroToBigQuery(String[] args, Pipeline pipeline) {
    AvroToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AvroToBigQueryOptions.class);
    options.setStreaming(true);

    this.options = options;
    this.pipeline = (pipeline != null) ? pipeline : Pipeline.create(options);
  }

  public interface AvroToBigQueryOptions extends GcpOptions, PubsubOptions, StreamingOptions {

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
    ValueProvider<String> getBigQueryTable();

    void setBigQueryTable(ValueProvider<String> bqTable);

    @Description("Is PubSub message JSON format")
    @Default.Boolean(false)
    boolean getJsonFormat();

    void setJsonFormat(boolean jsonFormat);

    @Description("User provided AVRO schema")
    @Validation.Required
    String getAvroSchema();

    void setAvroSchema(String avroSchema);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(AvroToBigQueryOptions.class);

    new AvroToBigQuery(args, null).buildPipeline().run().waitUntilFinish();
  }

  @VisibleForTesting
  Pipeline buildPipeline() {
    String bigQueryTable = String
        .format("%s:%s.%s", options.getProject(), options.getDataset().get(),
            options.getBigQueryTable().get());

    Schema avroSchema = new Schema.Parser().parse(options.getAvroSchema());

    PCollection<GenericRecord> records =
        pipeline.apply("Read PubSub", new PubSubReader())
            .setCoder(AvroUtils.schemaCoder(avroSchema));

    // [START gcs_write]
    // Write to GCS
    records
        .apply("Window for 10 seconds", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(
            "Write Avro file",
            AvroIO.writeGenericRecords(avroSchema).to(options.getOutputPath())
                .withWindowedWrites().withNumShards(5));
    // [END gcs_write]

    // [START bq_write]
    // Write to BigQuery
    records.apply(
        "Write to BigQuery",
        BigQueryIO.<GenericRecord>write()
            .to(bigQueryTable)
            .useBeamSchema()
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .optimizedWrites());
    // [END bq_write]

    return pipeline;
  }


  private class PubSubReader extends PTransform<PBegin, PCollection<GenericRecord>> {

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {

      // [START read_input]
      Schema avroSchema = new Schema.Parser().parse(options.getAvroSchema());

      if (options.getJsonFormat()) {
        return input
            .apply("Read Json", PubsubIO.readStrings().fromTopic(options.getInputPath()))
            .apply("Make GenericRecord", MapElements.via(JsonToAvroFn.of(avroSchema)));
      } else {
        return input.apply("Read GenericRecord", PubsubIO.readAvroGenericRecords(avroSchema)
            .fromTopic(options.getInputPath()));
      }
      // [END read_input]
    }
  }


  /**
   * Transform JSON objects to GenericRecord.
   */
  public static class JsonToAvroFn extends SimpleFunction<String, GenericRecord> {

    private final String schema;

    public JsonToAvroFn(String schema) {
      this.schema = schema;
    }

    public static JsonToAvroFn of(String avroSchema) {
      return new JsonToAvroFn(avroSchema);
    }

    public static JsonToAvroFn of(Schema avroSchema) {
      return of(avroSchema.toString());
    }

    @Override
    public GenericRecord apply(String avroJson) {
      try {
        Schema avroSchema = getSchema();
        return new GenericDatumReader<GenericRecord>(avroSchema)
            .read(null, DecoderFactory.get().jsonDecoder(avroSchema, avroJson));
      } catch (IOException ioException) {
        throw new RuntimeException("Error parsing Avro JSON", ioException);
      }
    }

    private Schema getSchema() {
      return new Schema.Parser().parse(schema);
    }
  }
}
