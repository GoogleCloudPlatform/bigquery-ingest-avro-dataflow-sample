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

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;

/**
 * Composite transform to write Avro files to GCS
 * @param <T extends SpecificRecordBase>
 */
public class AvroWriter<T extends SpecificRecordBase> extends PTransform<PCollection<T>, POutput> {
  private ValueProvider<String> outputPath;
  private Class<T> recordType;

  public AvroWriter<T> withOutputPath(ValueProvider<String> outputPath) {
    this.outputPath = outputPath;
    return this;
  }

  public AvroWriter<T> withRecordType(Class<T> recordType) {
    this.recordType = recordType;
    return this;
  }

  @Override
  public POutput expand(PCollection<T> inputRecords) {
    // Write to GCS
    return inputRecords
        .apply("Window for 10 seconds", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(
            "Write Avro file",
            AvroIO.write(recordType).to(outputPath).withWindowedWrites().withNumShards(5));
  }
}
