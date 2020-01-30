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

import com.google.cloud.solutions.beamavro.beans.OrderDetails;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

/**
 * Transform JSON objects to Avro objects
 */
public class JSONToAvro extends DoFn<String, OrderDetails> {
  @ProcessElement
  public void processElement(ProcessContext context) {
    String line = context.element();
    Gson gson = new Gson();
    Order vals = gson.fromJson(line, Order.class);
    OrderDetails.Builder builder = OrderDetails.newBuilder();

    List<com.google.cloud.solutions.beamavro.beans.OrderItem> items = new ArrayList<>();

    for (OrderItem item : vals.getItems()) {
      com.google.cloud.solutions.beamavro.beans.OrderItem.Builder itemBuilder =
          com.google.cloud.solutions.beamavro.beans.OrderItem.newBuilder();
      itemBuilder.setName(item.name).setId(item.id).setPrice(item.price);
      items.add(itemBuilder.build());
    }
    builder.setId(vals.id)
        .setItems(items)
        .setTimestamp(new DateTime(vals.timestamp))
        .setDt(new LocalDate(vals.timestamp));
    context.output(builder.build());
  }
}
