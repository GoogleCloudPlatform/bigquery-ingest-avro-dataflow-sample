# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate random order records and send them to Cloud PubSub.

Args:
  project: GCP Project
  topic: Pub/Sub Topic
  num: Number of messages
  format: avro/json
"""

import argparse
import json
import logging
import random
import time
import avro.schema
import avro.io
import io
import datetime

import strgen

from google.cloud import pubsub_v1

logging.basicConfig(level=logging.DEBUG)

"""
  Convert object map to avro byte array
"""
class AvroConvertor(object):
  def __init__(self):
    self.schema = avro.schema.Parse(open("../orderdetails.avsc", "rb").read())

  def convert(self, obj_map):
    writer = avro.io.DatumWriter(self.schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(obj_map, encoder)
    val = bytes_writer.getvalue()
    return val


def get_obj_arr(num_records, avro=False):
  res_arr = []
  a = AvroConvertor()
  id_start = random.randint(1, 1000000)
  for i in range(0, num_records):
    o = {}
    itms = []
    num = random.randint(1, 5)
    for x in range(0, num):
      itm = {}
      itm_id = random.randint(1, 5)
      qty = random.randint(1, 20)
      itm['id'] = itm_id
      itm['name'] = str(strgen.StringGenerator(r'[\d\w]{10}').render())
      itm['price'] = random.uniform(1, 100)
      itms.append(itm)
    o['items'] = itms
    o['id'] = id_start
    id_start += 1
    o['timestamp'] = int(round(time.time() * 1000))
    if avro:
      days = (datetime.datetime.utcnow() - datetime.datetime(1970,1,1)).days
      o['dt'] = days
      res_arr.append(a.convert(o))
    else:
      obj_str = json.dumps(o)
      res_arr.append(json.dumps(o).encode('utf-8'))
  return res_arr


def main(args):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(args.project, args.topic)
  avro = True
  if args.format == 'json':
    avro = False
  obj_arr = get_obj_arr(args.num, avro)

  for obj_str in obj_arr:
    future = publisher.publish(topic_path, data=obj_str)
    logging.info('Published: %s', obj_str)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-p', '--project', help='GCP Project', required=True)
  parser.add_argument('-t', '--topic', help='Pub/Sub Topic', required=True)
  parser.add_argument('-f', '--format', help='Message format (Avro/JSON)', required=True)
  parser.add_argument('-n', '--num', type=int,
                      help='Number of messages', required=True)

  args = parser.parse_args()
  main(args)
