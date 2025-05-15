/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

public class DemoLocal {
    public static void main(String[] args) throws IOException, TimeoutException {
        Map<String, String> catalogProps = ImmutableMap.of(
            "type", "hadoop",
            "warehouse", "file:///Users/ahmedabualsaud/github/beamcollege-managed-io/warehouse");
        String sourceTable = "beamcollege.source_table";
        String destTable = "beamcollege.dest_table_23";
        PortablePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PortablePipelineOptions.class);
        options.setJobEndpoint("localhost:12321");
        options.setRunner(PortableRunner.class);

        
        Schema schema = Schema.builder()
            .addInt64Field("id")
            .addStringField("name")
            .addBooleanField("bool")
            .build();
        
        Pipeline p = Pipeline.create(options);
        PCollection<Row> input = p
            .apply(GenerateSequence.from(0).to(100))
            .apply(MapElements.into(TypeDescriptors.rows())
                .via(i -> Row.withSchema(schema)
                    .addValues(Integer.valueOf(String.valueOf(i)), String.valueOf((char) ('a' + i % 26)), i % 3 == 0)
                    .build())).setRowSchema(schema)
            .apply(Managed.write(Managed.ICEBERG)
                .withConfig(
                    ImmutableMap.of(
                        "table", sourceTable,
                        "catalog_properties", catalogProps)))
            .getSinglePCollection();

        input
            .apply(CapitalizeStrings.of("name"))
            .apply(Count.perKey())
            .apply(WordCountRows.create())
            .apply(Managed.write(Managed.ICEBERG)
                .withConfig(
                    ImmutableMap.of(
                        "table", destTable,
                        "catalog_properties", catalogProps)));

        p.run().waitUntilFinish();
    }

    static class WordCountRows extends PTransform<PCollection<KV<String, Long>>, PCollection<Row>> {
        Schema schema = Schema.builder()
            .addStringField("name").addInt64Field("count").build();

        static WordCountRows create() {
            return new WordCountRows();
        }

        @Override
        public PCollection<Row> expand(PCollection<KV<String, Long>> input) {
            return input
                    .apply(
                        MapElements.into(TypeDescriptors.rows())
                            .via(kv -> {
                                    System.out.println(kv.getKey() + ": " + kv.getValue());
                                    Row row = Row.withSchema(schema)
                                        .addValues(kv.getKey(), kv.getValue())
                                        .build();
                                    return row;
                                }))
                .setRowSchema(schema);
        }
    }

    static class CapitalizeStrings extends PTransform<PCollection<Row>, PCollection<KV<String, Row>>> {
        private final String field;

        CapitalizeStrings(String field) {
            this.field = field;
        }

        static CapitalizeStrings of(String field) {
            return new CapitalizeStrings(field);
        }

        @Override
        public PCollection<KV<String, Row>> expand(PCollection<Row> input) {
            return input
                    .apply(MapElements.into(TypeDescriptors.rows())
                        .via(r -> Row.fromRow(r).withFieldValue(field,
                            r.getString(field).toUpperCase()).build()))
                .setRowSchema(input.getSchema())
                .apply(WithKeys.of(r -> r.getString("name")))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SchemaCoder.of(input.getSchema())));
        }
    }
}
