/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.metadata.RelationName;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class TableLookupByOidBenchmark {

    @Param({"1", "10", "50"})
    public int numSchemas;

    public Metadata metadata;
    public List<Integer> allTableOids;
    public int counter = 0;

    @Setup
    public void setup() {
        Metadata.Builder builder = Metadata.builder(0);
        allTableOids = new ArrayList<>();

        for (int s = 0; s < numSchemas; s++) {
            for (int r = 0; r < 100; r++) {
                RelationName ident = new RelationName("schema_" + s, "table_" + r);
                RelationMetadata rel = new RelationMetadata.BlobTable(
                    builder.tableOidSupplier().nextOid(),
                    ident,
                    "uuid_" + s + "_" + r,
                    Settings.EMPTY,
                    IndexMetadata.State.OPEN
                );
                builder.setRelation(rel);
                allTableOids.add(rel.oid());
            }
        }
        metadata = builder.build();
    }

    @Setup(Level.Trial)
    public void verify() {
        for (int oid : allTableOids) {
            if (!Objects.equals(metadata.getRelationFast(oid), metadata.getRelation(oid))) {
                throw new IllegalStateException("The same relationMetadata are expected");
            }
        }
    }

    @Benchmark
    public void measure_oid_lookup(Blackhole bh) {
        int targetOid = allTableOids.get(counter++ % allTableOids.size());
        bh.consume(metadata.getRelation(targetOid));
    }

    @Benchmark
    public void measure_improved_oid_lookup(Blackhole bh) {
        int targetOid = allTableOids.get(counter++ % allTableOids.size());
        bh.consume(metadata.getRelationFast(targetOid));
    }
}
