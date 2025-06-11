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

package io.crate.statistics.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.execution.ddl.tables.CreateTableRequest;
import io.crate.metadata.ColumnIdent;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.types.DataTypes;

public class StatisticsTest extends ESTestCase {

    @Test
    public void test_basic() throws Exception {
        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            ColumnStats<?> columnStats = new ColumnStats<Integer>(1.0D, 2.0D, 3.0D, DataTypes.INTEGER, MostCommonValues.empty(), List.of());
            ColumnIdent ident = ColumnIdent.of("a");
//        Statistics statistics = new Statistics(RelationName.fromIndexName("doc.test"), 1L, 200L, Map.of(ident, columnStats));
            Statistics statistics = new Statistics(allocator, 1L, 200L, Map.of(ident, columnStats));
            assertThat(statistics.numDocs()).isEqualTo(1);
            assertThat(statistics.sizeInBytes()).isEqualTo(200L);
            ColumnStats<?> result = statistics.getColumnStats(ident);
            assertThat(result).isEqualTo(columnStats);
        }
    }

    @Test
    public void test_streaming() throws Exception {
        try (
            BytesStreamOutput out = new BytesStreamOutput();
            final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            ColumnStats<?> columnStats = new ColumnStats<Integer>(1.0D, 2.0D, 3.0D, DataTypes.INTEGER, MostCommonValues.empty(), List.of());
            ColumnIdent ident = ColumnIdent.of("a");
            Statistics statistics = new Statistics(allocator, 1L, 200L, Map.of(ident, columnStats));
            statistics.write(out);
            try (final RootAllocator newAllocator = new RootAllocator(Integer.MAX_VALUE)) {
                Statistics fromStream = new Statistics(newAllocator, out.bytes().streamInput());
//        long actual = statistics.numDocs();

            long expected = fromStream.numDocs();
                System.out.println("expected = " + expected);
//        assertThat(actual).isEqualTo(expected);
//            assertThat(statistics.sizeInBytes()).isEqualTo(fromStream.sizeInBytes());
//            assertThat(statistics.statsByColumn()).isEqualTo(fromStream.statsByColumn());
            }
        }
    }



}
