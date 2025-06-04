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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.types.DataTypes;

public class StatisticsTest extends ESTestCase {

    final Map<ColumnIdent, ColumnStats<?>> columnStats = Map.of(
        ColumnIdent.of("a"), new ColumnStats<>(1.0D, 2.0D, 3.0D, DataTypes.INTEGER, MostCommonValues.empty(), List.of()),
        ColumnIdent.of("b"), new ColumnStats<>(3.0D, 4.0D, 5.0D, DataTypes.INTEGER, MostCommonValues.empty(), List.of()),
        ColumnIdent.of("c"), new ColumnStats<>(6.0D, 7.0D, 8.0D, DataTypes.INTEGER, MostCommonValues.empty(), List.of())
    );

    @Test
    public void test_basic() {
        Map<ColumnIdent, ColumnStats<?>> statsByColumn = columnStats;
        try (BufferAllocator bufferAllocator = new RootAllocator()) {
            Statistics statistics = new Statistics(bufferAllocator, 1L, 200L, statsByColumn);
            assertThat(statistics.numDocs()).isEqualTo(1);
            assertThat(statistics.sizeInBytes()).isEqualTo(200L);
            var result = statistics.statsByColumn();
            assertThat(result).isEqualTo(statsByColumn);
            statistics.close();
        }
    }

    @Test
    public void test_streaming() throws IOException {
        try (BufferAllocator bufferAllocator = new RootAllocator();
            BytesStreamOutput out = new BytesStreamOutput()) {
            Statistics statistics = new Statistics(bufferAllocator, 1L, 200L, columnStats);
            statistics.write(out);
            Statistics fromStream = new Statistics(bufferAllocator, out.bytes().streamInput());
            assertThat(statistics.numDocs()).isEqualTo(fromStream.numDocs());
            assertThat(statistics.sizeInBytes()).isEqualTo(fromStream.sizeInBytes());
            assertThat(statistics.statsByColumn()).isEqualTo(fromStream.statsByColumn());
            fromStream.close();
            statistics.close();
        }
    }
}
