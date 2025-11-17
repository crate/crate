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

package io.crate.execution.dml;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;

public class MixedVersionStorageTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_reading_5_9_tables_with_raw() throws Exception {

        var columns = List.of("id", "name", "numbers");

        var builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.V_5_9_0,
            "create table t (id int, name text, numbers array(int))"
        );
        builder.indexValues(columns, 8, "name8", List.of(8, 80));
        builder.indexValues(columns, 9, "name9", List.of(9, 90));
        try (var tester = builder.build()) {
            var lookup = StoredRowLookup.create(Version.V_5_9_0, tester.tableInfo(), List.of());
            var reader = tester.searcher().getTopReaderContext().leaves().getFirst();
            var row = lookup.getStoredRow(new ReaderContext(reader), 0);
            assertThat(row.asRaw()).isNotEmpty();
            assertThat(row.get(List.of("numbers"))).isOfAnyClassIn(ArrayList.class);
        }
    }

    @Test
    public void test_writing_null_subcolumn_to_ignored_object_in_table_without_oids() throws Exception {
        var columns = List.of("obj");
        var builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.V_5_4_0, // No oids
            "create table t (obj object(ignored))"
        );
        Map<String, Object> map = new HashMap<>();
        map.put("key", null);
        builder.indexValues(columns, map);
        try (var tester = builder.build()) {
            // We write NULL-s to translog since 5.10.13 (https://github.com/crate/crate/pull/18369).
            // This change introduced a regression, causing NULL values in the OBJECT(IGNORED)
            // to be persisted twice in the translog, if table had no oids.
            // We imitate translog lookup to ensure that we are not hitting JSON parsing exception when parsing the source.
            boolean fromTranslog = true;
            var lookup = StoredRowLookup.create(Version.V_5_4_0, tester.tableInfo(), List.of(), List.of(), fromTranslog);
            var reader = tester.searcher().getTopReaderContext().leaves().getFirst();
            var row = lookup.getStoredRow(new ReaderContext(reader), 0);
            // Used to fail with "JsonParseException: Duplicate field 'key'".
            assertThat(row.asMap().get("obj")).isEqualTo(map);
        }
    }
}
