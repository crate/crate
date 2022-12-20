/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.statistics;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.Row1;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;


public class TransportAnalyzeActionTest extends ESTestCase {

    @Test
    public void test_create_stats_for_tables_with_array_columns_with_nulls() {
        var rows = new ArrayList<String>();
        rows.add(null);

        ArrayType<String> type = DataTypes.STRING_ARRAY;
        var samples = new Samples(
            List.of(new Row1(rows), new Row1(rows)),
            List.of(type.streamer()),
            2,
            type.valueBytes(rows)
        );
        var references = List.<Reference>of(
            new SimpleReference(
                new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy"), "dummy"),
                RowGranularity.DOC,
                DataTypes.STRING_ARRAY,
                0,
                null)
        );
        var stats = TransportAnalyzeAction.createTableStats(samples, references);
        assertThat(stats.numDocs, is(2L));
    }
}
