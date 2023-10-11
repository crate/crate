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

package io.crate.execution.dsl.projection;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ColumnIndexWriterProjectionTest {

    private RelationName relationName = new RelationName("dummy", "table");

    /**
     * Tests a fix for a regression introduced by an incorrect writing of the columns size by
     * https://github.com/crate/crate/commit/468ef007fa8#diff-43b49f4a960311009a1e5c62672bcc13f8721f0c91f5b87e4d7c0d53146005c8R211
     * This led to a NPE https://github.com/crate/crate/issues/10874.
     */
    @Test
    public void test_streaming_with_lot_of_target_columns() throws Exception {
        int numColumns = 200;
        ArrayList<Reference> targetColumns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            var ident = new ColumnIdent(String.valueOf(i));
            targetColumns.add(ref(ident, DataTypes.INTEGER));
        }
        var projection = new ColumnIndexWriterProjection(
            relationName,
            null,
            List.of(),
            targetColumns,
            false,
            Collections.emptyMap(),
            List.of(),
            List.of(),
            null,
            null,
            Settings.EMPTY,
            true,
            List.of(),
            List.of()
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(projection, out);

        StreamInput in = out.bytes().streamInput();
        ColumnIndexWriterProjection p2 = (ColumnIndexWriterProjection) Projection.fromStream(in);

        assertThat(p2, is(projection));
    }

    private Reference ref(ColumnIdent column, DataType<?> type) {
        return new SimpleReference(new ReferenceIdent(relationName, column), RowGranularity.DOC, type, 0, null);
    }

    private Reference partitionRef(ColumnIdent column, DataType<?> type) {
        return new SimpleReference(new ReferenceIdent(relationName, column), RowGranularity.PARTITION, type, 0, null);
    }
}
