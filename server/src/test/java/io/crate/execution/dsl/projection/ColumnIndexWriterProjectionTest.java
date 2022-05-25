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

import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ColumnIndexWriterProjectionTest {

    private RelationName relationName = new RelationName("dummy", "table");

    @Test
    public void testTargetColumnRefsAndSymbolsAreCorrectAfterExclusionOfPartitionColumns() {
        /*
         *  pk:             [ymd, domain, area, isp]
         *  partitioned by: [ymd, isp]
         *  insert into t   (ymd, domain, area, isp, h) select ...
         *
         *  We don't write PartitionedBy columns, so they're excluded from the allTargetColumns:
         *
         *  expected targetRefs:      [ domain, area, h ]
         *  expected column symbols:  [ ic1,   ic2, ic4 ]
         */
        ColumnIdent ymd = new ColumnIdent("ymd");
        ColumnIdent domain = new ColumnIdent("domain");
        ColumnIdent area = new ColumnIdent("area");
        ColumnIdent isp = new ColumnIdent("isp");
        ColumnIdent h = new ColumnIdent("h");
        SimpleReference ymdRef = partitionRef(ymd, DataTypes.STRING);
        SimpleReference domainRef = ref(domain, DataTypes.STRING);
        SimpleReference areaRef = ref(area, DataTypes.STRING);
        SimpleReference ispRef = partitionRef(isp, DataTypes.STRING);
        SimpleReference hRef = ref(h, DataTypes.INTEGER);
        List<ColumnIdent> primaryKeys = Arrays.asList(ymd, domain, area, isp);
        List<SimpleReference> targetColumns = Arrays.asList(ymdRef, domainRef, areaRef, ispRef, hRef);
        List<SimpleReference> targetColumnsExclPartitionColumns = Arrays.asList(domainRef, areaRef, hRef);
        InputColumns.SourceSymbols targetColsCtx = new InputColumns.SourceSymbols(targetColumns);
        List<Symbol> primaryKeySymbols = InputColumns.create(
            Arrays.asList(ymdRef, domainRef, areaRef, ispRef), targetColsCtx);
        List<Symbol> partitionedBySymbols = InputColumns.create(Arrays.asList(ymdRef, ispRef), targetColsCtx);
        List<Symbol> columnSymbols = InputColumns.create(
            targetColumnsExclPartitionColumns,
            targetColsCtx);

        ColumnIndexWriterProjection projection = new ColumnIndexWriterProjection(
            relationName,
            null,
            primaryKeys,
            targetColumns,
            targetColumnsExclPartitionColumns,
            columnSymbols,
            false,
            null,
            primaryKeySymbols,
            partitionedBySymbols,
            null,
            null,
            Settings.EMPTY,
            true,
            List.of(),
            null
        );

        assertThat(projection.columnReferencesExclPartition(), is(Arrays.asList(
            domainRef, areaRef, hRef)
        ));
        assertThat(projection.columnSymbolsExclPartition(), is(Arrays.asList(
            new InputColumn(1, DataTypes.STRING),
            new InputColumn(2, DataTypes.STRING),
            new InputColumn(4, DataTypes.INTEGER))));
    }

    /**
     * Tests a fix for a regression introduced by an incorrect writing of the columns size by
     * https://github.com/crate/crate/commit/468ef007fa8#diff-43b49f4a960311009a1e5c62672bcc13f8721f0c91f5b87e4d7c0d53146005c8R211
     * This led to a NPE https://github.com/crate/crate/issues/10874.
     */
    @Test
    public void test_streaming_with_lot_of_target_columns() throws Exception {
        int numColumns = 200;
        ArrayList<SimpleReference> targetColumns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            var ident = new ColumnIdent(String.valueOf(i));
            targetColumns.add(ref(ident, DataTypes.INTEGER));
        }
        InputColumns.SourceSymbols targetColsCtx = new InputColumns.SourceSymbols(targetColumns);
        List<Symbol> columnSymbols = InputColumns.create(
            targetColumns,
            targetColsCtx);
        var projection = new ColumnIndexWriterProjection(
            relationName,
            null,
            List.of(),
            targetColumns,
            targetColumns,
            columnSymbols,
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

    private SimpleReference ref(ColumnIdent column, DataType<?> type) {
        return new SimpleReference(new ReferenceIdent(relationName, column), RowGranularity.DOC, type, 0, null);
    }

    private SimpleReference partitionRef(ColumnIdent column, DataType<?> type) {
        return new SimpleReference(new ReferenceIdent(relationName, column), RowGranularity.PARTITION, type, 0, null);
    }
}
