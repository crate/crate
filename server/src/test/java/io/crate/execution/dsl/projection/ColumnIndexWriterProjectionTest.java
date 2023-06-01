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

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.RowGranularity;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;


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
        Reference ymdRef = partitionRef(ymd, DataTypes.STRING);
        Reference domainRef = ref(domain, DataTypes.STRING);
        Reference areaRef = ref(area, DataTypes.STRING);
        Reference ispRef = partitionRef(isp, DataTypes.STRING);
        Reference hRef = ref(h, DataTypes.INTEGER);
        List<ColumnIdent> primaryKeys = Arrays.asList(ymd, domain, area, isp);
        List<Reference> targetColumns = Arrays.asList(ymdRef, domainRef, areaRef, ispRef, hRef);
        List<Reference> targetColumnsExclPartitionColumns = Arrays.asList(domainRef, areaRef, hRef);
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
            false,
            false,
            true,
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

        assertThat(projection.columnReferencesExclPartition())
            .containsExactlyElementsOf(
                Arrays.asList(domainRef, areaRef, hRef)
            );

        assertThat(projection.columnSymbolsExclPartition())
            .containsExactlyElementsOf(
                Arrays.asList(
                    new InputColumn(1, DataTypes.STRING),
                    new InputColumn(2, DataTypes.STRING),
                    new InputColumn(4, DataTypes.INTEGER)
                )
            );
    }

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
            false,
            false,
            true,
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

        assertThat(p2).isEqualTo(projection);
    }

    @Test
    public void test_serialization_fail_fast() throws IOException {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            new ArrayType<>(DataTypes.UNTYPED_OBJECT),
            ColumnPolicy.STRICT,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            Literal.of(Map.of("f", 10)
            )
        );

        boolean failFast = true;
        ColumnIndexWriterProjection expected = new ColumnIndexWriterProjection(
            relationName,
            "pIdent",
            List.of(),
            List.of(reference),
            List.of(reference),
            List.of(new InputColumn(0)),
            false,
            false,
            failFast,
            false,
            null,
            List.of(),
            List.of(),
            null,
            null,
            Settings.builder().put("fail_fast", true).build(),
            true,
            List.of(),
            List.of()
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_3_0);
        expected.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_3_0);

        assertThat(new ColumnIndexWriterProjection(in).failFast()).isNotEqualTo(expected.failFast());

        BytesStreamOutput out2 = new BytesStreamOutput();
        out2.setVersion(Version.V_5_4_0);
        expected.writeTo(out2);

        StreamInput in2 = out2.bytes().streamInput();
        in2.setVersion(Version.V_5_4_0);

        assertThat(new ColumnIndexWriterProjection(in2).failFast()).isEqualTo(expected.failFast());
    }

    @Test
    public void test_serialization_validation_flag() throws IOException {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            new ArrayType<>(DataTypes.UNTYPED_OBJECT),
            ColumnPolicy.STRICT,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            Literal.of(Map.of("f", 10)
            )
        );
        // validation property set to false
        boolean validation = false;
        ColumnIndexWriterProjection validationFlagSetToFalse = new ColumnIndexWriterProjection(
            relationName,
            "pIdent",
            List.of(),
            List.of(reference),
            List.of(reference),
            List.of(new InputColumn(0)),
            false,
            false,
            false,
            validation,
            null,
            List.of(),
            List.of(),
            null,
            null,
            Settings.builder().put("validation", false).build(),
            true,
            List.of(),
            List.of()
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_3_0);
        validationFlagSetToFalse.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_3_0);

        assertThat(new ColumnIndexWriterProjection(in).validation()).isTrue(); // validation flag value lost and set to default (true)

        BytesStreamOutput out2 = new BytesStreamOutput();
        out2.setVersion(Version.V_5_4_0);
        validationFlagSetToFalse.writeTo(out2);

        StreamInput in2 = out2.bytes().streamInput();
        in2.setVersion(Version.V_5_4_0);

        assertThat(new ColumnIndexWriterProjection(in2).validation()).isFalse(); // validation flag value recovered
    }

    private Reference ref(ColumnIdent column, DataType<?> type) {
        return new SimpleReference(new ReferenceIdent(relationName, column), RowGranularity.DOC, type, 0, null);
    }

    private Reference partitionRef(ColumnIdent column, DataType<?> type) {
        return new SimpleReference(new ReferenceIdent(relationName, column), RowGranularity.PARTITION, type, 0, null);
    }
}
