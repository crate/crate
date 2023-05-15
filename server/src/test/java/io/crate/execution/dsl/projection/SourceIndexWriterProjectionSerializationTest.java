/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dsl.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class SourceIndexWriterProjectionSerializationTest {

    @Test
    public void testSerializationFailFast() throws IOException {
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
            0,
            Literal.of(Map.of("f", 10)
            )
        );
        String partitionIdent = "pIdent";
        InputColumn inputColumn = new InputColumn(123);
        List<ColumnIdent> primaryKeys = List.of(new ColumnIdent("colIdent"));
        List<Symbol> partitionedBySymbols = List.of(reference);
        ColumnIdent clusteredByColumn = new ColumnIdent("col1");
        Settings settings = Settings.builder().put("fail_fast", true).build();
        // fail_fast property set to true
        SourceIndexWriterProjection expected = new SourceIndexWriterProjection(
            relationName,
            partitionIdent,
            reference,
            inputColumn,
            primaryKeys,
            partitionedBySymbols,
            clusteredByColumn,
            settings,
            null,
            List.of(),
            null,
            AbstractIndexWriterProjection.OUTPUTS,
            false
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_6_0);
        expected.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_4_6_0);

        assertThat(new SourceIndexWriterProjection(in).failFast()).isNotEqualTo(expected.failFast());

        BytesStreamOutput out2 = new BytesStreamOutput();
        out2.setVersion(Version.V_4_7_0);
        expected.writeTo(out2);

        StreamInput in2 = out2.bytes().streamInput();
        in2.setVersion(Version.V_4_7_0);

        assertThat(new SourceIndexWriterProjection(in2).failFast()).isEqualTo((expected.failFast()));
    }

    @Test
    public void testSerializationValidationFlag() throws IOException {
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
            0,
            Literal.of(Map.of("f", 10)
            )
        );
        String partitionIdent = "pIdent";
        InputColumn inputColumn = new InputColumn(123);
        List<ColumnIdent> primaryKeys = List.of(new ColumnIdent("colIdent"));
        List<Symbol> partitionedBySymbols = List.of(reference);
        ColumnIdent clusteredByColumn = new ColumnIdent("col1");
        Settings settings = Settings.builder().put("validation", false).build();
        // validation property set to false
        SourceIndexWriterProjection validationFlagSetToFalse = new SourceIndexWriterProjection(
            relationName,
            partitionIdent,
            reference,
            inputColumn,
            primaryKeys,
            partitionedBySymbols,
            clusteredByColumn,
            settings,
            null,
            List.of(),
            null,
            AbstractIndexWriterProjection.OUTPUTS,
            false
        );
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_7_0);
        validationFlagSetToFalse.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_4_7_0);

        new SourceIndexWriterProjection(in);

        BytesStreamOutput out2 = new BytesStreamOutput();
        out2.setVersion(Version.V_4_8_0);
        validationFlagSetToFalse.writeTo(out2);

        StreamInput in2 = out2.bytes().streamInput();
        in2.setVersion(Version.V_4_8_0);

        new SourceIndexWriterProjection(in2);
    }
}
