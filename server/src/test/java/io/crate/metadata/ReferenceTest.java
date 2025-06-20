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

package io.crate.metadata;

import static io.crate.testing.Asserts.assertThat;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

public class ReferenceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testEquals() {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        var dataType1 = new ArrayType<>(DataTypes.UNTYPED_OBJECT);
        var dataType2 = new ArrayType<>(DataTypes.UNTYPED_OBJECT);
        Symbol defaultExpression1 = Literal.of(dataType1, List.of(Map.of("f", 10)));
        Symbol defaultExpression2 = Literal.of(dataType2, List.of(Map.of("f", 10)));
        SimpleReference reference1 = new SimpleReference(referenceIdent,
                                                         RowGranularity.DOC,
                                                         dataType1,
                                                         IndexType.PLAIN,
                                                         false,
                                                         true,
                                                         1,
                                                         111,
                                                         true,
                                                         defaultExpression1);
        SimpleReference reference2 = new SimpleReference(referenceIdent,
                                                         RowGranularity.DOC,
                                                         dataType2,
                                                         IndexType.PLAIN,
                                                         false,
                                                         true,
                                                         1,
                                                         111,
                                                         true,
                                                         defaultExpression2);
        assertThat(reference1).isEqualTo(reference2);
        assertThat(reference2).isNotEqualTo(reference2.withDropped(false));
    }

    @Test
    public void testStreaming() throws Exception {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        var dataType = new ArrayType<>(DataTypes.UNTYPED_OBJECT);
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            dataType,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            111,
            true,
            Literal.of(dataType, List.of(Map.of("f", 10))
            )
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Reference.toStream(out, reference);

        StreamInput in = out.bytes().streamInput();
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2).isEqualTo(reference);
    }

    @Test
    public void test_streaming_of_reference_position_before_4_6_0() throws Exception {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "object_column");
        var dataType = new ArrayType<>(DataTypes.UNTYPED_OBJECT);
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            dataType,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            COLUMN_OID_UNASSIGNED,
            false,
            Literal.of(dataType, List.of(Map.of("f", 10))
            )
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_5_0);
        Reference.toStream(out, reference);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_4_5_0);
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2).isEqualTo(reference);
    }

    @Test
    public void test_mapping_generation_for_varchar_with_length() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (xs varchar(40))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference reference = table.getReference(ColumnIdent.of("xs"));
        Map<String, Object> mapping = reference.toMapping(reference.position());
        assertThat(mapping)
            .containsEntry("length_limit", 40)
            .containsEntry("position", 1)
            .containsEntry("oid", 1L)
            .containsEntry("type", "keyword")
            .doesNotContainKey("dropped")
            .hasSize(4);
        IndexMetadata indexMetadata = clusterService.state().metadata().indices().valuesIt().next();
        assertThat(indexMetadata.mapping()).isNull();
        assertThat(reference.valueType()).isEqualTo(StringType.of(40));
    }

    @Test
    public void test_mapping_generation_for_string_without_doc_values() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (xs string storage with (columnstore = false))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference reference = table.getReference(ColumnIdent.of("xs"));
        Map<String, Object> mapping = reference.toMapping(reference.position());
        assertThat(mapping)
            .containsEntry("position", 1)
            .containsEntry("oid", 1L)
            .containsEntry("type", "keyword")
            .doesNotContainKey("dropped")
            .containsEntry("doc_values", "false")
            .hasSize(4);
        IndexMetadata indexMetadata = clusterService.state().metadata().indices().valuesIt().next();
        assertThat(indexMetadata.mapping()).isNull();
        assertThat(reference.hasDocValues()).isFalse();
    }

    @Test
    public void test_mapping_generation_for_float_without_doc_values() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
                .addTable("create table tbl (xs float storage with (columnstore = false))");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference reference = table.getReference(ColumnIdent.of("xs"));
        Map<String, Object> mapping = reference.toMapping(reference.position());
        assertThat(mapping)
            .containsEntry("position", 1)
            .containsEntry("oid", 1L)
            .containsEntry("type", "float")
            .doesNotContainKey("dropped")
            .containsEntry("doc_values", "false")
            .hasSize(4);
        IndexMetadata indexMetadata = clusterService.state().metadata().indices().valuesIt().next();
        assertThat(indexMetadata.mapping()).isNull();
        assertThat(reference.hasDocValues()).isFalse();
        assertThat(reference.valueType()).isEqualTo(DataTypes.FLOAT);
    }

    @Test
    public void test_streaming_column_policy_removed_on_or_after_5_10_0() throws IOException {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "o");
        var dataType = new ArrayType<>(ObjectType.of(ColumnPolicy.IGNORED)
            .setInnerType("o2", ObjectType.of(ColumnPolicy.STRICT).build())
            .build());
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            dataType,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            COLUMN_OID_UNASSIGNED,
            false,
            null
        );
        var expectedType = new ArrayType<>(ObjectType.of(ColumnPolicy.IGNORED)
            // inner object's column policy cannot be recovered, STRICT > DYNAMIC
            .setInnerType("o2", ObjectType.of(ColumnPolicy.DYNAMIC).build())
            .build());
        SimpleReference expectedRef = (SimpleReference) reference.withValueType(expectedType);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_9_0);
        Reference.toStream(out, reference);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_9_0);
        Reference reference2 = Reference.fromStream(in);

        assertThat(reference2).isEqualTo(expectedRef);
    }

    @Test
    public void test_mapping_generation_default_expression() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (xs string default 'foo')");
        DocTableInfo table = e.resolveTableInfo("tbl");
        Reference reference = table.getReference(ColumnIdent.of("xs"));
        Map<String, Object> mapping = reference.toMapping(reference.position());
        assertThat(mapping)
            .containsEntry("position", 1)
            .containsEntry("oid", 1L)
            .containsEntry("type", "keyword")
            .doesNotContainKey("dropped")
            .containsEntry("default_expr", "'foo'")
            .hasSize(4);
        IndexMetadata indexMetadata = clusterService.state().metadata().indices().valuesIt().next();
        assertThat(indexMetadata.mapping()).isNull();
        assertThat(reference.defaultExpression()).isLiteral("foo");
    }

    @Test
    public void test_storage_ident_without_oid() {
        RelationName relationName = new RelationName("doc", "test");
        ReferenceIdent referenceIdent = new ReferenceIdent(relationName, "o");
        var dataType = new ArrayType<>(ObjectType.of(ColumnPolicy.IGNORED)
            .setInnerType("o2", ObjectType.of(ColumnPolicy.STRICT).build())
            .build());
        SimpleReference reference = new SimpleReference(
            referenceIdent,
            RowGranularity.DOC,
            dataType,
            IndexType.FULLTEXT,
            false,
            true,
            0,
            COLUMN_OID_UNASSIGNED,  // important! with an oid the storageIdent is the oid itself
            false,
            null
        );

        var docRef = DocReferences.toDocLookup(reference);
        assertThat(docRef.storageIdent()).isEqualTo("o");
    }
}
