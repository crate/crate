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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.fdw.ForeignTablesMetadata;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.role.Role;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

@SuppressWarnings("deprecation")
public class MetadataTest extends ESTestCase {

    @Test
    public void test_bwc_read_writes_with_6_1_0() throws Exception {
        Metadata metadata = new Metadata.Builder(Metadata.OID_UNASSIGNED)
                .columnOID(123L)
                .build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_6_1_0);
        metadata.writeTo(out); // OID should be written, 6.1.0 expects it.
        var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        in.setVersion(Version.V_6_1_0);
        Metadata recievedMetadata = Metadata.readFrom(in); // We are reading from 6.1.0, which sends out OID.
        assertThat(recievedMetadata.columnOID()).isEqualTo(123L);
    }

    @Test
    public void test_bwc_streaming_table_oid() throws IOException {
        int tableOID = 123;
        Metadata metadata = Metadata.builder(tableOID)
            .build();
        assertThat(metadata.currentMaxTableOid()).isEqualTo(tableOID);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_6_2_0);
        metadata.writeTo(out);
        var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        in.setVersion(Version.V_6_2_0);
        Metadata recievedMetadata = Metadata.readFrom(in);
        assertThat(recievedMetadata.currentMaxTableOid()).isEqualTo(Metadata.OID_UNASSIGNED);

        out = new BytesStreamOutput();
        metadata.writeTo(out);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        recievedMetadata = Metadata.readFrom(in);
        assertThat(recievedMetadata.currentMaxTableOid()).isEqualTo(tableOID);

        // diff streaming
        Metadata prevMetadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .removeCustom(IndexGraveyard.TYPE)
            .build();
        // build metadataDiff with currentMaxTableOid = 123
        var metadataDiff = metadata.diff(Version.CURRENT, prevMetadata);

        out = new BytesStreamOutput();
        metadataDiff.writeTo(out);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        var receivedMetadataDiff = Metadata.readDiffFrom(in);
        // check that currMetadata.currentMaxTableOid is overridden by metadataDiff.currentMaxTableOid which is greater
        var currMetadata = Metadata.builder(120).build();
        assertThat(receivedMetadataDiff.apply(currMetadata).currentMaxTableOid()).isEqualTo(tableOID);

        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_2_0);
        metadataDiff.writeTo(out);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        in.setVersion(Version.V_6_2_0);
        receivedMetadataDiff = Metadata.readDiffFrom(in);
        // check that currMetadata.currentMaxTableOid is not overridden by metadataDiff.currentMaxTableOid which is UNASSIGNED
        currMetadata = Metadata.builder(120).build();
        assertThat(receivedMetadataDiff.apply(currMetadata).currentMaxTableOid()).isEqualTo(120);
    }

    @Test
    public void test_deleted_tables_in_metadataDiff_indices_templates_are_applied_to_source_metadata_schemas() {
        Metadata metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .put(IndexTemplateMetadata.builder(PartitionName.templateName("doc", "t1"))
                .patterns(List.of(PartitionName.templatePrefix("doc", "t1")))
                .settings(Settings.EMPTY)
                .putMapping("{}")
                .build())
            .put(IndexMetadata.builder("t2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                true
            ).build();

        assertThat(metadata.schemas()).isEmpty();
        assertThat(metadata.indices().size()).isEqualTo(1);
        assertThat(metadata.indices().keySet().iterator().next()).isEqualTo("t2");
        assertThat(metadata.templates().size()).isEqualTo(1);
        assertThat(metadata.templates().keySet().iterator().next()).isEqualTo(".partitioned.t1.");

        var mdBuilder = Metadata.builder(metadata);
        Metadata upgraded = mdBuilder
            .setTable(
                new RelationName("doc", "t1"),
                List.of(),
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build(),
                null,
                ColumnPolicy.STRICT,
                null,
                Map.of(),
                List.of(),
                List.of(ColumnIdent.of("col1")),
                IndexMetadata.State.OPEN,
                List.of(),
                1L,
                mdBuilder.tableOidSupplier().nextOid())
            .setTable(
                new RelationName("doc", "t2"),
                List.of(),
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build(),
                null,
                ColumnPolicy.STRICT,
                null,
                Map.of(),
                List.of(),
                List.of(),
                IndexMetadata.State.OPEN,
                List.of("t2"),
                2L,
                mdBuilder.tableOidSupplier().nextOid()
            ).build();

        assertThat(upgraded.schemas().get("doc").relations().size()).isEqualTo(2);
        assertThat(upgraded.schemas().get("doc").relations().get("t1")).isNotNull();
        assertThat(upgraded.schemas().get("doc").relations().get("t2")).isNotNull();
        assertThat(upgraded.indices().size()).isEqualTo(1);
        assertThat(upgraded.indices().keySet().iterator().next()).isEqualTo("t2");
        assertThat(upgraded.templates().size()).isEqualTo(1);
        assertThat(upgraded.templates().keySet().iterator().next()).isEqualTo(".partitioned.t1.");

        // diff that holds table(t1, t2) deletes
        var metadataDiff = Metadata.EMPTY_METADATA.diff(Version.CURRENT, metadata);

        Metadata deletesApplied = metadataDiff.apply(upgraded);

        assertThat(deletesApplied.indices().size()).isEqualTo(0);
        assertThat(deletesApplied.templates().size()).isEqualTo(0);
        assertThat(deletesApplied.schemas().size()).isEqualTo(0); // make sure deleted from schemas as well
    }

    @Test
    public void test_bwc_streaming_views() throws IOException {
        Metadata metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .setView(
                new RelationName("mySchema", "myView"),
                "SELECT 10",
                "John",
                SearchPath.createSearchPathFrom("mySchema", "doc"),
                true
            )
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_6_2_0);
        metadata.writeTo(out);
        var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        in.setVersion(Version.V_6_2_0);
        Metadata receivedMetadata = Metadata.readFrom(in);
        assertThat(receivedMetadata.relations(RelationMetadata.View.class)).containsExactly(
            new RelationMetadata.View(
                new RelationName("mySchema", "myView"),
                "SELECT 10",
                "John",
                SearchPath.createSearchPathFrom("mySchema", "doc"),
                true
            )
        );
        assertThat((ViewsMetadata) receivedMetadata.custom(ViewsMetadata.TYPE)).isNull();
    }

    @Test
    public void test_bwc_streaming_foreign_tables() throws IOException {
        var name = new RelationName("mySchema", "myFT");
        List<Reference> references = List.of(new SimpleReference(
            name,
            ColumnIdent.of("col"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null)
        );
        Metadata metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .setForeignTable(
                name,
                references,
                "myServer",
                Settings.builder().put("foo", "bar").build()
            )
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_6_2_0);
        metadata.writeTo(out);
        var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        in.setVersion(Version.V_6_2_0);
        Metadata receivedMetadata = Metadata.readFrom(in);
        assertThat(receivedMetadata.relations(RelationMetadata.ForeignTable.class)).containsExactly(
            new RelationMetadata.ForeignTable(
                name,
                references.stream().collect(Collectors.toMap(Reference::column, x -> x)),
                "myServer",
                Settings.builder().put("foo", "bar").build()
            )
        );
        assertThat((ForeignTablesMetadata) receivedMetadata.custom(ForeignTablesMetadata.TYPE)).isNull();
    }

    @Test
    public void test_bwc_streaming_udfs() throws Exception {
        UserDefinedFunctionMetadata udf = new UserDefinedFunctionMetadata(
            "my_schema",
            "foo",
            List.of(FunctionArgumentDefinition.of(DataTypes.INTEGER)),
            DataTypes.INTEGER,
            "x-lang",
            "return 42"
        );
        Metadata metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .setUDF(udf)
            .build();
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_6_2_0);
            metadata.writeTo(out);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                in.setVersion(Version.V_6_2_0);
                Metadata receivedMetadata = Metadata.readFrom(in);
                UserDefinedFunctionsMetadata udfMetadata = receivedMetadata.custom(UserDefinedFunctionsMetadata.TYPE);
                assertThat(udfMetadata).isNull();
                SchemaMetadata schemaMetadata = receivedMetadata.schemas().get("my_schema");
                assertThat(schemaMetadata).isNotNull();
                assertThat(schemaMetadata.udfs()).containsExactly(udf);
            }
        }
    }

    @Test
    public void test_preserves_udfs_if_customs_from_6_2_contain_no_deletes() throws Exception {
        UserDefinedFunctionMetadata udf1 = new UserDefinedFunctionMetadata(
            "my_schema",
            "foo",
            List.of(FunctionArgumentDefinition.of(DataTypes.INTEGER)),
            DataTypes.INTEGER,
            "x-lang",
            "return 42"
        );
        Metadata v62Metadata1 = Metadata.builder(Metadata.OID_UNASSIGNED)
            .customs(Map.of(UserDefinedFunctionsMetadata.TYPE, UserDefinedFunctionsMetadata.of(udf1)))
            // Add view in the schema to simulate a change in the SchemaMetadata
            .setView(
                new RelationName("my_schema", "v1"),
                "select 1",
                Role.CRATE_USER.name(),
                SearchPath.pathWithPGCatalogAndDoc(),
                false
            )
            .build();

        Metadata v62Metadata2 = Metadata.builder(Metadata.OID_UNASSIGNED)
            .customs(Map.of(UserDefinedFunctionsMetadata.TYPE, UserDefinedFunctionsMetadata.of(udf1)))
            .build();

        Metadata v63Metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .setUDF(udf1)
            .build();

        try (var out = new BytesStreamOutput()) {
            Version version = Version.V_6_2_2;
            out.setVersion(version);

            Diff<Metadata> diff = v62Metadata2.diff(version, v62Metadata1);
            diff.writeTo(out);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                in.setVersion(version);
                Diff<Metadata> diffFrom = Metadata.readDiffFrom(in);
                Metadata receivedMetadata = diffFrom.apply(v63Metadata);
                UserDefinedFunctionsMetadata udfMetadata = receivedMetadata.custom(UserDefinedFunctionsMetadata.TYPE);
                assertThat(udfMetadata).isNull();
                SchemaMetadata schemaMetadata = receivedMetadata.schemas().get("my_schema");
                assertThat(schemaMetadata).isNotNull();
                assertThat(schemaMetadata.udfs()).containsExactly(udf1);
            }
        }
    }

    @Test
    public void test_preserves_udfs_if_customs_from_6_2_delete_udf_subset() throws Exception {
        UserDefinedFunctionMetadata udf1 = new UserDefinedFunctionMetadata(
            "my_schema",
            "foo",
            List.of(FunctionArgumentDefinition.of(DataTypes.INTEGER)),
            DataTypes.INTEGER,
            "x-lang",
            "return 42"
        );
        UserDefinedFunctionMetadata udf2 = new UserDefinedFunctionMetadata(
            "my_schema",
            "bar",
            List.of(FunctionArgumentDefinition.of(DataTypes.INTEGER)),
            DataTypes.INTEGER,
            "x-lang",
            "return 24"
        );
        Metadata v62Metadata1 = Metadata.builder(Metadata.OID_UNASSIGNED)
            .customs(Map.of(UserDefinedFunctionsMetadata.TYPE, UserDefinedFunctionsMetadata.of(udf1, udf2)))
            // Add view in the schema to simulate a change in the SchemaMetadata
            .setView(
                new RelationName("my_schema", "v1"),
                "select 1",
                Role.CRATE_USER.name(),
                SearchPath.pathWithPGCatalogAndDoc(),
                false
            )
            .build();

        Metadata v62Metadata2 = Metadata.builder(Metadata.OID_UNASSIGNED)
            .customs(Map.of(UserDefinedFunctionsMetadata.TYPE, UserDefinedFunctionsMetadata.of(udf1)))
            .build();

        Metadata v63Metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .setUDF(udf1)
            .setUDF(udf2)
            .build();

        try (var out = new BytesStreamOutput()) {
            Version version = Version.V_6_2_2;
            out.setVersion(version);

            Diff<Metadata> diff = v62Metadata2.diff(version, v62Metadata1);
            diff.writeTo(out);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                in.setVersion(version);
                Diff<Metadata> diffFrom = Metadata.readDiffFrom(in);
                Metadata receivedMetadata = diffFrom.apply(v63Metadata);
                UserDefinedFunctionsMetadata udfMetadata = receivedMetadata.custom(UserDefinedFunctionsMetadata.TYPE);
                assertThat(udfMetadata).isNull();
                SchemaMetadata schemaMetadata = receivedMetadata.schemas().get("my_schema");
                assertThat(schemaMetadata).isNotNull();
                assertThat(schemaMetadata.udfs()).containsExactly(udf1);
            }
        }
    }

    @Test
    public void test_tables_without_col_oids_do_not_get_assigned_oids() {
        RelationName tableName = new RelationName(DocSchemaInfo.NAME, "test");
        Reference col1 = new SimpleReference(tableName, ColumnIdent.of("col1"), RowGranularity.DOC, DataTypes.INTEGER, 1, null);
        Reference col2 = new SimpleReference(tableName, ColumnIdent.of("col2"), RowGranularity.DOC, DataTypes.STRING, 2, null);
        List<Reference> columns = List.of(col1, col2);
        RelationMetadata.Table table = new RelationMetadata.Table(
            123,
            tableName,
            columns,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_4_8).build(),
            null,
            ColumnPolicy.DYNAMIC,
            null,
            Map.of(),
            List.of(),
            List.of(),
            IndexMetadata.State.OPEN,
            List.of(UUIDs.randomBase64UUID()),
            0
        );
        Metadata.Builder builder = Metadata.builder(Metadata.OID_UNASSIGNED);
        builder.setRelation(table);

        builder.setTable(
            tableName,
            columns,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_4_8).build(),
            null,
            ColumnPolicy.DYNAMIC,
            null,
            Map.of(),
            List.of(),
            List.of(),
            IndexMetadata.State.OPEN,
            List.of(UUIDs.randomBase64UUID()),
            table.tableVersion() + 1,
            table.oid()
        );
        List<Reference> newCols = ((RelationMetadata.Table) builder.getRelation(tableName)).columns();
        assertThat(newCols).containsExactlyInAnyOrder(col1, col2);
    }
}
