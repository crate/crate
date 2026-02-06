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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.test.ESTestCase.settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.exceptions.DependentObjectsExists;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.fdw.ForeignTablesMetadata;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

public class MetadataTest {

    @Test
    public void test_bwc_read_writes_with_6_1_0() throws Exception {
        Metadata metadata = new Metadata.Builder(Metadata.OID_UNASSIGNED)
                .columnOID(123L)
                // builder() adds IndexGraveyard custom, which causes "can't read named writeable from StreamInput" error on reads.
                // In production NamedWriteableAwareStreamInput is used.
                // Resetting it here for simplicity as it's irrelevant for the test.
                .removeCustom(IndexGraveyard.TYPE)
                .build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("6.1.0"));
        metadata.writeTo(out); // OID should be written, 6.1.0 expects it.
        var in = out.bytes().streamInput();
        in.setVersion(Version.fromString("6.1.0"));
        Metadata recievedMetadata = Metadata.readFrom(in); // We are reading from 6.1.0, which sends out OID.
        assertThat(recievedMetadata.columnOID()).isEqualTo(123L);
    }

    @Test
    public void test_cant_drop_schema_with_dependent_view() throws Exception {
        assertThatThrownBy(() ->
            new Metadata.Builder(Metadata.OID_UNASSIGNED)
                .createSchema("foo")
                .putCustom(
                    ViewsMetadata.TYPE,
                    ViewsMetadata.addOrReplace(
                        null,
                        new RelationName("foo", "bar"),
                        "select 1",
                        null,
                        SearchPath.pathWithPGCatalogAndDoc(),
                        true
                    ))
                .dropSchema("foo")
        ).isExactlyInstanceOf(DependentObjectsExists.class);
    }

    @Test
    public void test_cant_drop_schema_with_dependent_udf() throws Exception {
        assertThatThrownBy(() ->
            new Metadata.Builder(Metadata.OID_UNASSIGNED)
                .createSchema("foo")
                .putCustom(
                    UserDefinedFunctionsMetadata.TYPE,
                    UserDefinedFunctionsMetadata.of(
                        new UserDefinedFunctionMetadata("foo", "bar", List.of(), DataTypes.INTEGER, "js", "")
                    )
                )
                .dropSchema("foo")
        ).isExactlyInstanceOf(DependentObjectsExists.class);
    }

    @Test
    public void test_cant_drop_schema_with_dependent_foreign_table() throws Exception {
        assertThatThrownBy(() ->
            new Metadata.Builder(Metadata.OID_UNASSIGNED)
                .createSchema("foo")
                .putCustom(
                    ForeignTablesMetadata.TYPE,
                    ForeignTablesMetadata.EMPTY.add(
                        new RelationName("foo", "bar"),
                        List.of(),
                        "server1",
                        Settings.EMPTY
                    )
                )
                .dropSchema("foo")
        ).isExactlyInstanceOf(DependentObjectsExists.class);
    }

    @Test
    public void test_streaming_table_oid() throws IOException {
        int tableOID = 123;
        Metadata metadata = Metadata.builder(tableOID)
            // builder() adds IndexGraveyard custom, which causes "can't read named writeable from StreamInput" error on reads.
            // In production NamedWriteableAwareStreamInput is used.
            // Resetting it here for simplicity as it's irrelevant for the test.
            .removeCustom(IndexGraveyard.TYPE)
            .build();
        assertThat(metadata.currentMaxTableOid()).isEqualTo(tableOID);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("6.2.0"));
        metadata.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.fromString("6.2.0"));
        Metadata recievedMetadata = Metadata.readFrom(in);
        assertThat(recievedMetadata.currentMaxTableOid()).isEqualTo(Metadata.OID_UNASSIGNED);

        out = new BytesStreamOutput();
        metadata.writeTo(out);
        in = out.bytes().streamInput();
        recievedMetadata = Metadata.readFrom(in);
        assertThat(recievedMetadata.currentMaxTableOid()).isEqualTo(tableOID);

        // diff streaming
        Metadata prevMetadata = Metadata.builder(100)
            .removeCustom(IndexGraveyard.TYPE)
            .build();
        var metadataDiff = metadata.diff(Version.CURRENT, prevMetadata);

        out = new BytesStreamOutput();
        metadataDiff.writeTo(out);
        in = out.bytes().streamInput();
        var receivedMetadataDiff = Metadata.readDiffFrom(in);
        var emptyMetadata = Metadata.builder(777).build(); // check tableOID is overwritten
        assertThat(receivedMetadataDiff.apply(emptyMetadata).currentMaxTableOid()).isEqualTo(tableOID);

        out = new BytesStreamOutput();
        out.setVersion(Version.fromString("6.2.0"));
        metadataDiff.writeTo(out);
        in = out.bytes().streamInput();
        in.setVersion(Version.fromString("6.2.0"));
        receivedMetadataDiff = Metadata.readDiffFrom(in);
        emptyMetadata = Metadata.builder(777).build(); // check tableOID is overwritten
        assertThat(receivedMetadataDiff.apply(emptyMetadata).currentMaxTableOid()).isEqualTo(Metadata.OID_UNASSIGNED);
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
        assertThat(metadata.indices().keysIt().next()).isEqualTo("t2");
        assertThat(metadata.templates().size()).isEqualTo(1);
        assertThat(metadata.templates().keysIt().next()).isEqualTo(".partitioned.t1.");

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
        assertThat(upgraded.indices().keysIt().next()).isEqualTo("t2");
        assertThat(upgraded.templates().size()).isEqualTo(1);
        assertThat(upgraded.templates().keysIt().next()).isEqualTo(".partitioned.t1.");

        // diff that holds table(t1, t2) deletes
        var metadataDiff = Metadata.EMPTY_METADATA.diff(Version.CURRENT, metadata);

        Metadata deletesApplied = metadataDiff.apply(upgraded);

        assertThat(deletesApplied.indices().size()).isEqualTo(0);
        assertThat(deletesApplied.templates().size()).isEqualTo(0);
        assertThat(deletesApplied.schemas().size()).isEqualTo(0); // make sure deleted from schemas as well
    }
}
