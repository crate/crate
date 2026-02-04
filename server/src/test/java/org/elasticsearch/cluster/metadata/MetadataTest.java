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
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.test.ESTestCase.settings;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;

public class MetadataTest {

    @Test
    public void test_bwc_read_writes_with_6_1_0() throws Exception {
        Metadata metadata = Metadata.builder()
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
    public void test_deleted_tables_in_metadataDiff_indices_templates_are_applied_to_source_metadata_schemas() {
        String t2UUID = UUIDs.randomBase64UUID();
        Metadata metadata = Metadata.builder()
            .put(IndexTemplateMetadata.builder(PartitionName.templateName("doc", "t1"))
                .patterns(List.of(PartitionName.templatePrefix("doc", "t1")))
                .settings(Settings.EMPTY)
                .putMapping("{}")
                .build())
            .put(IndexMetadata.builder("t2")
                    .settings(settings(Version.CURRENT).put(SETTING_INDEX_UUID, t2UUID))
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
                1L)
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
                List.of(t2UUID),
                2L
            ).build();

        assertThat(upgraded.schemas().get("doc").relations().size()).isEqualTo(2);
        assertThat(upgraded.schemas().get("doc").relations().get("t1")).isNotNull();
        assertThat(upgraded.schemas().get("doc").relations().get("t2")).isNotNull();
        assertThat(upgraded.indices().size()).isEqualTo(1);
        assertThat(upgraded.indices().keysIt().next()).isEqualTo("t2");
        assertThat(upgraded.templates().size()).isEqualTo(1);
        assertThat(upgraded.templates().keysIt().next()).isEqualTo(".partitioned.t1.");

        // diff that holds table(t1, t2) deletes
        var metadataDiff = Metadata.EMPTY_METADATA.diff(metadata);

        Metadata deletesApplied = metadataDiff.apply(upgraded);

        assertThat(deletesApplied.indices().size()).isEqualTo(0);
        assertThat(deletesApplied.templates().size()).isEqualTo(0);
        assertThat(deletesApplied.schemas().size()).isEqualTo(0); // make sure deleted from schemas as well
    }
}
