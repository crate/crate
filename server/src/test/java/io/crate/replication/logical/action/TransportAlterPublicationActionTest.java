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

package io.crate.replication.logical.action;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.junit.Test;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.sql.tree.AlterPublication;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportAlterPublicationActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_unknown_table_raises_exception() {
        var pub = new Publication("owner", false, List.of());
        var metadata = Metadata.builder().build();
        var request = new TransportAlterPublicationAction.Request(
            "pub1",
            AlterPublication.Operation.SET,
            List.of(RelationName.fromIndexName("t1"))
        );

        assertThrows(
            RelationUnknown.class,
            () -> TransportAlterPublicationAction.updatePublication(request, metadata, pub)
        );
    }

    @Test
    public void test_set_tables_on_existing_publication() {
        var oldPublication = new Publication("owner", false, List.of(RelationName.fromIndexName("t1")));
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder("t2")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
                true
            )
            .build();
        var request = new TransportAlterPublicationAction.Request(
            "pub1",
            AlterPublication.Operation.SET,
            List.of(RelationName.fromIndexName("t2"))
        );

        var newPublication = TransportAlterPublicationAction.updatePublication(request, metadata, oldPublication);
        assertThat(newPublication, not(oldPublication));
        assertThat(newPublication.tables()).containsExactly(RelationName.fromIndexName("t2"));
    }

    @Test
    public void test_add_table_on_existing_publication() {
        var oldPublication = new Publication("owner", false, List.of(RelationName.fromIndexName("t1")));
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder("t2")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
                true
            )
            .build();
        var request = new TransportAlterPublicationAction.Request(
            "pub1",
            AlterPublication.Operation.ADD,
            List.of(RelationName.fromIndexName("t2"))
        );

        var newPublication = TransportAlterPublicationAction.updatePublication(request, metadata, oldPublication);
        assertThat(newPublication, not(oldPublication));
        assertThat(newPublication.tables(), containsInAnyOrder(RelationName.fromIndexName("t1"), RelationName.fromIndexName("t2")));
    }

    @Test
    public void test_drop_table_from_existing_publication() {
        var oldPublication = new Publication(
            "owner",
            false,
            List.of(RelationName.fromIndexName("t1"), RelationName.fromIndexName("t2"))
        );
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder("t2")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
                true
            )
            .build();
        var request = new TransportAlterPublicationAction.Request(
            "pub1",
            AlterPublication.Operation.DROP,
            List.of(RelationName.fromIndexName("t2"))
        );

        var newPublication = TransportAlterPublicationAction.updatePublication(request, metadata, oldPublication);
        assertThat(newPublication, not(oldPublication));
        assertThat(newPublication.tables(), containsInAnyOrder(RelationName.fromIndexName("t1")));
    }
}
