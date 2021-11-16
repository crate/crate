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

package io.crate.replication.logical;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class MetadataTrackerTest extends ESTestCase {

    @Test
    public void test_index_mappings_is_transfered_between_two_clusterstates() throws Exception {
        var mappingMetadata = new MappingMetadata("test", Map.of("1", "one"));
        var indexMetadata = IndexMetadata.builder("test").putMapping(mappingMetadata)
            .settings(settings(Version.CURRENT))
            .version(1)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        PublicationsMetadata publicationsMetadata = new PublicationsMetadata(
            Map.of("pub1", new Publication("user", false, List.of(new RelationName("doc", "test"))))
        );

        var publisherClusterState = ClusterState.builder(new ClusterName("publisher"))
            .version(1L)
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(PublicationsMetadata.TYPE, publicationsMetadata).build())
            .build();

        var subscriptionsMetadata = new SubscriptionsMetadata(
            Map.of("sub1", new Subscription(
                       "user1",
                       ConnectionInfo.fromURL("crate://example.com:4310?user=valid_user&password=123"),
                       List.of("pub1"),
                       Settings.EMPTY
                   )
            )
        );

        var subscriberClusterState = ClusterState.builder(new ClusterName("subscriber"))
            .version(1L)
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(SubscriptionsMetadata.TYPE, subscriptionsMetadata).build())
            .build();

        var syncedSubriberClusterState = MetadataTracker.updateMetadata("sub1", subscriberClusterState, publisherClusterState);
        // Nothing in the indexMetadata changed, so the clusterstate must be equal
        assertThat(subscriberClusterState, is(syncedSubriberClusterState));


        //Now lets change the mapping on the publisher publisherClusterState
        var updatedMappingMetadata = new MappingMetadata("test", Map.of("1", "one", "2", "two"));
        var updatedIndexMetadata = IndexMetadata.builder(indexMetadata).putMapping(updatedMappingMetadata).mappingVersion(2L).build();
        var updatedPublisherClusterState = ClusterState.builder(publisherClusterState).metadata(Metadata.builder().put(
            updatedIndexMetadata,
            true).putCustom(PublicationsMetadata.TYPE, publicationsMetadata).build()).build();

        syncedSubriberClusterState = MetadataTracker.updateMetadata("sub1", subscriberClusterState, updatedPublisherClusterState);

        assertThat(subscriberClusterState, is(not(syncedSubriberClusterState)));

        var syncedIndexMetadata = syncedSubriberClusterState.metadata().index("test");
        // Verify that mappings are in-sync between updated publisher cluster and new subscriber clusterstate
        assertThat(syncedIndexMetadata.mapping(), is(updatedIndexMetadata.mapping()));
    }
}
