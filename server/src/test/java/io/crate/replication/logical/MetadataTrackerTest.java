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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class MetadataTrackerTest extends ESTestCase {

    IndexMetadata buildIndexMetadata() throws IOException {
        var mappingMetadata = new MappingMetadata("test", Map.of("1", "one"));
        return IndexMetadata.builder("test").putMapping(mappingMetadata)
            .settings(settings(Version.CURRENT))
            .version(1)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    ClusterState buildPublisherClusterState(IndexMetadata indexMetadata) {
        PublicationsMetadata publicationsMetadata = new PublicationsMetadata(
            Map.of("pub1", new Publication("user", false, List.of(new RelationName("doc", "test"))))
        );

        return ClusterState.builder(new ClusterName("publisher"))
            .version(1L)
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(PublicationsMetadata.TYPE, publicationsMetadata).build())
            .build();
    }

    ClusterState buildSubscriberClusterState(IndexMetadata indexMetadata) {
        var subscriptionsMetadata = new SubscriptionsMetadata(
            Map.of("sub1", new Subscription(
                       "user1",
                       ConnectionInfo.fromURL("crate://example.com:4310?user=valid_user&password=123"),
                       List.of("pub1"),
                       Settings.EMPTY
                   )
            )
        );

        return ClusterState.builder(new ClusterName("subscriber"))
            .version(1L)
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(SubscriptionsMetadata.TYPE, subscriptionsMetadata).build())
            .build();
    }

    @Test
    public void test_mappings_is_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var publisherMetadata = buildIndexMetadata();
        var publisherClusterState = buildPublisherClusterState(publisherMetadata);
        var subscriberMetadata = buildIndexMetadata();
        var subscriberClusterState = buildSubscriberClusterState(subscriberMetadata);

        var syncedSubriberClusterState = MetadataTracker.updateIndexMetadata("sub1",
                                                                             subscriberClusterState,
                                                                             publisherClusterState,
                                                                             IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
        // Nothing in the indexMetadata changed, so the clusterstate must be equal
        assertThat(subscriberClusterState, is(syncedSubriberClusterState));


        // Let's change the mapping on the publisher publisherClusterState
        var updatedMappingMetadata = new MappingMetadata("test", Map.of("1", "one", "2", "two"));
        var updatedPublisherMetadata = IndexMetadata.builder(buildIndexMetadata()).putMapping(updatedMappingMetadata).mappingVersion(
            2L).build();
        var updatedPublisherClusterState = buildPublisherClusterState(updatedPublisherMetadata);

        syncedSubriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            subscriberClusterState,
            updatedPublisherClusterState,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        assertThat(subscriberClusterState, is(not(syncedSubriberClusterState)));
        var syncedIndexMetadata = syncedSubriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.mapping(), is(updatedPublisherMetadata.mapping()));
        assertThat(updatedPublisherMetadata.mapping(), is(updatedMappingMetadata));

    }

    @Test
    public void test_dynamic_setting_is_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var publisherMetadata = buildIndexMetadata();
        var subscriberMetadata = buildIndexMetadata();
        var subscriberClusterState = buildSubscriberClusterState(subscriberMetadata);

        var settingsBuilder = settings(Version.CURRENT).put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 5);
        var updatedPublisherMetadata = IndexMetadata.builder(publisherMetadata).settings(settingsBuilder)
            .numberOfShards(1)
            .numberOfReplicas(0).
            version(1)
            .build();

        var updatedPublisherClusterState = buildPublisherClusterState(updatedPublisherMetadata);
        var syncedSubriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            subscriberClusterState,
            updatedPublisherClusterState,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.getSettings(), is(updatedPublisherMetadata.getSettings()));
        assertThat(syncedIndexMetadata.getSettings().getAsInt(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), null),
                   is(5));
    }

    @Test
    public void test_private_setting_is_not_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var publisherMetadata = buildIndexMetadata();
        var subscriberMetadata = buildIndexMetadata();
        var subscriberClusterState = buildSubscriberClusterState(subscriberMetadata);

        var publisherIndexUuid = UUIDs.randomBase64UUID();
        var settingsBuilder = settings(Version.CURRENT).put(SETTING_INDEX_UUID, publisherIndexUuid);
        var updatedPublisherMetadata = IndexMetadata.builder(publisherMetadata).settings(settingsBuilder)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .version(1)
            .build();
        var updatedPublisherClusterState = buildPublisherClusterState(updatedPublisherMetadata);
        var syncedSubriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            subscriberClusterState,
            updatedPublisherClusterState,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.getSettings(), is(not(updatedPublisherMetadata.getSettings())));
        assertThat(syncedIndexMetadata.getSettings().get(SETTING_INDEX_UUID, "default"), is(not(publisherIndexUuid)));
    }

    @Test
    public void test_non_replicatable_setting_is_not_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var publisherMetadata = buildIndexMetadata();
        var subscriberMetadata = buildIndexMetadata();
        var subscriberClusterState = buildSubscriberClusterState(subscriberMetadata);

        var settingsBuilder = settings(Version.CURRENT).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 5);
        var updatedPublisherMetadata = IndexMetadata.builder(publisherMetadata).settings(settingsBuilder)
            .numberOfShards(1)
            .version(1)
            .build();

        var updatedPublisherClusterState = buildPublisherClusterState(updatedPublisherMetadata);
        var syncedSubriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            subscriberClusterState,
            updatedPublisherClusterState,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.getSettings(), is(not(updatedPublisherMetadata.getSettings())));
        assertThat(INDEX_NUMBER_OF_REPLICAS_SETTING.get(syncedIndexMetadata.getSettings()), is(0));
    }

}
