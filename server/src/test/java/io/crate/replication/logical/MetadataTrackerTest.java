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

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.MetadataTracker.RestoreDiff;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.PublicationsStateAction.Response;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.role.Role;

public class MetadataTrackerTest extends ESTestCase {

    private ClusterState PUBLISHER_CLUSTER_STATE;
    private ClusterState SUBSCRIBER_CLUSTER_STATE;
    private PublicationsStateAction.Response publicationsStateResponse;
    private RelationName testTable;

    private static class Builder {

        private ClusterState clusterState;

        public Builder(String clusterName) {
            clusterState = ClusterState.builder(new ClusterName(clusterName)).build();
        }

        public Builder(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        public Builder addTable(String name, Map<String, Object> mapping, Settings settings) throws IOException {
            var indexMetadata = IndexMetadata.builder(name)
                .putMapping(new MappingMetadata(mapping))
                .settings(settings(Version.CURRENT).put(settings))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .put(indexMetadata, true))
                .routingTable(RoutingTable.builder(clusterState.routingTable())
                    .add(IndexRoutingTable.builder(indexMetadata.getIndex())
                        .addShard(newShardRouting(name, 0, "dummy_node", true, ShardRoutingState.STARTED))
                        .build())
                    .build())
                .incrementVersion()
                .build();
            return this;
        }

        public Builder addPartition(String alias, String partition) throws IOException {
            Map<String, Object> mapping = Map.of();
            Settings settings = Settings.EMPTY;
            var indexMetadata = IndexMetadata.builder(partition)
                .putMapping(new MappingMetadata(mapping))
                .settings(settings(Version.CURRENT).put(settings))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata(alias))
                .build();
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .put(indexMetadata, true))
                .routingTable(RoutingTable.builder(clusterState.routingTable())
                    .add(IndexRoutingTable.builder(indexMetadata.getIndex())
                        .addShard(newShardRouting(partition, 0, "dummy_node", true, ShardRoutingState.STARTED))
                        .build())
                    .build())
                .incrementVersion()
                .build();
            return this;
        }

        public Builder addReplicatingTable(String subscriptionName,
                                           String name,
                                           Map<String, Object> mapping,
                                           Settings settings) throws IOException {
            var newSettings = Settings.builder()
                .put(settings)
                .put(REPLICATION_SUBSCRIPTION_NAME.getKey(), subscriptionName)
                .build();
            return addTable(name, mapping, newSettings);
        }

        public Builder addPartitionedTable(RelationName relation, List<PartitionName> partitions) throws IOException {
            var templateName = PartitionName.templateName(relation.schema(), relation.name());
            var newTemplateMetadata = IndexTemplateMetadata
                .builder(templateName)
                .patterns(Collections.singletonList(PartitionName.templatePrefix(relation.schema(), relation.name())))
                .putAlias(new AliasMetadata(relation.indexNameOrAlias()))
                .putMapping("{\"default\": {}}")
                .build();

            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .put(newTemplateMetadata))
                .incrementVersion()
                .build();

            for (var partitionName : partitions) {
                addPartition(relation.indexNameOrAlias(), partitionName.asIndexName());
            }

            return this;
        }

        public Builder updateTableMapping(String name, Map<String, Object> newMapping) throws IOException {
            var indexMetadata = clusterState.metadata().index(name);
            var newIndexMetadata = IndexMetadata.builder(indexMetadata)
                .putMapping(new MappingMetadata(newMapping))
                .mappingVersion(2L)
                .build();
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .put(newIndexMetadata, true))
                .incrementVersion()
                .build();
            return this;
        }

        public Builder updateTableSettings(String name, Settings newSettings) throws IOException {
            var indexMetadata = clusterState.metadata().index(name);
            var updatedSettings = Settings.builder()
                .put(indexMetadata.getSettings())
                .put(newSettings)
                .build();
            var newIndexMetadata = IndexMetadata.builder(indexMetadata)
                .settings(updatedSettings)
                .build();
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .put(newIndexMetadata, true))
                .incrementVersion()
                .build();
            return this;
        }

        public Builder addSubscription(String name, List<String> publications, List<String> replicatingRelations) {
            var subscription = new Subscription(
                "user1",
                ConnectionInfo.fromURL("crate://localhost"),
                publications,
                Settings.EMPTY,
                replicatingRelations.stream().map(RelationName::fromIndexName)
                    .collect(Collectors.toMap(rn -> rn,
                                              rn -> new Subscription.RelationState(Subscription.State.MONITORING,
                                                                                   null)))
            );
            var subscriptionsMetadata = SubscriptionsMetadata.newInstance(
                clusterState.metadata().custom(SubscriptionsMetadata.TYPE));
            subscriptionsMetadata.subscription().put(name, subscription);

            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .putCustom(SubscriptionsMetadata.TYPE, subscriptionsMetadata)
                              .build())
                .incrementVersion()
                .build();
            return this;
        }

        public Builder addPublication(String name, List<String> relations) {
            var publication = new Publication(
                "user",
                relations.isEmpty(),
                relations.stream().map(RelationName::fromIndexName).toList()
            );
            var publicationsMetadata = PublicationsMetadata.newInstance(
                clusterState.metadata().custom(PublicationsMetadata.TYPE));
            publicationsMetadata.publications().put(name, publication);

            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata())
                              .putCustom(PublicationsMetadata.TYPE, publicationsMetadata)
                              .build())
                .incrementVersion()
                .build();
            return this;
        }

        public ClusterState build() {
            return clusterState;
        }

    }


    @Before
    public void setUpStates() throws Exception {
        PUBLISHER_CLUSTER_STATE = new Builder("publisher")
            .addTable("test", Map.of("1", "one"), Settings.EMPTY)
            .addPublication("pub1", List.of("test"))
            .build();

        testTable = new RelationName("doc", "test");
        publicationsStateResponse = new Response(Map.of(
            testTable,
            RelationMetadata.fromMetadata(testTable, PUBLISHER_CLUSTER_STATE.metadata(), ignored -> true)), List.of());

        SUBSCRIBER_CLUSTER_STATE = new Builder("subscriber")
            .addReplicatingTable("sub1", "test", Map.of("1", "one"), Settings.EMPTY)
            .addSubscription("sub1", List.of("pub1"), List.of("test"))
            .build();
    }

    @Test
    public void test_mappings_is_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var syncedSubscriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            publicationsStateResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
        // Nothing in the indexMetadata changed, so the cluster state must be equal
        assertThat(SUBSCRIBER_CLUSTER_STATE, is(syncedSubscriberClusterState));

        // Let's change the mapping on the publisher publisherClusterState
        Map<String, Object> updatedMapping = Map.of("1", "one", "2", "two");
        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableMapping("test", updatedMapping)
            .build();

        var updatedResponse = new Response(
            Map.of(
                testTable,
                RelationMetadata.fromMetadata(testTable, updatedPublisherClusterState.metadata(), ignored -> true)
            ),
            List.of()
        );

        syncedSubscriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        assertThat(SUBSCRIBER_CLUSTER_STATE, is(not(syncedSubscriberClusterState)));
        var syncedIndexMetadata = syncedSubscriberClusterState.metadata().index("test");
        var updatedPublisherMetadata = updatedPublisherClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.mapping(), is(updatedPublisherMetadata.mapping()));
    }

    @Test
    public void test_dynamic_setting_is_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var newSettings = Settings.builder().put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 5).build();

        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableSettings("test", newSettings)
            .build();
        var updatedResponse = new Response(
            Map.of(
                testTable,
                RelationMetadata.fromMetadata(testTable, updatedPublisherClusterState.metadata(), ignored -> true)
            ),
            List.of()
        );
        var syncedSubscriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubscriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.getSettings().getAsInt(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), null),
                   is(5));
    }

    @Test
    public void test_private_setting_is_not_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var publisherIndexUuid = UUIDs.randomBase64UUID();
        var newSettings = Settings.builder().put(SETTING_INDEX_UUID, publisherIndexUuid).build();

        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableSettings("test", newSettings)
            .build();
        var updatedResponse = new Response(
            Map.of(
                testTable,
                RelationMetadata.fromMetadata(testTable, updatedPublisherClusterState.metadata(), ignored -> true)
            ),
            List.of()
        );

        var syncedSubscriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubscriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.getSettings().get(SETTING_INDEX_UUID, "default"), is(not(publisherIndexUuid)));
    }

    @Test
    public void test_non_replicatable_setting_is_not_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var newSettings = Settings.builder().put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 5).build();
        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableSettings("test", newSettings)
            .build();
        var updatedResponse = new Response(
            Map.of(
                testTable,
                RelationMetadata.fromMetadata(testTable, updatedPublisherClusterState.metadata(), ignored -> true)
            ),
            List.of()
        );

        var syncedSubscriberClusterState = MetadataTracker.updateIndexMetadata(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubscriberClusterState.metadata().index("test");
        assertThat(INDEX_NUMBER_OF_REPLICAS_SETTING.get(syncedIndexMetadata.getSettings()), is(0));
    }

    @Test
    public void test_restore_and_state_update_has_no_new_relations() {
        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            publicationsStateResponse
        );
        assertThat(restoreDiff.relationsForStateUpdate(), empty());
    }

    @Test
    public void test_new_table_of_publication_for_all_tables_will_be_restored() throws Exception {
        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .build();

        var publisherState = new Builder("publisher")
            .addTable("t2", Map.of(), Settings.EMPTY)
            .addPublication("pub1", List.of("t2"))
            .build();

        RelationName table = new RelationName("doc", "t2");
        var updatedResponse = new Response(
            Map.of(
                table,
                RelationMetadata.fromMetadata(table, publisherState.metadata(), ignored -> true)
            ),
            List.of()
        );

        RestoreDiff restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            updatedResponse
        );
        assertThat(restoreDiff.relationsForStateUpdate(), contains(RelationName.fromIndexName("t2")));
        assertThat(restoreDiff.indexNamesToRestore(), contains("t2"));
        assertThat(restoreDiff.templatesToRestore(), empty());
    }

    @Test
    public void test_new_empty_partitioned_table_of_publication_for_all_tables_will_be_restored() throws Exception {
        var newRelation = RelationName.fromIndexName("p1");
        var templateName = PartitionName.templateName(newRelation.schema(), newRelation.name());

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .build();

        RelationName p1 = RelationName.fromIndexName("p1");
        var publisherState = new Builder("publisher")
            .addPartitionedTable(p1, List.of())
            .addPublication("pub1", List.of(p1.indexNameOrAlias()))
            .build();
        var publisherStateResponse = new Response(
            Map.of(p1, RelationMetadata.fromMetadata(p1, publisherState.metadata(), ignored -> true)),
            List.of()
        );

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );
        assertThat(restoreDiff.relationsForStateUpdate(), contains(RelationName.fromIndexName("p1")));
        assertThat(restoreDiff.indexNamesToRestore(), empty());
        assertThat(restoreDiff.templatesToRestore(), contains(templateName));
    }

    @Test
    public void test_new_partitioned_table_with_partition_of_publication_for_all_tables_will_be_restored() throws Exception {
        var newRelation = RelationName.fromIndexName("p1");
        var newPartitionName = new PartitionName(newRelation, "dummy");
        var templateName = PartitionName.templateName(newRelation.schema(), newRelation.name());

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .build();

        var publisherState = new Builder("publisher")
            .addPublication("pub1", List.of(newRelation.indexNameOrAlias()))
            .addPartitionedTable(newRelation, List.of(newPartitionName))
            .build();
        RelationMetadata relationMetadata = RelationMetadata.fromMetadata(newRelation, publisherState.metadata(), ignored -> true);
        var publisherStateResponse = new Response(Map.of(newRelation, relationMetadata), List.of());

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );

        assertThat(restoreDiff.relationsForStateUpdate(), contains(newRelation));
        assertThat(restoreDiff.indexNamesToRestore(), contains(newPartitionName.asIndexName()));
        assertThat(restoreDiff.templatesToRestore(), contains(templateName));
    }

    @Test
    public void test_new_partition_of_already_replicating_partition_table_will_be_restored() throws Exception {
        var relationName = RelationName.fromIndexName("p1");
        var newPartitionName = new PartitionName(relationName, "dummy");

        var subscriberClusterState = new Builder("subscriber")
            .addPartitionedTable(relationName, List.of())
            .addSubscription("sub1", List.of("pub1"), List.of("p1"))
            .build();

        var publisherState = new Builder("publisher")
            .addPublication("pub1", List.of(relationName.indexNameOrAlias()))
            .addPartitionedTable(relationName, List.of(newPartitionName))
            .build();
        RelationMetadata relationMetadata = RelationMetadata.fromMetadata(relationName, publisherState.metadata(), ignored -> true);
        var publisherStateResponse = new Response(Map.of(relationName, relationMetadata), List.of());

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );

        assertThat(restoreDiff.relationsForStateUpdate(), contains(relationName));
        assertThat(restoreDiff.indexNamesToRestore(), contains(newPartitionName.asIndexName()));
        assertThat(restoreDiff.templatesToRestore(), empty());
    }

    @Test
    public void test_new_partitioned_table_of_publication_with_concrete_tables_will_be_restored() throws Exception {
        var newRelationName = RelationName.fromIndexName("p1");
        var newPartitionName = new PartitionName(newRelationName, "dummy");
        var newTemplateName = PartitionName.templateName(newRelationName.schema(), newRelationName.name());

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of("t1"))
            .addReplicatingTable("sub1", "t1", Map.of(), Settings.EMPTY)
            .build();

        var publisherState = new Builder("publisher")
            .addPublication("pub1", List.of("p1", "t1"))
            .addTable("t1", Map.of(), Settings.EMPTY)
            .addPartitionedTable(newRelationName, List.of(newPartitionName))
            .build();

        PublicationsMetadata publicationsMetadata = publisherState.metadata().custom(PublicationsMetadata.TYPE);
        Publication publication = publicationsMetadata.publications().get("pub1");
        var publisherStateResponse = new Response(
                publication.resolveCurrentRelations(publisherState, Role.CRATE_USER, Role.CRATE_USER, "dummy"), List.of());

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );

        assertThat(restoreDiff.relationsForStateUpdate(), contains(newRelationName));
        assertThat(restoreDiff.indexNamesToRestore(), is(List.of(newPartitionName.asIndexName())));
        assertThat(restoreDiff.templatesToRestore(), contains(newTemplateName));
    }

    @Test
    public void test_existing_partitioned_table_and_partition_must_not_be_restored() throws Exception {
        var existingRelation = RelationName.fromIndexName("p1");
        var existingPartition = new PartitionName(existingRelation, "dummy");

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .addPartitionedTable(existingRelation, List.of(existingPartition))
            .build();
        var publisherClusterState = new Builder("publisher")
            .addPartitionedTable(existingRelation, List.of(existingPartition))
            .addPublication("pub1", List.of("p1"))
            .build();

        PublicationsMetadata publicationsMetadata = publisherClusterState.metadata().custom(PublicationsMetadata.TYPE);
        Publication publication = publicationsMetadata.publications().get("pub1");
        var publisherStateResponse = new Response(
                publication.resolveCurrentRelations(publisherClusterState, Role.CRATE_USER, Role.CRATE_USER, "dummy"), List.of());

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );

        assertThat(restoreDiff.relationsForStateUpdate(), empty());
        assertThat(restoreDiff.indexNamesToRestore(), empty());
        assertThat(restoreDiff.templatesToRestore(), empty());
    }
}
