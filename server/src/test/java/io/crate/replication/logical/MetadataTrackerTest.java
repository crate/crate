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

import static io.crate.replication.logical.LogicalReplicationSettings.PUBLISHER_INDEX_UUID;
import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static io.crate.role.Role.CRATE_USER;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
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

import io.crate.common.collections.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.PublicationsStateAction.Response;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

public class MetadataTrackerTest extends ESTestCase {

    private ClusterState PUBLISHER_CLUSTER_STATE;
    private ClusterState SUBSCRIBER_CLUSTER_STATE;
    private PublicationsStateAction.Response publicationsStateResponse;

    private static class Builder {

        private ClusterState clusterState;

        public Builder(String clusterName) {
            clusterState = ClusterState.builder(new ClusterName(clusterName)).build();
        }

        public Builder(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        public Builder addTable(String name, List<Reference> columns, Settings settings) throws IOException {
            RelationName relationName = RelationName.fromIndexName(name);

            Settings settingsWithDefaults = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), "0")
                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), "1")
                .put(settings)
                .build();

            Metadata metadata = Metadata.builder(clusterState.metadata())
                .setTable(
                    relationName,
                    columns,
                    settingsWithDefaults,
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(),
                    IndexMetadata.State.OPEN,
                    List.of(),
                    1L
                )
                .build();

            clusterState = ClusterState.builder(clusterState)
                .metadata(metadata)
                .incrementVersion()
                .build();

            RelationMetadata.Table table = metadata.getRelation(relationName);
            assert table != null : "Table " + relationName + " not found in metadata";

            return addPartition(table, new PartitionName(relationName, List.of()), settingsWithDefaults);
        }

        private Builder addPartition(RelationMetadata.Table table, PartitionName partitionName, Settings settings) throws IOException {
            Map<String, Object> mapping = Map.of();
            var indexMetadata = IndexMetadata.builder(partitionName.asIndexName())
                .putMapping(new MappingMetadata(mapping))
                .settings(settings(Version.CURRENT).put(settings).put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
                .partitionValues(partitionName.values())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

            Metadata.Builder mdBuilder = Metadata.builder(clusterState.metadata());
            mdBuilder.put(indexMetadata, true);
            mdBuilder.addIndexUUIDs(table, List.of(indexMetadata.getIndexUUID()));

            clusterState = ClusterState.builder(clusterState)
                .metadata(mdBuilder)
                .routingTable(RoutingTable.builder(clusterState.routingTable())
                    .add(IndexRoutingTable.builder(indexMetadata.getIndex())
                        .addShard(newShardRouting(indexMetadata.getIndex().getName(), 0, "dummy_node", true, ShardRoutingState.STARTED))
                        .build())
                    .build())
                .incrementVersion()
                .build();
            return this;
        }

        public Builder addReplicatingTable(String subscriptionName,
                                           String name,
                                           List<Reference> columns,
                                           Settings settings) throws IOException {
            var newSettings = Settings.builder()
                .put(settings)
                .put(REPLICATION_SUBSCRIPTION_NAME.getKey(), subscriptionName)
                .build();
            return addTable(name, columns, newSettings);
        }

        public Builder addPartitionedTable(RelationName relationName, List<PartitionName> partitions) throws IOException {
            Metadata metadata = Metadata.builder(clusterState.metadata())
                .setTable(
                    relationName,
                    buildReferences(relationName, Map.of("p1", 1)),
                    Settings.EMPTY,
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(ColumnIdent.of("p1")),
                    IndexMetadata.State.OPEN,
                    List.of(),
                    1L
                )
                .build();

            clusterState = ClusterState.builder(clusterState)
                .metadata(metadata)
                .incrementVersion()
                .build();

            RelationMetadata.Table table = metadata.getRelation(relationName);
            assert table != null : "Table " + relationName + " not found in metadata";

            for (var partitionName : partitions) {
                addPartition(table, partitionName, Settings.EMPTY);
            }

            return this;
        }

        public Builder addColumn(String name, Reference newColumn) throws IOException {
            RelationName relationName = RelationName.fromIndexName(name);
            RelationMetadata.Table table = clusterState.metadata().getRelation(relationName);
            assert table != null : "Table " + relationName + " not found in metadata";

            Metadata metadata = Metadata.builder(clusterState.metadata())
                .setTable(
                    table.name(),
                    Lists.concat(table.columns(), List.of(newColumn)),
                    table.settings(),
                    table.routingColumn(),
                    table.columnPolicy(),
                    table.pkConstraintName(),
                    table.checkConstraints(),
                    table.primaryKeys(),
                    table.partitionedBy(),
                    table.state(),
                    table.indexUUIDs(),
                    table.tableVersion() + 1
                )
                .build();

            DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(createNodeContext());
            var tableInfo = docTableInfoFactory.create(relationName, metadata);
            Metadata.Builder metadataBuilder = Metadata.builder(metadata);
            tableInfo.writeTo(metadata, metadataBuilder);

            clusterState = ClusterState.builder(clusterState)
                .metadata(metadataBuilder)
                .incrementVersion()
                .build();
            return this;
        }

        public Builder updateTableSettings(String name, Settings newSettings) {
            RelationName relationName = RelationName.fromIndexName(name);
            Metadata.Builder mdBuilder = Metadata.builder(clusterState.metadata());
            RelationMetadata.Table table = clusterState.metadata().getRelation(relationName);
            if (table == null) {
                throw new IllegalArgumentException("Table " + relationName + " does not exist in cluster state");
            }
            mdBuilder.setTable(
                table.name(),
                table.columns(),
                Settings.builder().put(table.settings()).put(newSettings).build(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                table.indexUUIDs(),
                table.tableVersion() + 1L
            );

            for (String indexUUID : table.indexUUIDs()) {
                var indexMetadata = clusterState.metadata().indexByUUID(indexUUID);
                var updatedSettings = Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(newSettings)
                    .build();
                var newIndexMetadata = IndexMetadata.builder(indexMetadata)
                    .settings(updatedSettings);
                mdBuilder.put(newIndexMetadata);
            }

            clusterState = ClusterState.builder(clusterState)
                .metadata(mdBuilder)
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
                    .collect(Collectors.toMap(rn -> rn, _ -> new Subscription.RelationState(Subscription.State.MONITORING, null)))
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

        private List<Reference> buildReferences(RelationName relationName, Map<String, Object> mapping) {
            return mapping.entrySet().stream()
                .map(entry -> new SimpleReference(
                    new ReferenceIdent(relationName, entry.getKey()),
                    RowGranularity.DOC,
                    DataTypes.guessType(entry.getValue()),
                    0,
                    null
                ))
                .collect(Collectors.toList());
        }

    }

    private static PublicationsStateAction.Response buildPublisherResponse(ClusterState publisherState,
                                                                           String publicationName) {
        PublicationsMetadata publicationsMetadata = publisherState.metadata().custom(PublicationsMetadata.TYPE);
        Publication publication = publicationsMetadata.publications().get(publicationName);
        Metadata.Builder mdBuilder = Metadata.builder();
        publication.resolveCurrentRelations(
            publisherState,
            () -> List.of(CRATE_USER),
            CRATE_USER,
            CRATE_USER,
            "dummy",
            mdBuilder
        );
        return new Response(mdBuilder.build(), List.of());
    }


    @Before
    public void setUpStates() throws Exception {
        RelationName relationName = new RelationName(null, "test");
        SimpleReference reference = new SimpleReference(new ReferenceIdent(relationName, "one"), RowGranularity.DOC, DataTypes.STRING, 1, null);
        PUBLISHER_CLUSTER_STATE = new Builder("publisher")
            .addTable("test", List.of(reference), Settings.EMPTY)
            .addPublication("pub1", List.of("test"))
            .build();

        RelationName testTable = new RelationName("doc", "test");
        publicationsStateResponse = buildPublisherResponse(PUBLISHER_CLUSTER_STATE, "pub1");

        List<IndexMetadata> publisherIndices = PUBLISHER_CLUSTER_STATE.metadata().getIndices(testTable, List.of(), false, im -> im);
        Settings replicatingTableSettings = Settings.builder()
            .put(REPLICATION_SUBSCRIPTION_NAME.getKey(), "sub1")
            .put(PUBLISHER_INDEX_UUID.getKey(), publisherIndices.getFirst().getIndexUUID())
            .build();

        SUBSCRIBER_CLUSTER_STATE = new Builder("subscriber")
            .addReplicatingTable("sub1", "test", List.of(reference), replicatingTableSettings)
            .addSubscription("sub1", List.of("pub1"), List.of("test"))
            .build();
    }

    @Test
    public void test_mappings_is_transferred_between_two_clustering_for_logical_replication() throws Exception {
        RelationName relationName = RelationName.fromIndexName("test");
        var syncedSubscriberClusterState = MetadataTracker.updateRelations(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            publicationsStateResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        // Nothing in the indexMetadata changed, so the cluster state must be equal
        assertThat(SUBSCRIBER_CLUSTER_STATE).isEqualTo(syncedSubscriberClusterState);

        // Let's change the mapping on the publisher publisherClusterState
        Map<String, Object> updatedMapping = Map.of("1", "one", "2", "two");
        SimpleReference newColumn = new SimpleReference(new ReferenceIdent(relationName, "two"), RowGranularity.DOC, DataTypes.STRING, 1, null);
        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .addColumn("test", newColumn)
            .build();

        var updatedResponse = buildPublisherResponse(updatedPublisherClusterState, "pub1");

        syncedSubscriberClusterState = MetadataTracker.updateRelations(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        assertThat(SUBSCRIBER_CLUSTER_STATE).isNotEqualTo(syncedSubscriberClusterState);

        RelationMetadata.Table publisherTable = updatedPublisherClusterState.metadata().getRelation(relationName);
        RelationMetadata.Table syncedTable = syncedSubscriberClusterState.metadata().getRelation(relationName);
        assertThat(syncedTable.columns()).isEqualTo(publisherTable.columns());

        for (String indexUUID : syncedTable.indexUUIDs()) {
            IndexMetadata syncedIndexMetadata = syncedSubscriberClusterState.metadata().indexByUUID(indexUUID);
            String publisherIndexUUID = PUBLISHER_INDEX_UUID.get(syncedIndexMetadata.getSettings());
            IndexMetadata publisherIndexMetadata = updatedPublisherClusterState.metadata().indexByUUID(publisherIndexUUID);
            assertThat(syncedIndexMetadata.mapping()).isEqualTo(publisherIndexMetadata.mapping());
        }
    }

    @Test
    public void test_dynamic_setting_is_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var newSettings = Settings.builder().put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 5).build();

        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableSettings("test", newSettings)
            .build();
        var updatedResponse = buildPublisherResponse(updatedPublisherClusterState, "pub1");
        var syncedSubscriberClusterState = MetadataTracker.updateRelations(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubscriberClusterState.metadata().index("test");
        assertThat(syncedIndexMetadata.getSettings().getAsInt(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), null)).isEqualTo(5);
    }

    @Test
    public void test_private_setting_is_not_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var newSettings = Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 50).build();

        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableSettings("test", newSettings)
            .build();
        var updatedResponse = buildPublisherResponse(updatedPublisherClusterState, "pub1");

        var syncedSubscriberClusterState = MetadataTracker.updateRelations(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        RelationMetadata.Table syncedTable = syncedSubscriberClusterState.metadata().getRelation(RelationName.fromIndexName("test"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(syncedTable.settings())).isNotEqualTo(50);

        for (String indexUUID : syncedTable.indexUUIDs()) {
            IndexMetadata indexMetadata = syncedSubscriberClusterState.metadata().indexByUUID(indexUUID);
            assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings())).isNotEqualTo(50);
        }
    }

    @Test
    public void test_non_replicatable_setting_is_not_transferred_between_two_clustering_for_logical_replication() throws Exception {
        var newSettings = Settings.builder().put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 5).build();
        var updatedPublisherClusterState = new Builder(PUBLISHER_CLUSTER_STATE)
            .updateTableSettings("test", newSettings)
            .build();
        var updatedResponse = buildPublisherResponse(updatedPublisherClusterState, "pub1");

        var syncedSubscriberClusterState = MetadataTracker.updateRelations(
            "sub1",
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            updatedResponse,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );
        var syncedIndexMetadata = syncedSubscriberClusterState.metadata().index("test");
        assertThat(INDEX_NUMBER_OF_REPLICAS_SETTING.get(syncedIndexMetadata.getSettings())).isEqualTo(0);
    }

    @Test
    public void test_restore_and_state_update_has_no_new_relations() {
        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(SUBSCRIBER_CLUSTER_STATE.metadata()).get("sub1"),
            SUBSCRIBER_CLUSTER_STATE,
            publicationsStateResponse
        );
        assertThat(restoreDiff.toRestore()).isEmpty();
    }

    @Test
    public void test_new_table_of_publication_for_all_tables_will_be_restored() throws Exception {
        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .build();

        var publisherState = new Builder("publisher")
            .addTable("t2", List.of(), Settings.EMPTY)
            .addPublication("pub1", List.of("t2"))
            .build();

        var updatedResponse = buildPublisherResponse(publisherState, "pub1");

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            updatedResponse
        );
        assertThat(restoreDiff.toRestore()).containsExactly(new TableOrPartition(RelationName.fromIndexName("t2"), null));
    }

    @Test
    public void test_new_empty_partitioned_table_of_publication_for_all_tables_will_be_restored() throws Exception {
        var newRelation = RelationName.fromIndexName("p1");

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .build();

        RelationName p1 = RelationName.fromIndexName("p1");
        var publisherState = new Builder("publisher")
            .addPartitionedTable(p1, List.of())
            .addPublication("pub1", List.of(p1.indexNameOrAlias()))
            .build();
        var publisherStateResponse = buildPublisherResponse(publisherState, "pub1");

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );
        assertThat(restoreDiff.relationsForStateUpdate()).containsExactly(newRelation);
        assertThat(restoreDiff.toRestore()).containsExactly(new TableOrPartition(p1, null));
    }

    @Test
    public void test_new_partitioned_table_with_partition_of_publication_for_all_tables_will_be_restored() throws Exception {
        var newRelation = RelationName.fromIndexName("p1");
        var newPartitionName = new PartitionName(newRelation, List.of("dummy"));

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .build();

        var publisherState = new Builder("publisher")
            .addPublication("pub1", List.of(newRelation.indexNameOrAlias()))
            .addPartitionedTable(newRelation, List.of(newPartitionName))
            .build();
        var publisherStateResponse = buildPublisherResponse(publisherState, "pub1");

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );

        assertThat(restoreDiff.relationsForStateUpdate()).containsExactly(newRelation);
        assertThat(restoreDiff.toRestore()).containsExactly(new TableOrPartition(newRelation, newPartitionName.ident()));
    }

    @Test
    public void test_new_partition_of_already_replicating_partition_table_will_be_restored() throws Exception {
        var relationName = RelationName.fromIndexName("p1");
        var newPartitionName = new PartitionName(relationName, List.of("dummy"));

        var subscriberClusterState = new Builder("subscriber")
            .addPartitionedTable(relationName, List.of())
            .addSubscription("sub1", List.of("pub1"), List.of("p1"))
            .build();

        var publisherState = new Builder("publisher")
            .addPublication("pub1", List.of(relationName.indexNameOrAlias()))
            .addPartitionedTable(relationName, List.of(newPartitionName))
            .build();
        var publisherStateResponse = buildPublisherResponse(publisherState, "pub1");

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );

        assertThat(restoreDiff.relationsForStateUpdate()).containsExactly(relationName);
        assertThat(restoreDiff.toRestore()).containsExactly(new TableOrPartition(relationName, newPartitionName.ident()));
    }

    @Test
    public void test_new_partitioned_table_of_publication_with_concrete_tables_will_be_restored() throws Exception {
        var newRelationName = RelationName.fromIndexName("p1");
        var newPartitionName = new PartitionName(newRelationName, List.of("dummy"));

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of("t1"))
            .addReplicatingTable("sub1", "t1", List.of(), Settings.EMPTY)
            .build();

        var publisherState = new Builder("publisher")
            .addPublication("pub1", List.of("p1", "t1"))
            .addTable("t1", List.of(), Settings.EMPTY)
            .addPartitionedTable(newRelationName, List.of(newPartitionName))
            .build();

        var publisherStateResponse = buildPublisherResponse(publisherState, "pub1");
        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );
        assertThat(restoreDiff.toRestore()).containsExactly(new TableOrPartition(newRelationName, newPartitionName.ident()));
    }

    @Test
    public void test_existing_partitioned_table_and_partition_must_not_be_restored() throws Exception {
        var existingRelation = RelationName.fromIndexName("p1");
        var existingPartition = new PartitionName(existingRelation, List.of("dummy"));

        var subscriberClusterState = new Builder("subscriber")
            .addSubscription("sub1", List.of("pub1"), List.of())
            .addPartitionedTable(existingRelation, List.of(existingPartition))
            .build();
        var publisherClusterState = new Builder("publisher")
            .addPartitionedTable(existingRelation, List.of(existingPartition))
            .addPublication("pub1", List.of("p1"))
            .build();

        var publisherStateResponse = buildPublisherResponse(publisherClusterState, "pub1");

        var restoreDiff = MetadataTracker.getRestoreDiff(
            SubscriptionsMetadata.get(subscriberClusterState.metadata()).get("sub1"),
            subscriberClusterState,
            publisherStateResponse
        );
        assertThat(restoreDiff.relationsForStateUpdate()).isEmpty();
        assertThat(restoreDiff.toRestore()).isEmpty();
    }
}
