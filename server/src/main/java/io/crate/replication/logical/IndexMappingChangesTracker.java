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

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.function.Function;

public class IndexMappingChangesTracker implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(IndexMappingChangesTracker.class);

    private final ThreadPool threadPool;
    private final Function<String, Client> remoteClient;
    private final ClusterService clusterService;
    public static final long REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC = 60000L;

    public IndexMappingChangesTracker(ThreadPool threadPool, Function<String, Client> remoteClient, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.remoteClient = remoteClient;
        this.clusterService = clusterService;
    }

    public void start(String clusterName) {
        LOGGER.debug("Schedule tracking for remote cluster state");
        Scheduler.Cancellable cancellable = threadPool.scheduleWithFixedDelay(() -> trackMapping(clusterName),
                                                                              TimeValue.timeValueSeconds(1),
                                                                              ThreadPool.Names.SNAPSHOT);
    }

    private void trackMapping(String clusterName) {
        LOGGER.debug("Start tracking for remote cluster state");
        getRemoteClusterState(clusterName, new ActionListener<>() {
            @Override
            public void onResponse(ClusterState remoteClusterState) {
                clusterService.submitStateUpdateTask("track-remote-metadata-changes", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState localClusterState) throws Exception {
                        if(!localClusterState.getNodes().getMasterNodeId().equals(localClusterState.getNodes().getLocalNodeId())) {
                            return localClusterState;
                        }
                        LOGGER.debug("Successfully fetched the cluster state from remote repository");
                        PublicationsMetadata publicationsMetadata = remoteClusterState.metadata().custom(PublicationsMetadata.TYPE);
                        SubscriptionsMetadata subscriptionsMetadata = localClusterState.metadata().custom(SubscriptionsMetadata.TYPE);
                        var followedTables = new HashSet<RelationName>();
                        // Find all tables we are subscribed to
                        for (var subscription  : subscriptionsMetadata.subscription().values()) {
                            for (String publicationName : subscription.publications()) {
                                Publication publication = publicationsMetadata.publications().get(publicationName);
                                followedTables.addAll(publication.tables());
                            }
                        }
                        // Check for all the subscribed tables if there were any changes in the metadata and take them
                        // over from the publisher cluster to the subscriber cluster
                        Metadata.Builder metadataBuilder = Metadata.builder(localClusterState.metadata());
                        boolean updateRequired = false;
                        for (RelationName followedTable : followedTables) {
                            IndexMetadata remoteIndexMetadata = remoteClusterState.metadata().index(followedTable.indexNameOrAlias());
                            IndexMetadata localIndexMetadata = localClusterState.metadata().index(followedTable.indexNameOrAlias());
                            if (!remoteIndexMetadata.equals(localIndexMetadata)) {
                                LOGGER.debug("Metadata changed for table {} detected", followedTable.name());
                                IndexMetadata.Builder builder = IndexMetadata.builder(localIndexMetadata).putMapping(
                                    remoteIndexMetadata.mapping()).mappingVersion(localIndexMetadata.getMappingVersion() +1 );
                                metadataBuilder.put(builder.build(), true);
                                updateRequired = true;
                            }
                        }

                        if (updateRequired) {
                            LOGGER.debug("Update mapping from remote changes");
                            ClusterState newClusterState = ClusterState.builder(localClusterState).metadata(metadataBuilder).build();
                            return newClusterState;
                        } else {
                            LOGGER.debug("No mapping update required");
                            return localClusterState;
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {

                    }
                });
            }
            @Override
            public void onFailure(Exception e) {
                LOGGER.error(e);
            }
        });
    }


    private void getRemoteClusterState(String clusterName, ActionListener<ClusterState> listener) {
        var clusterStateRequest = remoteClient.apply(clusterName).admin().cluster().prepareState()
            .setWaitForTimeOut(new TimeValue(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC))
            .request();

        remoteClient.apply(clusterName).admin().cluster().execute(
            ClusterStateAction.INSTANCE, clusterStateRequest, new ActionListener<>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    listener.onResponse(clusterStateResponse.getState());
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    @Override
    public void close() throws IOException {

    }

}
