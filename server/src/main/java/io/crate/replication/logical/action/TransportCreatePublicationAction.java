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

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_PUBLICATION_NAMES;
import static io.crate.replication.logical.action.PublicationsStateAction.TransportAction.resolveRelationsNames;

import io.crate.exceptions.RelationUnknown;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.replication.logical.exceptions.PublicationAlreadyExistsException;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.user.UserLookup;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

@Singleton
public class TransportCreatePublicationAction extends AbstractDDLTransportAction<CreatePublicationRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "internal:crate:replication/logical/publication/create";
    private final UserLookup userLookup;

    @Inject
    public TransportCreatePublicationAction(TransportService transportService,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            UserLookup userLookup) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              indexNameExpressionResolver,
              CreatePublicationRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
              "create-publication");
        this.userLookup = userLookup;
    }

    @Override
    public ClusterStateTaskExecutor<CreatePublicationRequest> clusterStateTaskExecutor(CreatePublicationRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState,
                                           CreatePublicationRequest request) throws Exception {
                Metadata currentMetadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                var oldMetadata = (PublicationsMetadata) mdBuilder.getCustom(PublicationsMetadata.TYPE);
                if (oldMetadata != null && oldMetadata.publications().containsKey(request.name())) {
                    throw new PublicationAlreadyExistsException(request.name());
                }

                // Ensure publication owner exists
                var user = userLookup.findUser(request.owner());
                if (user == null) {
                    throw new IllegalStateException(
                        String.format(
                            Locale.ENGLISH, "Publication '%s' cannot be created as the user '%s' owning the publication has been dropped.",
                            request.name(),
                            request.owner()
                        )
                    );
                }

                // Ensure tables exists
                for (var relation : request.tables()) {
                    if (currentMetadata.hasIndex(relation.indexNameOrAlias()) == false
                        && currentMetadata.templates().containsKey(PartitionName.templateName(relation.schema(), relation.name())) == false) {
                        throw new RelationUnknown(relation);
                    }
                }

                // create a new instance of the metadata, to guarantee the cluster changed action.
                var newMetadata = PublicationsMetadata.newInstance(oldMetadata);
                var publication = new Publication(request.owner(), request.isForAllTables(), request.tables());
                newMetadata.publications().put(
                    request.name(),
                    publication
                );
                assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
                mdBuilder.putCustom(PublicationsMetadata.TYPE, newMetadata);
                List<RelationName> tablesToUpdate;
                if (request.isForAllTables()) {
                    tablesToUpdate = resolveRelationsNames(publication, currentState, user);
                } else {
                    tablesToUpdate = request.tables();
                }
                addPublicationSetting(indexNameExpressionResolver,
                                      request.name(),
                                      tablesToUpdate,
                                      currentState,
                                      mdBuilder);

                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }
        };
    }

    static void addPublicationSetting(IndexNameExpressionResolver indexNameExpressionResolver,
                                      String publicationName,
                                      List<RelationName> tables,
                                      ClusterState currentState,
                                      Metadata.Builder mdBuilder) {
        applyPublicationSetting(indexNameExpressionResolver, tables, currentState, mdBuilder, x -> {
            x.add(publicationName);
            return x;
        });
    }

    static void applyPublicationSetting(IndexNameExpressionResolver indexNameExpressionResolver,
                                      List<RelationName> tables,
                                      ClusterState currentState,
                                      Metadata.Builder mdBuilder,
                                      Function<List<String>, List<String>> f) {
        for (var relationName : tables) {
            var concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                currentState,
                IndicesOptions.lenientExpandOpen(),
                relationName.indexNameOrAlias()
            );
            for (var indexName : concreteIndices) {
                IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                assert indexMetadata != null : "Cannot resolve indexMetadata for relation=" + relationName;
                var settings = indexMetadata.getSettings();
                var publicationNames = REPLICATION_PUBLICATION_NAMES.get(settings);
                if (publicationNames == null) {
                    publicationNames = new ArrayList<>();
                }
                var updatedPublicationNames = f.apply(publicationNames);
                var settingsBuilder = Settings.builder().put(settings);
                settingsBuilder.putList(REPLICATION_PUBLICATION_NAMES.getKey(), updatedPublicationNames);
                mdBuilder.put(
                    IndexMetadata
                        .builder(indexMetadata)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion())
                        .settings(settingsBuilder)
                );
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(CreatePublicationRequest request,
                                               ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
