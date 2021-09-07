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

import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PublicationsStateAction extends ActionType<PublicationsStateAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/publication/state";
    public static final PublicationsStateAction INSTANCE = new PublicationsStateAction();

    public PublicationsStateAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Singleton
    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final LogicalReplicationService logicalReplicationService;
        private final Schemas schemas;

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               LogicalReplicationService logicalReplicationService,
                               Schemas schemas) {
            super(Settings.EMPTY,
                  NAME,
                  false,
                  transportService,
                  clusterService,
                  threadPool,
                  indexNameExpressionResolver,
                  Request::new);
            this.logicalReplicationService = logicalReplicationService;
            this.schemas = schemas;

            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(Request request,
                                       ClusterState state,
                                       ActionListener<Response> listener) throws Exception {
            List<RelationDetails> relationDetails = new ArrayList<>();
            for (var publicationName : request.publications()) {
                var publication = logicalReplicationService.publications().get(publicationName);
                if (publication == null) {
                    listener.onFailure(new PublicationUnknownException(publicationName));
                    return;
                }

                List<RelationName> relationNames;
                if (publication.isForAllTables()) {
                    relationNames = InformationSchemaIterables.tablesStream(schemas)
                        .filter(t -> t instanceof DocTableInfo)
                        .map(RelationInfo::ident)
                        .toList();
                } else {
                    relationNames = publication.tables();
                }
                for (var relationName : relationNames) {
                    var relationDetail = relationDetail(relationName, state);
                    if (relationDetail != null) {
                        relationDetails.add(relationDetail);
                    }
                }
            }
            var response = new Response(
                relationDetails.stream().map(d -> d.relationName).toList(),
                relationDetails.stream().flatMap(d -> d.concreteIndices.stream()).toList(),
                relationDetails.stream().flatMap(d -> d.concreteTemplates.stream()).toList()
            );
            listener.onResponse(response);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request,
                                                   ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Nullable
        private RelationDetails relationDetail(RelationName relationName, ClusterState clusterState) {
            var relationDetails = new RelationDetails(relationName);
            var indexNameOrTemplateName = relationName.indexNameOrAlias();
            if (clusterState.metadata().hasIndex(indexNameOrTemplateName)) {
                relationDetails.concreteIndices.add(indexNameOrTemplateName);
            } else {
                var templateName = PartitionName.templateName(relationName.schema(), relationName.name());
                var indexTemplate = clusterState.metadata().templates().get(templateName);
                if (indexTemplate == null) {
                    return null;
                }
                relationDetails.concreteTemplates.add(templateName);
                var partitionIndices = Arrays.asList(indexNameExpressionResolver.concreteIndices(
                    clusterState, IndicesOptions.lenientExpandOpen(), indexNameOrTemplateName));
                partitionIndices.forEach(i -> relationDetails.concreteIndices.add(i.getName()));
            }
            return relationDetails;
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final List<String> publications;

        public Request(List<String> publications) {
            this.publications = publications;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            publications = Arrays.stream(in.readStringArray()).toList();
        }

        public List<String> publications() {
            return publications;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(publications.toArray(new String[0]));
        }
    }

    public static class Response extends TransportResponse {

        private final List<RelationName> tables;
        private final List<String> concreteIndices;
        private final List<String> concreteTemplates;

        public Response(List<RelationName> tables,
                        List<String> concreteIndices,
                        List<String> concreteTemplates) {
            this.tables = tables;
            this.concreteIndices = concreteIndices;
            this.concreteTemplates = concreteTemplates;
        }

        public Response(StreamInput in) throws IOException {
            int tablesSize = in.readVInt();
            tables = new ArrayList<>(tablesSize);
            for (int i = 0; i < tablesSize; i++) {
                tables.add(new RelationName(in));
            }
            concreteIndices = Arrays.stream(in.readStringArray()).toList();
            concreteTemplates = Arrays.stream(in.readStringArray()).toList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(tables.size());
            for (var relationName : tables) {
                relationName.writeTo(out);
            }
            out.writeStringArray(concreteIndices.toArray(new String[0]));
            out.writeStringArray(concreteTemplates.toArray(new String[0]));
        }

        public List<RelationName> tables() {
            return tables;
        }

        public List<String> concreteIndices() {
            return concreteIndices;
        }

        public List<String> concreteTemplates() {
            return concreteTemplates;
        }
    }

    private static class RelationDetails {
        private final RelationName relationName;
        private final ArrayList<String> concreteIndices = new ArrayList<>();
        private final ArrayList<String> concreteTemplates = new ArrayList<>();

        public RelationDetails(RelationName relationName) {
            this.relationName = relationName;
        }
    }
}
