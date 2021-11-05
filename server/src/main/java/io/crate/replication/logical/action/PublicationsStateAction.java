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

import io.crate.common.annotations.VisibleForTesting;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.exceptions.PublicationUnknownException;
import io.crate.replication.logical.metadata.Publication;
import io.crate.user.Privilege;
import io.crate.user.User;
import io.crate.user.UserLookup;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static io.crate.user.Privilege.Type.READ_WRITE_DEFINE;

public class PublicationsStateAction extends ActionType<PublicationsStateAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/publication/state";
    public static final PublicationsStateAction INSTANCE = new PublicationsStateAction();

    private static final Logger LOGGER = Loggers.getLogger(PublicationsStateAction.class);

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
        private final UserLookup userLookup;

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               LogicalReplicationService logicalReplicationService,
                               Schemas schemas,
                               UserLookup userLookup) {
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
            this.userLookup = userLookup;

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
            ArrayList<RelationName> relationNames = new ArrayList<>();
            ArrayList<String> concreteIndices = new ArrayList<>();
            ArrayList<String> concreteTemplates = new ArrayList<>();

            // Ensure subscribing user was not dropped after remote connection was established on another side.
            // Subscribing users must be checked on a publisher side as they belong to the publishing cluster.
            User subscriber = userLookup.findUser(request.subscribingUserName());
            if (subscriber == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ENGLISH, "Cannot create a subscription, subscribing user '%s' was not found.",
                        request.subscribingUserName()
                    )
                );
            }

            for (var publicationName : request.publications()) {
                var publication = logicalReplicationService.publications().get(publicationName);
                if (publication == null) {
                    listener.onFailure(new PublicationUnknownException(publicationName));
                    return;
                }

                // Publication owner cannot be null as we ensure that users who owns publication cannot be dropped.
                // Also, before creating publication or subscription we check that owner was not dropped right before creation.
                User publicationOwner = userLookup.findUser(publication.owner());

                List<RelationName> relationNamesOfPublication = resolveRelationsNames(publication, schemas, publicationOwner, subscriber);
                for (var relationName : relationNamesOfPublication) {

                    var indexNameOrTemplateName = relationName.indexNameOrAlias();
                    if (state.metadata().hasIndex(indexNameOrTemplateName)) {
                        concreteIndices.add(indexNameOrTemplateName);
                    } else {
                        var templateName = PartitionName.templateName(relationName.schema(), relationName.name());
                        var indexTemplate = state.metadata().templates().get(templateName);
                        if (indexTemplate == null) {
                            continue;
                        }
                        concreteTemplates.add(templateName);
                        var partitionIndices = Arrays.asList(indexNameExpressionResolver.concreteIndices(
                            state, IndicesOptions.lenientExpandOpen(), indexNameOrTemplateName));
                        partitionIndices.forEach(i -> concreteIndices.add(i.getName()));
                    }
                    relationNames.add(relationName);
                }
            }
            var response = new Response(
                relationNames,
                concreteIndices,
                concreteTemplates
            );
            listener.onResponse(response);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request,
                                                   ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @VisibleForTesting
        static List<RelationName> resolveRelationsNames(Publication publication,
                                                        Schemas schemas,
                                                        User publicationOwner,
                                                        User subscribedUser) {

            List<RelationName> relationNames;
            if (publication.isForAllTables()) {
                relationNames = InformationSchemaIterables.tablesStream(schemas)
                    .filter(t -> {
                        if (t instanceof DocTableInfo dt) {
                            boolean softDeletes;
                            if ((softDeletes = IndexSettings.INDEX_SOFT_DELETES_SETTING.get(dt.parameters())) == false) {
                                LOGGER.warn(
                                    "Table '{}' won't be replicated as the required table setting " +
                                    "'soft_deletes.enabled' is set to: {}",
                                    dt.ident(),
                                    softDeletes
                                );
                                return false;
                            }
                            return true;
                        }
                        return false;
                    })
                    .filter(t -> userCanPublish(t.ident(), publicationOwner))
                    .filter(t -> subscriberCanRead(t.ident(), subscribedUser))
                    .map(RelationInfo::ident)
                    .toList();
            } else {
                // No need to call userCanPublish() since for the pre-defined tables case this check was already done on the publication creation.
                // Soft deletes check for pre-defined tables was done in LogicalReplicationAnalyzer on the publication creation.
                relationNames = publication.tables()
                    .stream()
                    .filter(t -> subscriberCanRead(t, subscribedUser))
                    .toList();
            }
            return relationNames;
        }

        private static boolean subscriberCanRead(RelationName relationName, User subscriber) {
            return subscriber.hasPrivilege(Privilege.Type.DQL, Privilege.Clazz.TABLE, relationName.fqn(), Schemas.DOC_SCHEMA_NAME);
        }

        private static boolean userCanPublish(RelationName relationName, User publicationOwner) {
            for (Privilege.Type type: READ_WRITE_DEFINE) {
                // This check is triggered only on ALL TABLES case.
                // Required privileges correspond to those we check for the pre-defined tables case in AccessControlImpl.visitCreatePublication.

                // Schemas.DOC_SCHEMA_NAME is a dummy parameter since we are passing fqn as ident.
                if (!publicationOwner.hasPrivilege(type, Privilege.Clazz.TABLE, relationName.fqn(), Schemas.DOC_SCHEMA_NAME)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final List<String> publications;
        private final String subscribingUserName;

        public Request(List<String> publications, String subscribingUserName) {
            this.publications = publications;
            this.subscribingUserName = subscribingUserName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            publications = in.readList(StreamInput::readString);
            subscribingUserName = in.readString();
        }

        public List<String> publications() {
            return publications;
        }

        public String subscribingUserName() {
            return subscribingUserName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(publications);
            out.writeString(subscribingUserName);
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
            tables = in.readList(RelationName::new);
            concreteIndices = in.readList(StreamInput::readString);
            concreteTemplates = in.readList(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(tables);
            out.writeStringCollection(concreteIndices);
            out.writeStringCollection(concreteTemplates);
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
}
