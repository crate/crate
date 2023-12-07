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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.user.Role;
import io.crate.user.RoleLookup;

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

        private final RoleLookup userLookup;

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               RoleLookup userLookup) {
            super(Settings.EMPTY,
                  NAME,
                  false,
                  transportService,
                  clusterService,
                  threadPool,
                  Request::new);
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
            // Ensure subscribing user was not dropped after remote connection was established on another side.
            // Subscribing users must be checked on a publisher side as they belong to the publishing cluster.
            Role subscriber = userLookup.findUser(request.subscribingUserName());
            if (subscriber == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ENGLISH, "Cannot build publication state, subscribing user '%s' was not found.",
                        request.subscribingUserName()
                    )
                );
            }

            PublicationsMetadata publicationsMetadata = state.metadata().custom(PublicationsMetadata.TYPE);
            if (publicationsMetadata == null) {
                LOGGER.trace("No publications found on remote cluster.");
                throw new IllegalStateException("Cannot build publication state, no publications found");
            }

            Map<RelationName, RelationMetadata> allRelationsInPublications = new HashMap<>();
            List<String> unknownPublications = new ArrayList<>();
            for (var publicationName : request.publications()) {
                var publication = publicationsMetadata.publications().get(publicationName);
                if (publication == null) {
                    unknownPublications.add(publicationName);
                    continue;
                }

                // Publication owner cannot be null as we ensure that users who owns publication cannot be dropped.
                // Also, before creating publication or subscription we check that owner was not dropped right before creation.
                Role publicationOwner = userLookup.findUser(publication.owner());
                allRelationsInPublications.putAll(publication.resolveCurrentRelations(state, publicationOwner, subscriber, publicationName));
            }
            listener.onResponse(new Response(allRelationsInPublications, unknownPublications));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request,
                                                   ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
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

        private final Map<RelationName, RelationMetadata> relationsInPublications;
        private final List<String> unknownPublications;

        public Response(Map<RelationName, RelationMetadata> relationsInPublications, List<String> unknownPublications) {
            this.relationsInPublications = relationsInPublications;
            this.unknownPublications = unknownPublications;
        }

        public Response(StreamInput in) throws IOException {
            relationsInPublications = in.readMap(RelationName::new, RelationMetadata::new);
            unknownPublications = in.readList(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(relationsInPublications, (o, v) -> v.writeTo(out), (o, v) -> v.writeTo(out));
            out.writeStringCollection(unknownPublications);
        }

        public Map<RelationName, RelationMetadata> relationsInPublications() {
            return relationsInPublications;
        }

        public List<String> unknownPublications() {
            return unknownPublications;
        }

        @Override
        public String toString() {
            return "Response{" + "relationsInPublications:" + relationsInPublications + '.' + "unknownPublications:" + unknownPublications + '}';
        }

        public List<String> concreteIndices() {
            return relationsInPublications.values().stream()
                .flatMap(x -> x.indices().stream())
                .map(x -> x.getIndex().getName())
                .toList();
        }

        public List<String> concreteTemplates() {
            return relationsInPublications.values().stream()
                .map(x -> x.template())
                .filter(Objects::nonNull)
                .map(x -> x.getName())
                .toList();
        }

        public Collection<RelationName> tables() {
            return relationsInPublications.keySet();
        }
    }
}
