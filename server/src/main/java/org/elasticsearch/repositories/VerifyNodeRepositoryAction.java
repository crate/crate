/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoriesService.VerifyResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class VerifyNodeRepositoryAction {

    private static final Logger LOGGER = LogManager.getLogger(VerifyNodeRepositoryAction.class);

    public static final String ACTION_NAME = "internal:admin/repository/verify";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    public VerifyNodeRepositoryAction(TransportService transportService, ClusterService clusterService, RepositoriesService repositoriesService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        transportService.registerRequestHandler(ACTION_NAME, VerifyNodeRepositoryRequest::new, ThreadPool.Names.SNAPSHOT, new VerifyNodeRepositoryRequestHandler());
    }

    public void verify(String repository, boolean readOnly, String verificationToken, final ActionListener<VerifyResponse> listener) {
        final DiscoveryNodes discoNodes = clusterService.state().nodes();
        final DiscoveryNode localNode = discoNodes.getLocalNode();

        final ObjectContainer<DiscoveryNode> masterAndDataNodes = discoNodes.getMasterAndDataNodes().values();
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (ObjectCursor<DiscoveryNode> cursor : masterAndDataNodes) {
            DiscoveryNode node = cursor.value;
            if (readOnly && node.getVersion().before(Version.V_4_2_0)) {
                continue;
            }
            nodes.add(node);
        }
        final CopyOnWriteArrayList<VerificationFailure> errors = new CopyOnWriteArrayList<>();
        final AtomicInteger counter = new AtomicInteger(nodes.size());
        for (final DiscoveryNode node : nodes) {
            if (node.equals(localNode)) {
                try {
                    doVerify(repository, verificationToken, localNode);
                } catch (Exception e) {
                    LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to verify repository", repository), e);
                    errors.add(new VerificationFailure(node.getId(), e));
                }
                if (counter.decrementAndGet() == 0) {
                    finishVerification(listener, nodes, errors);
                }
            } else {
                transportService.sendRequest(node, ACTION_NAME, new VerifyNodeRepositoryRequest(repository, verificationToken), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        if (counter.decrementAndGet() == 0) {
                            finishVerification(listener, nodes, errors);
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        errors.add(new VerificationFailure(node.getId(), exp));
                        if (counter.decrementAndGet() == 0) {
                            finishVerification(listener, nodes, errors);
                        }
                    }
                });
            }
        }
    }

    public void finishVerification(ActionListener<VerifyResponse> listener, List<DiscoveryNode> nodes, CopyOnWriteArrayList<VerificationFailure> errors) {
        listener.onResponse(new RepositoriesService.VerifyResponse(nodes.toArray(new DiscoveryNode[nodes.size()]), errors.toArray(new VerificationFailure[errors.size()])));
    }

    private void doVerify(String repositoryName, String verificationToken, DiscoveryNode localNode) {
        Repository repository = repositoriesService.repository(repositoryName);
        repository.verify(verificationToken, localNode);
    }

    public static class VerifyNodeRepositoryRequest extends TransportRequest {

        private final String repository;
        private final String verificationToken;

        VerifyNodeRepositoryRequest(String repository, String verificationToken) {
            this.repository = repository;
            this.verificationToken = verificationToken;
        }

        public VerifyNodeRepositoryRequest(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            verificationToken = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(verificationToken);
        }
    }

    class VerifyNodeRepositoryRequestHandler implements TransportRequestHandler<VerifyNodeRepositoryRequest> {
        @Override
        public void messageReceived(VerifyNodeRepositoryRequest request, TransportChannel channel, Task task) throws Exception {
            DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
            try {
                doVerify(request.repository, request.verificationToken, localNode);
            } catch (Exception ex) {
                LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to verify repository", request.repository), ex);
                throw ex;
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
