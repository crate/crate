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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 */
public class RepositoriesService implements ClusterStateApplier {

    private static final Logger LOGGER = LogManager.getLogger(RepositoriesService.class);

    private final Map<String, Repository.Factory> typesRegistry;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final VerifyNodeRepositoryAction verifyAction;

    private volatile Map<String, Repository> repositories = Collections.emptyMap();

    @Inject
    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService,
                               Map<String, Repository.Factory> typesRegistry,
                               ThreadPool threadPool) {
        this.typesRegistry = typesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.addStateApplier(this);
        }
        this.verifyAction = new VerifyNodeRepositoryAction(transportService, clusterService, this);
    }

    /**
     * Registers new repository in the cluster
     * <p>
     * This method can be only called on the master node. It tries to create a new repository on the master
     * and if it was successful it adds new repository to cluster metadata.
     *
     * @param request  register repository request
     * @param listener register repository listener
     */
    public void registerRepository(final RegisterRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name, request.type, request.settings);

        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        if (request.verify) {
            registrationListener = new VerifyingRegisterRepositoryListener(request.name, listener);
        } else {
            registrationListener = listener;
        }

        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<>(request, registrationListener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws IOException {
                ensureRepositoryNotInUse(currentState, request.name);
                // Trying to create the new repository on master to make sure it works
                if (!registerRepository(newRepositoryMetadata)) {
                    // The new repository has the same settings as the old one - ignore
                    return currentState;
                }
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                if (repositories == null) {
                    LOGGER.info("put repository [{}]", request.name);
                    repositories = new RepositoriesMetadata(
                        Collections.singletonList(new RepositoryMetadata(request.name, request.type, request.settings)));
                } else {
                    boolean found = false;
                    List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);

                    for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                        if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                            found = true;
                            repositoriesMetadata.add(newRepositoryMetadata);
                        } else {
                            repositoriesMetadata.add(repositoryMetadata);
                        }
                    }
                    if (!found) {
                        LOGGER.info("put repository [{}]", request.name);
                        repositoriesMetadata.add(new RepositoryMetadata(request.name, request.type, request.settings));
                    } else {
                        LOGGER.info("update repository [{}]", request.name);
                    }
                    repositories = new RepositoriesMetadata(repositoriesMetadata);
                }
                mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOGGER.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name), e);
                super.onFailure(source, e);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // repository is created on both master and data nodes
                return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
            }
        });
    }

    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the master node. It removes repository information from cluster metadata.
     *
     * @param request  unregister repository request
     * @param listener unregister repository listener
     */
    public void unregisterRepository(final UnregisterRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<>(request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                ensureRepositoryNotInUse(currentState, request.name);
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                if (repositories != null && repositories.repositories().size() > 0) {
                    List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                    boolean changed = false;
                    for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                        if (Regex.simpleMatch(request.name, repositoryMetadata.name())) {
                            LOGGER.info("delete repository [{}]", repositoryMetadata.name());
                            changed = true;
                        } else {
                            repositoriesMetadata.add(repositoryMetadata);
                        }
                    }
                    if (changed) {
                        repositories = new RepositoriesMetadata(repositoriesMetadata);
                        mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }
                }
                if (Regex.isMatchAllPattern(request.name)) { // we use a wildcard so we don't barf if it's not present.
                    return currentState;
                }
                throw new RepositoryMissingException(request.name);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // repository was created on both master and data nodes
                return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
            }
        });
    }

    public void verifyRepository(final String repositoryName, final ActionListener<VerifyResponse> listener) {
        final Repository repository = repository(repositoryName);
        final boolean readOnly = repository.isReadOnly();
        try {
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                try {
                    final String verificationToken = repository.startVerification();
                    if (verificationToken != null) {
                        try {
                            verifyAction.verify(repositoryName, readOnly, verificationToken, new ActionListener<VerifyResponse>() {
                                @Override
                                public void onResponse(VerifyResponse verifyResponse) {
                                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                        try {
                                            repository.endVerification(verificationToken);
                                        } catch (Exception e) {
                                            LOGGER.warn(() -> new ParameterizedMessage(
                                                "[{}] failed to finish repository verification", repositoryName), e);
                                            listener.onFailure(e);
                                            return;
                                        }
                                        listener.onResponse(verifyResponse);
                                    });
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }
                            });
                        } catch (Exception e) {
                            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                try {
                                    repository.endVerification(verificationToken);
                                } catch (Exception inner) {
                                    inner.addSuppressed(e);
                                    LOGGER.warn(() -> new ParameterizedMessage(
                                        "[{}] failed to finish repository verification", repositoryName), inner);
                                }
                                listener.onFailure(e);
                            });
                        }
                    } else {
                        listener.onResponse(new VerifyResponse(new DiscoveryNode[0], new VerificationFailure[0]));
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }


    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            RepositoriesMetadata oldMetadata = event.previousState().getMetadata().custom(RepositoriesMetadata.TYPE);
            RepositoriesMetadata newMetadata = event.state().getMetadata().custom(RepositoriesMetadata.TYPE);

            // Check if repositories got changed
            if ((oldMetadata == null && newMetadata == null) || (oldMetadata != null && oldMetadata.equals(newMetadata))) {
                return;
            }

            LOGGER.trace("processing new index repositories for state version [{}]", event.state().version());

            Map<String, Repository> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                if (newMetadata == null || newMetadata.repository(entry.getKey()) == null) {
                    LOGGER.debug("unregistering repository [{}]", entry.getKey());
                    closeRepository(entry.getValue());
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, Repository> builder = new HashMap<>();
            if (newMetadata != null) {
                // Now go through all repositories and update existing or create missing
                for (RepositoryMetadata repositoryMetadata : newMetadata.repositories()) {
                    Repository repository = survivors.get(repositoryMetadata.name());
                    if (repository != null) {
                        // Found previous version of this repository
                        RepositoryMetadata previousMetadata = repository.getMetadata();
                        if (previousMetadata.type().equals(repositoryMetadata.type()) == false
                            || previousMetadata.settings().equals(repositoryMetadata.settings()) == false) {
                            // Previous version is different from the version in settings
                            LOGGER.debug("updating repository [{}]", repositoryMetadata.name());
                            closeRepository(repository);
                            repository = null;
                            try {
                                repository = createRepository(repositoryMetadata);
                            } catch (RepositoryException ex) {
                                // TODO: this catch is bogus, it means the old repo is already closed,
                                // but we have nothing to replace it
                                LOGGER.warn(() -> new ParameterizedMessage("failed to change repository [{}]", repositoryMetadata.name()), ex);
                            }
                        }
                    } else {
                        try {
                            repository = createRepository(repositoryMetadata);
                        } catch (RepositoryException ex) {
                            LOGGER.warn(() -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetadata.name()), ex);
                        }
                    }
                    if (repository != null) {
                        LOGGER.debug("registering repository [{}]", repositoryMetadata.name());
                        builder.put(repositoryMetadata.name(), repository);
                    }
                }
            }
            repositories = Collections.unmodifiableMap(builder);
        } catch (Exception ex) {
            LOGGER.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Returns registered repository
     * <p>
     * This method is called only on the master node
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(String repositoryName) {
        Repository repository = repositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    /**
     * Creates a new repository and adds it to the list of registered repositories.
     * <p>
     * If a repository with the same name but different types or settings already exists, it will be closed and
     * replaced with the new repository. If a repository with the same name exists but it has the same type and settings
     * the new repository is ignored.
     *
     * @param repositoryMetadata new repository metadata
     * @return {@code true} if new repository was added or {@code false} if it was ignored
     */
    private boolean registerRepository(RepositoryMetadata repositoryMetadata) throws IOException {
        Repository previous = repositories.get(repositoryMetadata.name());
        if (previous != null) {
            RepositoryMetadata previousMetadata = previous.getMetadata();
            if (previousMetadata.equals(repositoryMetadata)) {
                // Previous version is the same as this one - ignore it
                return false;
            }
        }
        Repository newRepo = createRepository(repositoryMetadata);
        if (previous != null) {
            closeRepository(previous);
        }
        Map<String, Repository> newRepositories = new HashMap<>(repositories);
        newRepositories.put(repositoryMetadata.name(), newRepo);
        repositories = newRepositories;
        return true;
    }

    /** Closes the given repository. */
    private void closeRepository(Repository repository) throws IOException {
        LOGGER.debug("closing repository [{}][{}]", repository.getMetadata().type(), repository.getMetadata().name());
        repository.close();
    }

    /**
     * Creates repository holder
     */
    private Repository createRepository(RepositoryMetadata repositoryMetadata) {
        LOGGER.debug("creating repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name());
        Repository.Factory factory = typesRegistry.get(repositoryMetadata.type());
        if (factory == null) {
            throw new RepositoryException(repositoryMetadata.name(),
                "repository type [" + repositoryMetadata.type() + "] does not exist");
        }
        try {
            Repository repository = factory.create(repositoryMetadata, typesRegistry::get);
            repository.start();
            return repository;
        } catch (Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("failed to create repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name()), e);
            throw new RepositoryException(repositoryMetadata.name(), "failed to create repository", e);
        }
    }

    private void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (SnapshotsService.isRepositoryInUse(clusterState, repository) || RestoreService.isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used ");
        }
    }

    private class VerifyingRegisterRepositoryListener implements ActionListener<ClusterStateUpdateResponse> {

        private final String name;

        private final ActionListener<ClusterStateUpdateResponse> listener;

        VerifyingRegisterRepositoryListener(String name, final ActionListener<ClusterStateUpdateResponse> listener) {
            this.name = name;
            this.listener = listener;
        }

        @Override
        public void onResponse(final ClusterStateUpdateResponse clusterStateUpdateResponse) {
            if (clusterStateUpdateResponse.isAcknowledged()) {
                // The response was acknowledged - all nodes should know about the new repository, let's verify them
                verifyRepository(name, new ActionListener<VerifyResponse>() {
                    @Override
                    public void onResponse(VerifyResponse verifyResponse) {
                        if (verifyResponse.failed()) {
                            listener.onFailure(new RepositoryVerificationException(name, verifyResponse.failureDescription()));
                        } else {
                            listener.onResponse(clusterStateUpdateResponse);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            } else {
                listener.onResponse(clusterStateUpdateResponse);
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    public Collection<Repository> getRepositoriesList() {
        return Collections.unmodifiableCollection(repositories.values());
    }

    /**
     * Register repository request
     */
    public static class RegisterRepositoryRequest extends ClusterStateUpdateRequest<RegisterRepositoryRequest> {

        final String cause;

        final String name;

        final String type;

        final boolean verify;

        Settings settings = Settings.EMPTY;

        /**
         * Constructs new register repository request
         *
         * @param cause repository registration cause
         * @param name  repository name
         * @param type  repository type
         * @param verify verify repository after creation
         */
        public RegisterRepositoryRequest(String cause, String name, String type, boolean verify) {
            this.cause = cause;
            this.name = name;
            this.type = type;
            this.verify = verify;
        }

        /**
         * Sets repository settings
         *
         * @param settings repository settings
         * @return this request
         */
        public RegisterRepositoryRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }
    }

    /**
     * Unregister repository request
     */
    public static class UnregisterRepositoryRequest extends ClusterStateUpdateRequest<UnregisterRepositoryRequest> {

        final String cause;

        final String name;

        /**
         * Creates a new unregister repository request
         *
         * @param cause repository unregistration cause
         * @param name  repository name
         */
        public UnregisterRepositoryRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

    }

    /**
     * Verify repository request
     */
    public static class VerifyResponse {

        private final VerificationFailure[] failures;

        private final DiscoveryNode[] nodes;

        public VerifyResponse(DiscoveryNode[] nodes, VerificationFailure[] failures) {
            this.nodes = nodes;
            this.failures = failures;
        }

        public VerificationFailure[] failures() {
            return failures;
        }

        public DiscoveryNode[] nodes() {
            return nodes;
        }

        public boolean failed() {
            return failures.length > 0;
        }

        public String failureDescription() {
            return Arrays
                .stream(failures)
                .map(failure -> failure.toString())
                .collect(Collectors.joining(", ", "[", "]"));
        }

    }

}
