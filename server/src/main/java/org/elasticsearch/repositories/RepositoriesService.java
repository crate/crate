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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.AlterRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.common.io.IOUtils;
import io.crate.concurrent.FutureActionListener;
import io.crate.exceptions.RepositoryAlreadyExistsException;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 */
public class RepositoriesService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger LOGGER = LogManager.getLogger(RepositoriesService.class);

    private final Map<String, Repository.Factory> typesRegistry;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final VerifyNodeRepositoryAction verifyAction;

    private final Map<String, Repository> internalRepositories = new ConcurrentHashMap<>();
    private volatile Map<String, Repository> repositories = Collections.emptyMap();

    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService,
                               Map<String, Repository.Factory> typesRegistry,
                               ThreadPool threadPool) {
        this.typesRegistry = typesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterEligibleNode(settings)) {
            if (isDedicatedVotingOnlyNode(DiscoveryNode.getRolesFromSettings(settings)) == false) {
                clusterService.addHighPriorityApplier(this);
            }
        }
        this.verifyAction = new VerifyNodeRepositoryAction(transportService, clusterService, this);
    }

    /**
     * Creates a new repository in the cluster.
     * <p>
     * This method can be only called on the master node. It tries to create a new repository on the master
     * and if it was successful it adds new repository to cluster metadata.
     *
     * @param request  register repository request
     * @return
     */
    public CompletableFuture<ClusterStateUpdateResponse> createRepository(final PutRepositoryRequest request) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";

        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());
        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> updateTask = new AckedClusterStateUpdateTask<>(request) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws IOException {
                RepositoriesMetadata existingRepos = currentState.metadata().custom(RepositoriesMetadata.TYPE);
                ensureRepositoryDoesNotExist(existingRepos, request.name());

                // Trying to create the new repository on master node to make sure it works.
                tryCreateRepo(newRepositoryMetadata);

                var updatedMeta = Metadata.builder(currentState.metadata())
                    .putCustom(RepositoriesMetadata.TYPE, createRepository(existingRepos, newRepositoryMetadata))
                    .build();
                return ClusterState.builder(currentState).metadata(updatedMeta).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOGGER.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name()), e);
                super.onFailure(source, e);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // repository is created on both master and data nodes
                return discoveryNode.isMasterEligibleNode() || discoveryNode.isDataNode();
            }
        };
        clusterService.submitStateUpdateTask("put_repository [" + request.name() + "]", updateTask);

        return updateTask.completionFuture().thenCompose(response -> {
            if (response.isAcknowledged()) {
                // The response was acknowledged - all nodes should know about the new repository, let's verify them
                FutureActionListener<List<DiscoveryNode>> listener = new FutureActionListener<>();
                verifyRepository(request.name(), listener);
                return listener.thenApply(ignored -> response);
            } else {
                return CompletableFuture.completedFuture(response);
            }
        });
    }

    private void ensureRepositoryDoesNotExist(RepositoriesMetadata repositories, String name) {
        if (repositories == null || repositories.repositories() == null) {
            return;
        }

        for (RepositoryMetadata r : repositories.repositories()) {
            if (r.name().equals(name)) {
                throw new RepositoryAlreadyExistsException(name);
            }
        }
    }

    private Metadata.Custom createRepository(@Nullable RepositoriesMetadata existingRepos, RepositoryMetadata newRepositoryMetadata) {
        LOGGER.info("creating new repository metadata [{}]", newRepositoryMetadata.name());
        if (existingRepos == null) {
            return new RepositoriesMetadata(Collections.singletonList(newRepositoryMetadata));
        }

        List<RepositoryMetadata> withNewRepo = new ArrayList<>(existingRepos.repositories().size() + 1);
        withNewRepo.addAll(existingRepos.repositories());
        withNewRepo.add(newRepositoryMetadata);

        return new RepositoriesMetadata(withNewRepo);
    }

    public CompletableFuture<AcknowledgedResponse> alterRepository(final AlterRepositoryRequest request) {
        assert lifecycle.started() : "Trying to alter repository but service is in state [" + lifecycle.state() + "]";

        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> updateTask = new AckedClusterStateUpdateTask<>(request) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // check if repository exists and is not being used
                var _ = repository(request.name());
                ensureRepositoryNotInUse(currentState, request.name());

                // try updating metadata, fail if repository not found
                RepositoriesMetadata repositories = currentState.metadata().custom(RepositoriesMetadata.TYPE);
                var updatedRepos = updateRepository(repositories.repositories(), request);
                if (repositories.repositories() == updatedRepos) {
                    LOGGER.info("request to alter repository [{}] produced no change", request.name());
                    return currentState;
                }

                // update and return cluster metadata
                LOGGER.info("alter repository [{}]", request.name());
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata())
                    .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(updatedRepos));
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                LOGGER.warn("failed to alter repository [{}]", request.name(), e);
                super.onFailure(source, e);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // repository is updated on both master and data nodes
                return discoveryNode.isMasterEligibleNode() || discoveryNode.isDataNode();
            }
        };
        clusterService.submitStateUpdateTask("alter_repository [" + request.name() + "]", updateTask);

        return updateTask.completionFuture().thenCompose(response -> {
            if (response.isAcknowledged()) {
                // The response was acknowledged - all nodes should know about the updated repository, let's verify them
                FutureActionListener<List<DiscoveryNode>> listener = new FutureActionListener<>();
                verifyRepository(request.name(), listener);
                return listener.thenApply(ignored -> new AcknowledgedResponse(true));
            } else {
                return CompletableFuture.completedFuture(new AcknowledgedResponse(false));
            }
        });
    }

    /**
     * Updates the repository with {@code name} in the provided list of repositories,
     * patching it with {@code newSettings}.
     * <p>
     * Validates the updated metadata by creating a temporary {@link Repository} object.
     *
     * @param repositories      the list of repositories to update
     * @param req               the request that contains properties to be set or reset
     * @return the updated list if the repository metadata changed, or the original list if nothing changed
     */
    private List<RepositoryMetadata> updateRepository(List<RepositoryMetadata> repositories, AlterRepositoryRequest req) {
        boolean found = false;
        RepositoryMetadata updatedMeta;

        List<RepositoryMetadata> updatedRepos = new ArrayList<>(repositories.size());
        for (var repoMeta : repositories) {
            if (!repoMeta.name().equals(req.name())) {
                updatedRepos.add(repoMeta);
                continue;
            }

            found = true;
            var settings = patchRepositorySettings(repoMeta, req);
            if (settings.equals(repoMeta.settings())) {
                return repositories;
            }

            updatedMeta = new RepositoryMetadata(repoMeta.name(), repoMeta.type(), settings);
            tryCreateRepo(updatedMeta);
            updatedRepos.add(updatedMeta);
        }

        if (!found) {
            LOGGER.error("alter repository [{}] requested, but repository has not been found", req.name());
            throw new RepositoryMissingException(req.name());
        }

        return updatedRepos;
    }

    private Settings patchRepositorySettings(RepositoryMetadata repoMeta, AlterRepositoryRequest req) {
        var updated = Settings.builder().put(repoMeta.settings());
        Set<String> required = typesRegistry.get(repoMeta.type()).settings().required().keySet();

        // Set properties
        // todo maybe handle `reset all`
        for (String prop : req.settings().keySet()) {
            String value = req.settings().get(prop);
            if (value == null) {
                if (required.contains(prop)) {
                    throw new IllegalArgumentException("cannot reset required: " + prop);
                }
                updated.remove(prop);
            } else {
                updated.put(prop, value);
            }
        }

        return updated.build();
    }

    // Tries to create a repository using the given metadata.
    // The repository is closed immediately after being created, and not persisted anywhere.
    // The method is useful for checking if the metadata can indeed be used to create a repository when needed.
    private void tryCreateRepo(RepositoryMetadata meta) {
        try (var _ = createRepository(meta)) {
            // Intentionally empty block (see method's documentation).
            // If the repository's `close()` method throws an exception, we let the exception be re-thrown,
            // since it's likely that something is wrong with the metadata.
        } catch (Exception e) {
            LOGGER.warn("failed validating metadata for repository [{}]", meta.name(), e);
            throw e;
        }
    }

    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the master node. It removes repository information from cluster metadata.
     *
     * @param request  unregister repository request
     * @return
     */
    public CompletableFuture<ClusterStateUpdateResponse> unregisterRepository(final DeleteRepositoryRequest request) {
        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> updateTask = new AckedClusterStateUpdateTask<>(request) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                if (repositories != null && repositories.repositories().size() > 0) {
                    List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                    boolean changed = false;
                    for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                        if (Regex.simpleMatch(request.name(), repositoryMetadata.name())) {
                            ensureRepositoryNotInUse(currentState, request.name());
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
                if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                    return currentState;
                }
                throw new RepositoryMissingException(request.name());
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // repository was created on both master and data nodes
                return discoveryNode.isMasterEligibleNode() || discoveryNode.isDataNode();
            }
        };
        clusterService.submitStateUpdateTask("delete_repository [" + request.name() + "]", updateTask);
        return updateTask.completionFuture();
    }

    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        final Repository repository = repository(repositoryName);
        final boolean readOnly = repository.isReadOnly();
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {

            @Override
            public void doRun() {
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        verifyAction.verify(repositoryName, readOnly, verificationToken, listener.withOnResponse((delegatedListener, verifyResponse) -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                            try {
                                repository.endVerification(verificationToken);
                            } catch (Exception e) {
                                LOGGER.warn(() -> new ParameterizedMessage(
                                    "[{}] failed to finish repository verification", repositoryName), e);
                                delegatedListener.onFailure(e);
                                return;
                            }
                            delegatedListener.onResponse(verifyResponse);
                        })));
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
                    listener.onResponse(Collections.emptyList());
                }
            }
        });
    }

    static boolean isDedicatedVotingOnlyNode(Set<DiscoveryNodeRole> roles) {
        return roles.contains(DiscoveryNodeRole.MASTER_ROLE) && roles.contains(DiscoveryNodeRole.DATA_ROLE) == false &&
            roles.stream().anyMatch(role -> role.roleName().equals("voting_only"));
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
            final ClusterState state = event.state();
            RepositoriesMetadata oldMetadata = event.previousState().metadata().custom(RepositoriesMetadata.TYPE);
            RepositoriesMetadata newMetadata = state.metadata().custom(RepositoriesMetadata.TYPE);

            // Check if repositories got changed
            if ((oldMetadata == null && newMetadata == null) || (oldMetadata != null && oldMetadata.equalsIgnoreGenerations(newMetadata))) {
                for (Repository repo : repositories.values()) {
                    repo.updateState(state);
                }
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
            for (Repository repo : builder.values()) {
                repo.updateState(state);
            }
            repositories = Collections.unmodifiableMap(builder);
        } catch (Exception ex) {
            assert false : new AssertionError(ex);
            LOGGER.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Gets the {@link RepositoryData} for the given repository.
     *
     * @param repositoryName repository name
     * @return repository data
     */
    public CompletableFuture<RepositoryData> getRepositoryData(final String repositoryName) {
        try {
            Repository repository = repository(repositoryName);
            return repository.getRepositoryData();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Returns registered repository.
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

        repository = internalRepositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }

        throw new RepositoryMissingException(repositoryName);
    }

    public void registerInternalRepository(String name, String type) {
        RepositoryMetadata metadata = new RepositoryMetadata(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.computeIfAbsent(name, (n) -> {
            LOGGER.debug("put internal repository [{}][{}]", name, type);
            return createRepository(metadata);
        });
        if (type.equals(repository.getMetadata().type()) == false) {
            LOGGER.warn(new ParameterizedMessage("internal repository [{}][{}] already registered. this prevented the registration of " +
                                                 "internal repository [{}][{}].", name, repository.getMetadata().type(), name, type));
        } else if (repositories.containsKey(name)) {
            LOGGER.warn(new ParameterizedMessage("non-internal repository [{}] already registered. this repository will block the " +
                                                 "usage of internal repository [{}][{}].", name, metadata.type(), name));
        }
    }

    public void unregisterInternalRepository(String name) {
        Repository repository = internalRepositories.remove(name);
        if (repository != null) {
            RepositoryMetadata metadata = repository.getMetadata();
            LOGGER.debug(() -> new ParameterizedMessage("delete internal repository [{}][{}].", metadata.type(), name));
            closeRepository(repository);
        }
    }

    /** Closes the given repository. */
    private void closeRepository(Repository repository) {
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
        Repository repository = null;
        try {
            repository = factory.create(repositoryMetadata);
            repository.start();
            return repository;
        } catch (RepositoryException e) {
            IOUtils.closeWhileHandlingException(repository);
            throw e;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(repository);
            LOGGER.warn("failed to create repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name(), e);
            throw new RepositoryException(
                repositoryMetadata.name(),
                "failed to create repository: " + e.getMessage(),
                e
            );
        }
    }

    private void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used");
        }
    }

    public Collection<Repository> getRepositoriesList() {
        return Collections.unmodifiableCollection(repositories.values());
    }

    public Map<String, Repository.Factory> typesRegistry() {
        return typesRegistry;
    }


    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    private static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        final SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
            if (repository.equals(snapshot.snapshot().getRepository())) {
                return true;
            }
        }
        for (SnapshotDeletionsInProgress.Entry entry :
            clusterState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries()) {
            if (entry.repository().equals(repository)) {
                return true;
            }
        }
        for (RestoreInProgress.Entry entry : clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (repository.equals(entry.snapshot().getRepository())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        clusterService.removeApplier(this);
        IOUtils.close(Lists.concat(internalRepositories.values(), repositories.values()));
    }
}
