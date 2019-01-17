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

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * An interface for interacting with a repository in snapshot and restore.
 * <p>
 * Implementations are responsible for reading and writing both metadata and shard data to and from
 * a repository backend.
 * <p>
 * To perform a snapshot:
 * <ul>
 * <li>Master calls {@link #initializeSnapshot(SnapshotId, List, org.elasticsearch.cluster.metadata.MetaData)}
 * with list of indices that will be included into the snapshot</li>
 * <li>Data nodes call {@link Repository#snapshotShard(IndexShard, Store, SnapshotId, IndexId, IndexCommit, IndexShardSnapshotStatus)}
 * for each shard</li>
 * <li>When all shard calls return master calls {@link #finalizeSnapshot} with possible list of failures</li>
 * </ul>
 */
public interface Repository extends LifecycleComponent {

    /**
     * An factory interface for constructing repositories.
     * See {@link org.elasticsearch.plugins.RepositoryPlugin}.
     */
    interface Factory {
        /**
         * Constructs a repository.
         * @param metadata    metadata for the repository including name and settings
         */
        Repository create(RepositoryMetaData metadata) throws Exception;

        default Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
            return create(metaData);
        }
    }

    /**
     * Returns metadata about this repository.
     */
    RepositoryMetaData getMetadata();

    /**
     * Reads snapshot description from repository.
     *
     * @param snapshotId  snapshot id
     * @return information about snapshot
     */
    SnapshotInfo getSnapshotInfo(SnapshotId snapshotId);

    /**
     * Returns global metadata associated with the snapshot.
     *
     * @param snapshotId the snapshot id to load the global metadata from
     * @return the global metadata about the snapshot
     */
    MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId);

    /**
     * Returns the index metadata associated with the snapshot.
     *
     * @param snapshotId the snapshot id to load the index metadata from
     * @param index      the {@link IndexId} to load the metadata from
     * @return the index metadata about the given index for the given snapshot
     */
    IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException;

    /**
     * Returns a {@link RepositoryData} to describe the data in the repository, including the snapshots
     * and the indices across all snapshots found in the repository.  Throws a {@link RepositoryException}
     * if there was an error in reading the data.
     */
    RepositoryData getRepositoryData();

    /**
     * Starts snapshotting process
     *
     * @param snapshotId snapshot id
     * @param indices    list of indices to be snapshotted
     * @param metaData   cluster metadata
     */
    void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData);

    /**
     * Finalizes snapshotting process
     * <p>
     * This method is called on master after all shards are snapshotted.
     *
     * @param snapshotId    snapshot id
     * @param indices       list of indices in the snapshot
     * @param startTime     start time of the snapshot
     * @param failure       global failure reason or null
     * @param totalShards   total number of shards
     * @param shardFailures list of shard failures
     * @param repositoryStateId the unique id identifying the state of the repository when the snapshot began
     * @param includeGlobalState include cluster global state
     * @return snapshot description
     */
    SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                  List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState);

    /**
     * Deletes snapshot
     *
     * @param snapshotId snapshot id
     * @param repositoryStateId the unique id identifying the state of the repository when the snapshot deletion began
     */
    void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId);

    /**
     * Returns snapshot throttle time in nanoseconds
     */
    long getSnapshotThrottleTimeInNanos();

    /**
     * Returns restore throttle time in nanoseconds
     */
    long getRestoreThrottleTimeInNanos();


    /**
     * Verifies repository on the master node and returns the verification token.
     * <p>
     * If the verification token is not null, it's passed to all data nodes for verification. If it's null - no
     * additional verification is required
     *
     * @return verification token that should be passed to all Index Shard Repositories for additional verification or null
     */
    String startVerification();

    /**
     * Called at the end of repository verification process.
     * <p>
     * This method should perform all necessary cleanup of the temporary files created in the repository
     *
     * @param verificationToken verification request generated by {@link #startVerification} command
     */
    void endVerification(String verificationToken);

    /**
     * Verifies repository settings on data node.
     * @param verificationToken value returned by {@link org.elasticsearch.repositories.Repository#startVerification()}
     * @param localNode         the local node information, for inclusion in verification errors
     */
    void verify(String verificationToken, DiscoveryNode localNode);

    /**
     * Returns true if the repository supports only read operations
     * @return true if the repository is read/only
     */
    boolean isReadOnly();

    /**
     * Creates a snapshot of the shard based on the index commit point.
     * <p>
     * The index commit point can be obtained by using {@link org.elasticsearch.index.engine.Engine#acquireLastIndexCommit} method.
     * Repository implementations shouldn't release the snapshot index commit point. It is done by the method caller.
     * <p>
     * As snapshot process progresses, implementation of this method should update {@link IndexShardSnapshotStatus} object and check
     * {@link IndexShardSnapshotStatus#isAborted()} to see if the snapshot process should be aborted.
     * @param shard               shard to be snapshotted
     * @param store               store to be snapshotted
     * @param snapshotId          snapshot id
     * @param indexId             id for the index being snapshotted
     * @param snapshotIndexCommit commit point
     * @param snapshotStatus      snapshot status
     */
    void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                       IndexShardSnapshotStatus snapshotStatus);

    /**
     * Restores snapshot of the shard.
     * <p>
     * The index can be renamed on restore, hence different {@code shardId} and {@code snapshotShardId} are supplied.
     *
     * @param shard           the shard to restore the index into
     * @param snapshotId      snapshot id
     * @param version         version of elasticsearch that created this snapshot
     * @param indexId         id of the index in the repository from which the restore is occurring
     * @param snapshotShardId shard id (in the snapshot)
     * @param recoveryState   recovery state
     */
    void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState);

    /**
     * Retrieve shard snapshot status for the stored snapshot
     *
     * @param snapshotId snapshot id
     * @param version    version of elasticsearch that created this snapshot
     * @param indexId    the snapshotted index id for the shard to get status for
     * @param shardId    shard id
     * @return snapshot status
     */
    IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId);


}
