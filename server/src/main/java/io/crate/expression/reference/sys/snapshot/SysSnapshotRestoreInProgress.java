/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference.sys.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;

public class SysSnapshotRestoreInProgress {

    private final String id;
    private final String name;
    private final String repository;
    private final String state;
    private final List<ShardRestoreInfo> shards;

    public static SysSnapshotRestoreInProgress of(RestoreInProgress.Entry entry) {
        return new SysSnapshotRestoreInProgress(
            entry.uuid(),
            entry.snapshot().getSnapshotId().getName(),
            entry.snapshot().getRepository(),
            entry.state().name(),
            ShardRestoreInfo.of(entry.shards())
        );
    }

    private SysSnapshotRestoreInProgress(String id,
                                         String name,
                                         String repository,
                                         String state,
                                         List<ShardRestoreInfo> shards) {
        this.id = id;
        this.name = name;
        this.repository = repository;
        this.state = state;
        this.shards = shards;
    }

    public String id() {
        return id;
    }

    public String repository() {
        return repository;
    }

    public String name() {
        return name;
    }

    public String state() {
        return state;
    }

    public List<ShardRestoreInfo> shards() {
        return shards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SysSnapshotRestoreInProgress that = (SysSnapshotRestoreInProgress) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(name, that.name) &&
               Objects.equals(repository, that.repository) &&
               Objects.equals(state, that.state) &&
               Objects.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, repository, state, shards);
    }

    public static class ShardRestoreInfo {

        public static List<ShardRestoreInfo> of(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
            var shardsRestoreInfo = new ArrayList<ShardRestoreInfo>(shards.size());
            for (ObjectObjectCursor<ShardId, RestoreInProgress.ShardRestoreStatus> shardEntry : shards) {
                ShardId shardId = shardEntry.key;
                RestoreInProgress.ShardRestoreStatus status = shardEntry.value;
                shardsRestoreInfo.add(
                    new ShardRestoreInfo(
                        shardId.id(),
                        IndexName.decode(shardId.getIndexName()),
                        status.state())
                );
            }
            return shardsRestoreInfo;
        }

        private final int id;
        private final IndexParts indexParts;
        private final RestoreInProgress.State state;

        public ShardRestoreInfo(int id, IndexParts indexParts, RestoreInProgress.State state) {
            this.id = id;
            this.indexParts = indexParts;
            this.state = state;
        }

        public int id() {
            return id;
        }

        public IndexParts indexParts() {
            return indexParts;
        }

        public RestoreInProgress.State state() {
            return state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardRestoreInfo that = (ShardRestoreInfo) o;
            return id == that.id &&
                   Objects.equals(indexParts.table(), that.indexParts.table()) &&
                   Objects.equals(indexParts.schema(), that.indexParts.schema()) &&
                   Objects.equals(indexParts.partitionIdent(), that.indexParts.partitionIdent()) &&
                   state == that.state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, indexParts, state);
        }
    }

}
