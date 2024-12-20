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

package org.elasticsearch.action.admin.indices.create;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

/**
 * Cluster state update request that allows to create an index
 */
public class CreateIndexClusterStateUpdateRequest extends ClusterStateUpdateRequest<CreateIndexClusterStateUpdateRequest> {

    private final String cause;
    private final String index;
    private Index recoverFrom;
    private ResizeType resizeType;
    private boolean copySettings;

    private Settings settings = Settings.EMPTY;

    private final Set<Alias> aliases = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    public CreateIndexClusterStateUpdateRequest(String cause, String index) {
        this.cause = cause;
        this.index = index;
    }

    public CreateIndexClusterStateUpdateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest aliases(Set<Alias> aliases) {
        this.aliases.addAll(aliases);
        return this;
    }

    public CreateIndexClusterStateUpdateRequest recoverFrom(Index recoverFrom) {
        this.recoverFrom = recoverFrom;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest resizeType(ResizeType resizeType) {
        this.resizeType = resizeType;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest copySettings(final boolean copySettings) {
        this.copySettings = copySettings;
        return this;
    }

    public String cause() {
        return cause;
    }

    public String index() {
        return index;
    }

    public Settings settings() {
        return settings;
    }

    public Set<Alias> aliases() {
        return aliases;
    }

    public Index recoverFrom() {
        return recoverFrom;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Returns the resize type or null if this is an ordinary create index request
     */
    public ResizeType resizeType() {
        return resizeType;
    }

    public boolean copySettings() {
        return copySettings;
    }

}
