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

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

/**
 * A request to create an index.
 * <p>
 * The index created can optionally be created with {@link #settings(org.elasticsearch.common.settings.Settings)}.
 *
 * @see org.elasticsearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see CreateIndexResponse
 */
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest {

    private String cause = "";

    private String index;

    private Settings settings = Settings.EMPTY;

    private final Set<Alias> aliases = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    /**
     * Constructs a new request to create an index with the specified name and settings.
     */
    public CreateIndexRequest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * The index name to create.
     */
    public String index() {
        return index;
    }

    /**
     * The settings to create the index with.
     */
    public Settings settings() {
        return settings;
    }

    /**
     * The cause for this index creation.
     */
    public String cause() {
        return cause;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    /**
     * Adds an alias that will be associated with the index when it gets created
     */
    public CreateIndexRequest alias(Alias alias) {
        this.aliases.add(alias);
        return this;
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that should be active for index creation to return.
     * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
     * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active before returning.
     * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link CreateIndexResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public CreateIndexRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public CreateIndexRequest(StreamInput in) throws IOException {
        super(in);
        cause = in.readString();
        index = in.readString();
        settings = readSettingsFromStream(in);
        Version version = in.getVersion();
        if (version.onOrAfter(Version.V_5_8_0)) {
            // no mapping anymore
        } else if (version.onOrAfter(Version.V_5_0_0)) {
            in.readOptionalString(); // maping
        } else {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                in.readString(); // type, was always "default"
                in.readString(); // mapping
            }
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(new Alias(in));
        }
        if (version.before(Version.V_4_3_0)) {
            in.readBoolean(); // updateAllTypes
        }
        waitForActiveShards = ActiveShardCount.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(index);
        writeSettingsToStream(out, settings);
        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_5_8_0)) {
            // no mapping anymore
        } else if (version.onOrAfter(Version.V_5_0_0)) {
            out.writeOptionalString(null);
        } else {
            // used to be a dict for mapping by type
            out.writeVInt(0);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        if (version.before(Version.V_4_3_0)) {
            out.writeBoolean(true); // updateAllTypes
        }
        waitForActiveShards.writeTo(out);
    }
}
