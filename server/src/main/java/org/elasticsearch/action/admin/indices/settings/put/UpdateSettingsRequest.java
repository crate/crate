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

package org.elasticsearch.action.admin.indices.settings.put;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import io.crate.metadata.PartitionName;

/**
 * Request for an update index settings action
 */
public class UpdateSettingsRequest extends AcknowledgedRequest<UpdateSettingsRequest> {

    private final List<PartitionName> partitions;
    private Settings settings = Settings.EMPTY;

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(Settings settings, List<PartitionName> partitions) {
        this.partitions = partitions;
        this.settings = settings;
    }

    public Settings settings() {
        return settings;
    }

    public List<PartitionName> partitions() {
        return partitions;
    }

    public UpdateSettingsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_6_0_0)) {
            partitions = BroadcastRequest.readPartitionNamesFromPre60(in);
        } else {
            partitions = in.readList(PartitionName::new);
        }
        settings = readSettingsFromStream(in);
        if (in.getVersion().before(Version.V_5_10_0)) {
            in.readBoolean(); // preserve existing
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_6_0_0)) {
            BroadcastRequest.writePartitionNamesToPre60(out, partitions);
        } else {
            out.writeCollection(partitions);
        }
        writeSettingsToStream(out, settings);
        if (out.getVersion().before(Version.V_5_10_0)) {
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdateSettingsRequest that = (UpdateSettingsRequest) o;
        return masterNodeTimeout.equals(that.masterNodeTimeout)
                && timeout.equals(that.timeout)
                && Objects.equals(settings, that.settings)
                && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterNodeTimeout, timeout, settings, partitions);
    }

}
