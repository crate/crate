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

package org.elasticsearch.action.admin.cluster.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.IndexName;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class ClusterStateRequest extends MasterNodeReadRequest<ClusterStateRequest> {

    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metadata = true;
    private boolean blocks = true;
    private boolean customs = true;
    private Long waitForMetadataVersion;
    private TimeValue waitForTimeout = DEFAULT_WAIT_FOR_NODE_TIMEOUT;
    private List<RelationName> relationNames = new ArrayList<>();

    public ClusterStateRequest() {
    }

    public ClusterStateRequest all() {
        routingTable = true;
        nodes = true;
        metadata = true;
        blocks = true;
        customs = true;
        relationNames = new ArrayList<>();
        return this;
    }

    public ClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metadata = false;
        blocks = false;
        customs = false;
        relationNames = new ArrayList<>();
        return this;
    }

    public boolean routingTable() {
        return routingTable;
    }

    public ClusterStateRequest routingTable(boolean routingTable) {
        this.routingTable = routingTable;
        return this;
    }

    public boolean nodes() {
        return nodes;
    }

    public ClusterStateRequest nodes(boolean nodes) {
        this.nodes = nodes;
        return this;
    }

    public boolean metadata() {
        return metadata;
    }

    public ClusterStateRequest metadata(boolean metadata) {
        this.metadata = metadata;
        return this;
    }

    public boolean blocks() {
        return blocks;
    }

    public ClusterStateRequest blocks(boolean blocks) {
        this.blocks = blocks;
        return this;
    }

    public ClusterStateRequest relationNames(List<RelationName> relationNames) {
        this.relationNames = relationNames;
        return this;
    }

    public List<RelationName> relationNames() {
        return relationNames;
    }

    public ClusterStateRequest customs(boolean customs) {
        this.customs = customs;
        return this;
    }

    public boolean customs() {
        return customs;
    }

    public TimeValue waitForTimeout() {
        return waitForTimeout;
    }

    public ClusterStateRequest waitForTimeout(TimeValue waitForTimeout) {
        this.waitForTimeout = waitForTimeout;
        return this;
    }

    public Long waitForMetadataVersion() {
        return waitForMetadataVersion;
    }

    public ClusterStateRequest(StreamInput in) throws IOException {
        super(in);
        routingTable = in.readBoolean();
        nodes = in.readBoolean();
        metadata = in.readBoolean();
        blocks = in.readBoolean();
        customs = in.readBoolean();
        String[] oldIndices = Strings.EMPTY_ARRAY;
        if (in.getVersion().before(Version.V_6_0_0)) {
            oldIndices = in.readStringArray();
            IndicesOptions.readIndicesOptions(in);
        }
        waitForTimeout = in.readTimeValue();
        waitForMetadataVersion = in.readOptionalLong();
        String[] oldTemplates = Strings.EMPTY_ARRAY;
        if (in.getVersion().before(Version.V_6_0_0)) {
            oldTemplates = in.readStringArray();
        }
        HashSet<RelationName> relationNamesSet = new HashSet<>();
        if (in.getVersion().onOrAfter(Version.V_6_0_0)) {
            relationNamesSet.addAll(in.readList(RelationName::new));
        }
        for (String index : oldIndices) {
            relationNamesSet.add(IndexName.decode(index).toRelationName());
        }
        for (String template : oldTemplates) {
            RelationName relationName = IndexName.decode(template).toRelationName();
            relationNamesSet.add(relationName);
        }
        relationNames = new ArrayList<>(relationNamesSet);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metadata);
        out.writeBoolean(blocks);
        out.writeBoolean(customs);
        if (out.getVersion().before(Version.V_6_0_0)) {
            // old indices
            out.writeStringArray(relationNames.stream().map(RelationName::indexNameOrAlias).toArray(String[]::new));
            IndicesOptions.LENIENT_EXPAND_OPEN.writeIndicesOptions(out);
        }
        out.writeTimeValue(waitForTimeout);
        out.writeOptionalLong(waitForMetadataVersion);
        if (out.getVersion().before(Version.V_6_0_0)) {
            // old templates
            out.writeStringArray(
                relationNames.stream()
                .map(r -> PartitionName.templateName(r.schema(), r.name()))
                .toArray(String[]::new)
            );
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
            out.writeList(relationNames);
        }
    }

    public ClusterStateRequest waitForMetadataVersion(long waitForMetadataVersion) {
        if (waitForMetadataVersion < 1) {
            throw new IllegalArgumentException("provided waitForMetadataVersion should be >= 1, but instead is [" +
                waitForMetadataVersion + "]");
        }
        this.waitForMetadataVersion = waitForMetadataVersion;
        return this;
    }
}
