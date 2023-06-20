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

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetadataStateFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Metadata associated with this node: its persistent node ID and its version.
 * The metadata is persisted in the data folder of this node and is reused across restarts.
 */
public final class NodeMetadata {

    static final String NODE_ID_KEY = "node_id";
    static final String NODE_VERSION_KEY = "node_version";

    private final String nodeId;

    private final Version nodeVersion;

    public NodeMetadata(final String nodeId, final Version nodeVersion) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.nodeVersion = Objects.requireNonNull(nodeVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeMetadata that = (NodeMetadata) o;
        return nodeId.equals(that.nodeId) &&
               nodeVersion.equals(that.nodeVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeVersion);
    }

    @Override
    public String toString() {
        return "NodeMetadata{" +
               "nodeId='" + nodeId + '\'' +
               ", nodeVersion=" + nodeVersion +
               '}';
    }

    public String nodeId() {
        return nodeId;
    }

    public Version nodeVersion() {
        return nodeVersion;
    }

    public NodeMetadata upgradeToCurrentVersion() {
        return upgradeToVersion(Version.CURRENT);
    }

    NodeMetadata upgradeToVersion(Version upgradeVersion) {
        if (nodeVersion.equals(Version.V_EMPTY)) {
            assert upgradeVersion.onOrAfter(Version.V_4_6_0) : "version is required in the node metadata from CrateDB 4.6 onwards";
            return new NodeMetadata(nodeId, upgradeVersion);
        }

        if (nodeVersion.before(upgradeVersion.minimumIndexCompatibilityVersion())) {
            throw new IllegalStateException(
                "cannot upgrade a node from version [" + nodeVersion + "] directly to version [" + upgradeVersion + "]");
        }

        //Ignore hotfix versions, because downgrades for hotfixes are allowed
        if (upgradeVersion.beforeMajorMinor(nodeVersion)) {
            throw new IllegalStateException(
                "cannot downgrade a node from version [" + nodeVersion + "] to version [" + upgradeVersion + "]");
        }

        return nodeVersion.equals(upgradeVersion) ? this : new NodeMetadata(nodeId, upgradeVersion);
    }

    private static class Builder {
        String nodeId;
        Version nodeVersion;

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public void setNodeVersionId(int nodeVersionId) {
            this.nodeVersion = Version.fromId(nodeVersionId);
        }

        public NodeMetadata build() {
            final Version nodeVersion;
            if (this.nodeVersion == null) {
                assert Version.CURRENT.onOrAfter(Version.V_4_6_0) : "version is required in the node metadata from CrateDB 4.6 onwards";
                nodeVersion = Version.V_EMPTY;
            } else {
                nodeVersion = this.nodeVersion;
            }

            return new NodeMetadata(nodeId, nodeVersion);
        }
    }

    static class NodeMetadataStateFormat extends MetadataStateFormat<NodeMetadata> {

        private ObjectParser<Builder, Void> objectParser;

        NodeMetadataStateFormat() {
            super("node-");
            objectParser = new ObjectParser<>("node_meta_data", false, Builder::new);
            objectParser.declareString(Builder::setNodeId, new ParseField(NODE_ID_KEY));
            objectParser.declareInt(Builder::setNodeVersionId, new ParseField(NODE_VERSION_KEY));
        }

        @Override
        protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
            XContentBuilder xContentBuilder = super.newXContentBuilder(type, stream);
            xContentBuilder.prettyPrint();
            return xContentBuilder;
        }

        @Override
        public void toXContent(XContentBuilder builder, NodeMetadata nodeMetadata) throws IOException {
            builder.field(NODE_ID_KEY, nodeMetadata.nodeId);
            builder.field(NODE_VERSION_KEY, nodeMetadata.nodeVersion.internalId);
        }

        @Override
        public NodeMetadata fromXContent(XContentParser parser) throws IOException {
            return objectParser.apply(parser, null).build();
        }
    }

    public static final MetadataStateFormat<NodeMetadata> FORMAT = new NodeMetadataStateFormat();
}
