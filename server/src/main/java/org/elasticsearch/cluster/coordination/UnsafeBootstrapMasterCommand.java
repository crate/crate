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

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;

public class UnsafeBootstrapMasterCommand extends ElasticsearchNodeCommand {

    private static final Logger LOGGER = LogManager.getLogger(UnsafeBootstrapMasterCommand.class);

    public static final String CLUSTER_STATE_TERM_VERSION_MSG_FORMAT =
            "Current node cluster state (term, version) pair is (%s, %s)";
    public static final String CONFIRMATION_MSG =
        DELIMITER +
            "\n" +
            "You should only run this tool if you have permanently lost half or more\n" +
            "of the master-eligible nodes in this cluster, and you cannot restore the\n" +
            "cluster from a snapshot. This tool can cause arbitrary data loss and its\n" +
            "use should be your last resort. If you have multiple surviving master\n" +
            "eligible nodes, you should run this tool on the node with the highest\n" +
            "cluster state (term, version) pair.\n" +
            "\n" +
            "Do you want to proceed?\n";

    static final String NOT_MASTER_NODE_MSG = "unsafe-bootstrap tool can only be run on master eligible node";

    static final String NO_NODE_METADATA_FOUND_MSG = "no node meta data is found, node has not been started yet?";

    static final String EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG =
            "last committed voting voting configuration is empty, cluster has never been bootstrapped?";

    public static final String MASTER_NODE_BOOTSTRAPPED_MSG = "Master node was successfully bootstrapped";
    public static final Setting<String> UNSAFE_BOOTSTRAP =
            ClusterService.USER_DEFINED_METADATA.getConcreteSetting("cluster.metadata.unsafe-bootstrap");

    public UnsafeBootstrapMasterCommand() {
        super("Forces the successful election of the current node after the permanent loss of the half or more master-eligible nodes");
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        terminal.println(Terminal.Verbosity.VERBOSE, "Checking node.master setting");
        Boolean master = Node.NODE_MASTER_SETTING.get(settings);
        if (master == false) {
            throw new ElasticsearchException(NOT_MASTER_NODE_MSG);
        }

        return true;
    }

    protected void processNodePaths(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading node metadata");
        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(LOGGER, NamedXContentRegistry.EMPTY, dataPaths);
        if (nodeMetadata == null) {
            throw new ElasticsearchException(NO_NODE_METADATA_FOUND_MSG);
        }

        String nodeId = nodeMetadata.nodeId();
        terminal.println(Terminal.Verbosity.VERBOSE, "Current nodeId is " + nodeId);

        final Tuple<Manifest, Metadata> manifestMetadataTuple = loadMetadata(terminal, dataPaths);
        final Manifest manifest = manifestMetadataTuple.v1();
        final Metadata metadata = manifestMetadataTuple.v2();
        final CoordinationMetadata coordinationMetadata = metadata.coordinationMetadata();
        if (coordinationMetadata == null ||
                coordinationMetadata.getLastCommittedConfiguration() == null ||
                coordinationMetadata.getLastCommittedConfiguration().isEmpty()) {
            throw new ElasticsearchException(EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
        }
        terminal.println(String.format(Locale.ROOT, CLUSTER_STATE_TERM_VERSION_MSG_FORMAT, coordinationMetadata.term(),
                metadata.version()));

        confirm(terminal, CONFIRMATION_MSG);

        CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .clearVotingConfigExclusions()
                .lastAcceptedConfiguration(new CoordinationMetadata.VotingConfiguration(Collections.singleton(nodeId)))
                .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Collections.singleton(nodeId)))
                .build();

        Settings persistentSettings = Settings.builder()
                .put(metadata.persistentSettings())
                .put(UNSAFE_BOOTSTRAP.getKey(), true)
                .build();
        Metadata newMetadata = Metadata.builder(metadata)
                .clusterUUID(Metadata.UNKNOWN_CLUSTER_UUID)
                .generateClusterUuidIfNeeded()
                .clusterUUIDCommitted(true)
                .persistentSettings(persistentSettings)
                .coordinationMetadata(newCoordinationMetadata)
                .build();

        writeNewMetadata(terminal, manifest, manifest.getCurrentTerm(), metadata, newMetadata, dataPaths);

        terminal.println(MASTER_NODE_BOOTSTRAPPED_MSG);
    }
}
