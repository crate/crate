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

package org.elasticsearch.discovery;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.PeerFinder.TransportAddressConnector;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportService;

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public class HandshakingTransportAddressConnector implements TransportAddressConnector {

    private static final Logger LOGGER = LogManager.getLogger(HandshakingTransportAddressConnector.class);

    // connection timeout for probes
    public static final Setting<TimeValue> PROBE_CONNECT_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.probe.connect_timeout",
            TimeValue.timeValueMillis(3000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // handshake timeout for probes
    public static final Setting<TimeValue> PROBE_HANDSHAKE_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.probe.handshake_timeout",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final TransportService transportService;
    private final TimeValue probeConnectTimeout;
    private final TimeValue probeHandshakeTimeout;

    public HandshakingTransportAddressConnector(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        probeConnectTimeout = PROBE_CONNECT_TIMEOUT_SETTING.get(settings);
        probeHandshakeTimeout = PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings);
    }

    @Override
    public void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener) {
        transportService.getThreadPool().generic().execute(new AbstractRunnable() {
            private final AbstractRunnable thisConnectionAttempt = this;

            @Override
            protected void doRun() {
                // We could skip this if the transportService were already connected to the given address, but the savings would be minimal
                // so we open a new connection anyway.

                final DiscoveryNode targetNode = new DiscoveryNode("", transportAddress.toString(),
                    UUIDs.randomBase64UUID(Randomness.get()), // generated deterministically for reproducible tests
                    transportAddress.address().getHostString(), transportAddress.getAddress(), transportAddress, emptyMap(),
                    emptySet(), Version.CURRENT.minimumCompatibilityVersion());

                LOGGER.trace("[{}] opening probe connection", thisConnectionAttempt);
                transportService.openConnection(
                    targetNode,
                    ConnectionProfile.buildSingleChannelProfile(
                        Type.REG,
                        probeConnectTimeout,
                        probeHandshakeTimeout,
                        TimeValue.MINUS_ONE,
                        null
                    ),
                    new ActionListener<>() {

                        @Override
                        public void onResponse(Connection connection) {
                            LOGGER.trace("[{}] opened probe connection", thisConnectionAttempt);

                            // use NotifyOnceListener to make sure the following line does not result in onFailure being called when
                            // the connection is closed in the onResponse handler
                            transportService.handshake(connection, probeHandshakeTimeout.millis(), new NotifyOnceListener<DiscoveryNode>() {

                                @Override
                                protected void innerOnResponse(DiscoveryNode remoteNode) {
                                    try {
                                        // success means (amongst other things) that the cluster names match
                                        LOGGER.trace("[{}] handshake successful: {}", thisConnectionAttempt, remoteNode);
                                        IOUtils.closeWhileHandlingException(connection);

                                        if (remoteNode.equals(transportService.getLocalNode())) {
                                            listener.onFailure(new ConnectTransportException(remoteNode, "local node found"));
                                        } else if (remoteNode.isMasterEligibleNode() == false) {
                                            listener.onFailure(new ConnectTransportException(remoteNode, "non-master-eligible node found"));
                                        } else {
                                            transportService.connectToNode(remoteNode, new ActionListener<Void>() {
                                                @Override
                                                public void onResponse(Void ignored) {
                                                    LOGGER.trace("[{}] full connection successful: {}", thisConnectionAttempt, remoteNode);
                                                    listener.onResponse(remoteNode);
                                                }

                                                @Override
                                                public void onFailure(Exception e) {
                                                    // we opened a connection and successfully performed a handshake, so we're definitely
                                                    // talking to a master-eligible node with a matching cluster name and a good version,
                                                    // but the attempt to open a full connection to its publish address failed; a common
                                                    // reason is that the remote node is listening on 0.0.0.0 but has made an inappropriate
                                                    // choice for its publish address.
                                                    LOGGER.warn(new ParameterizedMessage(
                                                        "[{}] completed handshake with [{}] but followup connection failed",
                                                        thisConnectionAttempt, remoteNode), e);
                                                    listener.onFailure(e);
                                                }
                                            });
                                        }
                                    } catch (Exception e) {
                                        listener.onFailure(e);
                                    }
                                }

                                @Override
                                protected void innerOnFailure(Exception e) {
                                    // we opened a connection and successfully performed a low-level handshake, so we were definitely
                                    // talking to an Elasticsearch node, but the high-level handshake failed indicating some kind of
                                    // mismatched configurations (e.g. cluster name) that the user should address

                                    boolean nodeClosed = e instanceof AlreadyClosedException
                                        || (e instanceof SendRequestTransportException transportException
                                            && transportException.getCause() instanceof NodeClosedException);

                                    if (!nodeClosed) {
                                        LOGGER.warn(new ParameterizedMessage("handshake failed for [{}]", thisConnectionAttempt), e);
                                    }
                                    IOUtils.closeWhileHandlingException(connection);
                                    listener.onFailure(e);
                                }
                            });

                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public String toString() {
                return "connectToRemoteMasterNode[" + transportAddress + "]";
            }
        });
    }
}
