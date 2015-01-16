/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.sys.node;

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.monitor.network.NetworkService;

class NodeNetworkTCPExpression extends SysNodeObjectReference {

    public static final String NAME = "tcp";

    public NodeNetworkTCPExpression(NetworkService networkService) {
        childImplementations.put(TCPConnectionsExpression.NAME, new TCPConnectionsExpression(networkService));
        childImplementations.put(TCPPacketsExpression.NAME, new TCPPacketsExpression(networkService));
    }

    static class TCPConnectionsExpression extends SysNodeObjectReference {

        public static final String NAME = "connections";

        private static final String INITIATED = "initiated";
        private static final String ACCEPTED = "accepted";
        private static final String CURR_ESTABLISHED = "curr_established";
        private static final String DROPPED = "dropped";
        private static final String EMBRYONIC_DROPPED = "embryonic_dropped";

        private final NetworkService networkService;

        protected TCPConnectionsExpression(NetworkService networkService) {
            this.networkService = networkService;
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(INITIATED, new TCPConnectionsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().activeOpens();
                }
            });
            childImplementations.put(ACCEPTED, new TCPConnectionsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().passiveOpens();
                }
            });
            childImplementations.put(CURR_ESTABLISHED, new TCPConnectionsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().currEstab();
                }
            });
            childImplementations.put(DROPPED, new TCPConnectionsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().estabResets();
                }
            });
            childImplementations.put(EMBRYONIC_DROPPED, new TCPConnectionsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().attemptFails();
                }
            });
        }

        private abstract class TCPConnectionsChildExpression extends SysNodeExpression<Long> {

        }
    }

    static class TCPPacketsExpression extends SysNodeObjectReference {

        public static final String NAME = "packets";

        private static final String SENT = "sent";
        private static final String RECEIVED = "received";
        private static final String RETRANSMITTED = "retransmitted";
        private static final String ERRORS_RECEIVED = "errors_received";
        private static final String RST_SENT = "rst_sent";

        private final NetworkService networkService;

        protected TCPPacketsExpression(NetworkService networkService) {
            this.networkService = networkService;
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(SENT, new TCPPacketsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().outSegs();
                }
            });
            childImplementations.put(RECEIVED, new TCPPacketsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().inSegs();
                }
            });
            childImplementations.put(RETRANSMITTED, new TCPPacketsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().retransSegs();
                }
            });
            childImplementations.put(ERRORS_RECEIVED, new TCPPacketsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().inErrs();
                }
            });
            childImplementations.put(RST_SENT, new TCPPacketsChildExpression() {
                @Override
                public Long value() {
                    return networkService.stats().tcp().outRsts();
                }
            });
        }

        private abstract class TCPPacketsChildExpression extends SysNodeExpression<Long> {

        }

    }

}
