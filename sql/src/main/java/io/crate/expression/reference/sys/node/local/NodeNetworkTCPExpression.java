/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.reference.sys.node.local;

import io.crate.expression.NestableInput;
import io.crate.monitor.ExtendedNetworkStats;
import io.crate.expression.reference.NestedObjectExpression;

class NodeNetworkTCPExpression extends NestedObjectExpression {

    private static final Long VALUE_UNAVAILABLE = -1L;

    NodeNetworkTCPExpression(ExtendedNetworkStats stats) {
        childImplementations.put(TCPConnectionsExpression.NAME, new TCPConnectionsExpression(stats));
        childImplementations.put(TCPPacketsExpression.NAME, new TCPPacketsExpression(stats));
    }

    private static class TCPConnectionsExpression extends NestedObjectExpression {

        public static final String NAME = "connections";

        private static final String INITIATED = "initiated";
        private static final String ACCEPTED = "accepted";
        private static final String CURR_ESTABLISHED = "curr_established";
        private static final String DROPPED = "dropped";
        private static final String EMBRYONIC_DROPPED = "embryonic_dropped";

        TCPConnectionsExpression(ExtendedNetworkStats stats) {
            addChildImplementations(stats.tcp());
        }

        private void addChildImplementations(final ExtendedNetworkStats.Tcp tcp) {
            childImplementations.put(INITIATED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.activeOpens();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(ACCEPTED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.passiveOpens();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(CURR_ESTABLISHED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.currEstab();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(DROPPED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.estabResets();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(EMBRYONIC_DROPPED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.attemptFails();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
        }

    }

    private static class TCPPacketsExpression extends NestedObjectExpression {

        public static final String NAME = "packets";

        private static final String SENT = "sent";
        private static final String RECEIVED = "received";
        private static final String RETRANSMITTED = "retransmitted";
        private static final String ERRORS_RECEIVED = "errors_received";
        private static final String RST_SENT = "rst_sent";

        TCPPacketsExpression(ExtendedNetworkStats stats) {
            addChildImplementations(stats.tcp());
        }

        private void addChildImplementations(final ExtendedNetworkStats.Tcp tcp) {
            childImplementations.put(SENT, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.outSegs();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(RECEIVED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.inSegs();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(RETRANSMITTED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.retransSegs();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(ERRORS_RECEIVED, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.inErrs();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
            childImplementations.put(RST_SENT, new NestableInput<Long>() {
                @Override
                public Long value() {
                    if (tcp != null) {
                        return tcp.outRsts();
                    }
                    return VALUE_UNAVAILABLE;
                }
            });
        }
    }
}
