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

package io.crate.expression.reference.sys.node;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;

/**
 * The network column of sys.nodes has been deprecated.
 * They remain available for backward compatibility but everything returns 0.
 */
class NodeNetworkTCPStatsExpression extends NestedNodeStatsExpression {

    private static final String CONNECTIONS = "connections";
    private static final String PACKETS = "packets";

    NodeNetworkTCPStatsExpression() {
        childImplementations.put(CONNECTIONS, new TCPConnectionsStatsExpression());
        childImplementations.put(PACKETS, new TCPPacketsStatsExpression());
    }

    private static class TCPConnectionsStatsExpression extends NestedNodeStatsExpression {

        private static final String INITIATED = "initiated";
        private static final String ACCEPTED = "accepted";
        private static final String CURR_ESTABLISHED = "curr_established";
        private static final String DROPPED = "dropped";
        private static final String EMBRYONIC_DROPPED = "embryonic_dropped";

        TCPConnectionsStatsExpression() {
            childImplementations.put(INITIATED, constant(0L));
            childImplementations.put(ACCEPTED, constant(0L));
            childImplementations.put(CURR_ESTABLISHED, constant(0L));
            childImplementations.put(DROPPED, constant(0L));
            childImplementations.put(EMBRYONIC_DROPPED, constant(0L));
        }
    }

    private static class TCPPacketsStatsExpression extends NestedNodeStatsExpression {

        private static final String SENT = "sent";
        private static final String RECEIVED = "received";
        private static final String RETRANSMITTED = "retransmitted";
        private static final String ERRORS_RECEIVED = "errors_received";
        private static final String RST_SENT = "rst_sent";

        TCPPacketsStatsExpression() {
            childImplementations.put(SENT, constant(0L));
            childImplementations.put(RECEIVED, constant(0L));
            childImplementations.put(RETRANSMITTED, constant(0L));
            childImplementations.put(ERRORS_RECEIVED, constant(0L));
            childImplementations.put(RST_SENT, constant(0L));
        }
    }
}
