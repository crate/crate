/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.node;

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.service.NodeService;


public class NodePortExpression extends SysNodeObjectReference {

    public static final String NAME = "port";

    abstract class PortExpression extends SysNodeExpression<Integer> {
    }

    public static final String HTTP = "http";
    public static final String TRANSPORT = "transport";

    private final NodeService nodeService;

    public NodePortExpression(NodeService nodeService) {
        this.nodeService = nodeService;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(HTTP, new PortExpression() {
            @Override
            public Integer value() {
                if (nodeService.info().getHttp() == null) {
                    return null;
                }
                return portFromAddress(nodeService.info().getHttp().address().publishAddress());
            }
        });
        childImplementations.put(TRANSPORT, new PortExpression() {
            @Override
            public Integer value() {
                return portFromAddress(nodeService.stats().getNode().address());
            }
        });
    }

    private Integer portFromAddress(TransportAddress address) {
        Integer port = 0;
        if (address instanceof InetSocketTransportAddress) {
            port = ((InetSocketTransportAddress) address).address().getPort();
        }
        return port;
    }

}
