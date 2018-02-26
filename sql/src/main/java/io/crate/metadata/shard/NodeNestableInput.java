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

package io.crate.metadata.shard;

import com.google.common.collect.ImmutableMap;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.LiteralNestableInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.node.DiscoveryNode;

import javax.annotation.Nullable;
import java.util.Map;

public class NodeNestableInput implements NestableInput<Map<String, BytesRef>> {

    private final LiteralNestableInput<BytesRef> nodeIdInput;
    private final LiteralNestableInput<BytesRef> nodeNameInput;
    private final ImmutableMap<String, BytesRef> value;

    public NodeNestableInput(DiscoveryNode localNode) {
        BytesRef nodeId = new BytesRef(localNode.getId());
        BytesRef nodeName = new BytesRef(localNode.getName());
        this.nodeIdInput = new LiteralNestableInput<>(nodeId);
        this.nodeNameInput = new LiteralNestableInput<>(nodeName);
        this.value = ImmutableMap.of(
            "id", nodeId,
            "name", nodeName
        );
    }

    @Nullable
    @Override
    public NestableInput<?> getChild(String name) {
        switch (name) {
            case "id":
                return nodeIdInput;
            case "name":
                return nodeNameInput;

            default:
                return null;
        }
    }

    @Override
    public Map<String, BytesRef> value() {
        return value;
    }
}
