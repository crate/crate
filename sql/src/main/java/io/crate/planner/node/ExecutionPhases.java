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

package io.crate.planner.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class ExecutionPhases {

    public static ExecutionPhase fromStream(StreamInput in) throws IOException {
        ExecutionPhase.Type type = ExecutionPhase.Type.values()[in.readVInt()];
        ExecutionPhase node = type.factory().create();
        node.readFrom(in);
        return node;
    }

    public static void toStream(StreamOutput out, ExecutionPhase node) throws IOException {
        out.writeVInt(node.type().ordinal());
        node.writeTo(out);
    }


    public static boolean hasDirectResponseDownstream(List<String> downstreamNodes) {
        for (String nodeId : downstreamNodes) {
            if (nodeId.equals(ExecutionPhase.DIRECT_RETURN_DOWNSTREAM_NODE)) {
                return true;
            }
        }
        return false;
    }
}
