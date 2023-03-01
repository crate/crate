/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.dsl.phases;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;

public class ExecutionPhases {

    /**
     * @return true if the executionNodes indicate a execution on the handler node.
     *
     * size == 0 is also true
     * (this currently is the case on MergePhases if there is a direct-response from previous phase to MergePhase)
     */
    public static boolean executesOnHandler(String handlerNode, Collection<String> executionNodes) {
        switch (executionNodes.size()) {
            case 0:
                return true;
            case 1:
                return executionNodes.iterator().next().equals(handlerNode);
            default:
                return false;
        }
    }

    public static ExecutionPhase fromStream(StreamInput in) throws IOException {
        return ExecutionPhase.Type.VALUES.get(in.readVInt()).fromStream(in);
    }

    public static void toStream(StreamOutput out, ExecutionPhase node) throws IOException {
        out.writeVInt(node.type().ordinal());
        node.writeTo(out);
    }

    public static boolean hasDirectResponseDownstream(Collection<String> downstreamNodes) {
        for (String nodeId : downstreamNodes) {
            if (nodeId.equals(ExecutionPhase.DIRECT_RESPONSE)) {
                return true;
            }
        }
        return false;
    }

    public static String debugPrint(ExecutionPhase phase) {
        StringBuilder sb = new StringBuilder("{phase:{id: ");
        sb.append("'");
        sb.append(phase.phaseId());
        sb.append("/");
        sb.append(phase.name());
        sb.append("'");
        sb.append(", ");
        sb.append("nodes:");
        sb.append("'");
        sb.append(phase.nodeIds());
        sb.append("'");
        if (phase instanceof UpstreamPhase) {
            UpstreamPhase uPhase = (UpstreamPhase) phase;
            sb.append(", dist:");
            sb.append(uPhase.distributionInfo().distributionType());
        }
        sb.append("}}");
        return sb.toString();
    }
}
