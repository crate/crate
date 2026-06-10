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

package org.elasticsearch.gateway;

import java.io.IOException;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;


/// @deprecated Removed support for dangling indices in 6.4 because index state
/// files no longer contain mapping information and require corresponding
/// RelationMetadata information to be useful.
///
/// This is kept for BWC to still accept requests from older nodes but they'll be noops
public class LocalAllocateDangledIndices {

    public static final String ACTION_NAME = "internal:gateway/local/allocate_dangled";

    @Inject
    public LocalAllocateDangledIndices(TransportService transportService) {
        transportService.registerRequestHandler(
            ACTION_NAME,
            ThreadPool.Names.SAME,
            AllocateDangledRequest::new,
            new AllocateDangledRequestHandler()
        );
    }

    class AllocateDangledRequestHandler implements TransportRequestHandler<AllocateDangledRequest> {
        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel) throws Exception {

            channel.sendResponse(new AllocateDangledResponse(false));
        }
    }

    public static class AllocateDangledRequest extends TransportRequest {

        final DiscoveryNode fromNode;
        final IndexMetadata[] indices;

        AllocateDangledRequest(DiscoveryNode fromNode, IndexMetadata[] indices) {
            this.fromNode = fromNode;
            this.indices = indices;
        }

        public AllocateDangledRequest(StreamInput in) throws IOException {
            super(in);
            fromNode = new DiscoveryNode(in);
            indices = in.readArray(IndexMetadata::readFrom, IndexMetadata[]::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fromNode.writeTo(out);
            out.writeArray(indices);
        }
    }

    public static class AllocateDangledResponse extends TransportResponse {

        private final boolean ack;

        AllocateDangledResponse(boolean ack) {
            this.ack = ack;
        }

        public AllocateDangledResponse(StreamInput in) throws IOException {
            ack = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(ack);
        }
    }
}
