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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.action.import_;

import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeImportRequest extends NodeOperationRequest {

    public static final int DEFAULT_BULK_SIZE = 10000;

    private BytesReference source;
    private String index;
    private String type;



    NodeImportRequest() {
    }

    public NodeImportRequest(String nodeId, ImportRequest request) {
        super(request, nodeId);
        this.source = request.source();
        this.index = request.index();
        this.type = request.type();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
        index = in.readOptionalString();
        type = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
        out.writeOptionalString(index);
        out.writeOptionalString(type);
    }

    public BytesReference source() {
        return source;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public int bulkSize() {
        return DEFAULT_BULK_SIZE;
    }
}
