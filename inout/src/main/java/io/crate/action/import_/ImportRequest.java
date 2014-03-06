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

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ImportRequest extends NodesOperationRequest<ImportRequest> {

    private BytesReference source;

    private String type;
    private String index;

    /**
     * Constructs a new import request against the provided nodes. No nodes provided
     * means it will run against all nodes.
     */
    public ImportRequest(String... nodeIds) {
        super(nodeIds);
    }

    /**
     * The query source to execute.
     * @return
     */
    public BytesReference source() {
        return source;
    }

    public ImportRequest source(String source) {
        return this.source(new BytesArray(source), false);
    }

    public ImportRequest source(BytesReference source, boolean unsafe) {
        this.source = source;
        return this;
    }

    public String type() {
        return this.type;
    }

    public void type(String type) {
        this.type = type;
    }

    public String index() {
        return this.index ;
    }

    public void index(String index) {
        this.index = index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readOptionalString();
        type = in.readOptionalString();
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(index);
        out.writeOptionalString(type);
        out.writeBytesReference(source);
    }

}
