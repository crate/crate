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

package org.cratedb.action.import_;

import org.cratedb.import_.Importer;
import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class NodeImportResponse extends NodeOperationResponse implements ToXContent {

    private Importer.Result result;

    NodeImportResponse() {
    }

    public NodeImportResponse(DiscoveryNode discoveryNode, Importer.Result result) {
        super(discoveryNode);
        this.result = result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject();
        builder.field(Fields.NODE_ID, this.getNode().id());
        builder.field(Fields.TOOK, result.took);
        builder.startArray(Fields.IMPORTED_FILES);
        for (Importer.ImportCounts counts : result.importCounts) {
            builder.startObject();
            builder.field(Fields.FILE_NAME, counts.fileName);
            builder.field(Fields.SUCCESSES, counts.successes);
            builder.field(Fields.FAILURES, counts.failures);
            if (counts.invalid > 0) {
                builder.field(Fields.INVALIDATED, counts.invalid);
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = new Importer.Result();
        result.took = in.readLong();
        int fileCount = in.readInt();
        for (int i = 0; i < fileCount; i++) {
            Importer.ImportCounts counts = new Importer.ImportCounts();
            counts.fileName = in.readString();
            counts.successes = in.readInt();
            counts.failures = in.readInt();
            counts.invalid = in.readInt();
            result.importCounts.add(counts);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(result.took);
        out.writeInt(result.importCounts.size());
        for (Importer.ImportCounts counts : result.importCounts) {
            out.writeString(counts.fileName);
            out.writeInt(counts.successes);
            out.writeInt(counts.failures);
            out.writeInt(counts.invalid);
        }
    }

    public static NodeImportResponse readNew(StreamInput in) throws IOException {
        NodeImportResponse response = new NodeImportResponse();
        response.readFrom(in);
        return response;
    }

    static final class Fields {
        static final XContentBuilderString NODE_ID = new XContentBuilderString("node_id");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString IMPORTED_FILES = new XContentBuilderString("imported_files");
        static final XContentBuilderString FILE_NAME = new XContentBuilderString("file_name");
        static final XContentBuilderString SUCCESSES = new XContentBuilderString("successes");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString INVALIDATED = new XContentBuilderString("invalidated");
    }

    public Importer.Result result() {
        return result;
    }
}
