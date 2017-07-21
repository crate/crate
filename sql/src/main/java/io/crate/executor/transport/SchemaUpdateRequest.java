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

package io.crate.executor.transport;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;

import java.io.IOException;

public class SchemaUpdateRequest extends MasterNodeRequest<SchemaUpdateRequest> {

    private Index index;
    private String mappingSource;

    public SchemaUpdateRequest() {
    }

    public SchemaUpdateRequest(Index index, String mappingSource) {
        this.index = index;
        this.mappingSource = mappingSource;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public Index index() {
        return index;
    }

    public String mappingSource() {
        return mappingSource;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        index.writeTo(out);
        out.writeString(mappingSource);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = new Index(in);
        mappingSource = in.readString();
    }
}
