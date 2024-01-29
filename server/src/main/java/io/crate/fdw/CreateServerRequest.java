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

package io.crate.fdw;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class CreateServerRequest extends AcknowledgedRequest<CreateServerRequest> {

    private final String name;
    private final String fdw;
    private final boolean ifNotExists;
    private final Map<String, Object> options;

    public CreateServerRequest(String name,
                               String fdw,
                               boolean ifNotExists,
                               Map<String, Object> options) {
        this.name = name;
        this.fdw = fdw;
        this.ifNotExists = ifNotExists;
        this.options = options;
    }

    public CreateServerRequest(StreamInput in) throws IOException {
        this.name = in.readString();
        this.fdw = in.readString();
        this.ifNotExists = in.readBoolean();
        this.options = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(fdw);
        out.writeBoolean(ifNotExists);
        out.writeMap(options, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    public String name() {
        return name;
    }

    public String fdw() {
        return fdw;
    }

    public Map<String, Object> options() {
        return options;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }
}
