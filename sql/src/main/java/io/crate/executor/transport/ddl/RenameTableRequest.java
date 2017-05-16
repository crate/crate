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

package io.crate.executor.transport.ddl;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class RenameTableRequest extends AcknowledgedRequest<RenameTableRequest> {

    private String[] sourceIndices;
    private String[] targetIndices;

    RenameTableRequest() {
    }

    public RenameTableRequest(String[] sourceIndices, String[] targetIndices) {
        this.sourceIndices = sourceIndices;
        this.targetIndices = targetIndices;
    }

    String[] sourceIndices() {
        return sourceIndices;
    }

    String[] targetIndices() {
        return targetIndices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndices == null || targetIndices == null) {
            validationException = addValidationError("source or target table name must not be null", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceIndices = in.readStringArray();
        targetIndices = in.readStringArray();
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(sourceIndices);
        out.writeStringArray(targetIndices);
        writeTimeout(out);
    }
}
