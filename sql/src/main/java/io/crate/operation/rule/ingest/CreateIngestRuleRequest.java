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

package io.crate.operation.rule.ingest;

import io.crate.metadata.rule.ingest.IngestRule;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CreateIngestRuleRequest extends AcknowledgedRequest<CreateIngestRuleRequest> {

    private String sourceIdent;
    private IngestRule ingestRule;

    CreateIngestRuleRequest() {
    }

    CreateIngestRuleRequest(String sourceIdent, IngestRule ingestRule) {
        this.sourceIdent = sourceIdent;
        this.ingestRule = ingestRule;
    }

    public String getSourceIdent() {
        return sourceIdent;
    }

    public IngestRule getIngestRule() {
        return ingestRule;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIdent == null || sourceIdent.trim().isEmpty()) {
            validationException = ValidateActions.addValidationError("Source ident is missing", null);
        }
        if (ingestRule == null) {
            validationException = ValidateActions.addValidationError("Ingest rule is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceIdent = in.readString();
        ingestRule = new IngestRule(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIdent);
        ingestRule.writeTo(out);
    }
}
