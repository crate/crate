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

import io.crate.metadata.TableIdent;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class OpenCloseTableOrPartitionRequest extends AcknowledgedRequest<OpenCloseTableOrPartitionRequest> {

    private TableIdent tableIdent;
    @Nullable
    private String partitionIndexName;
    private boolean openTable = false;

    OpenCloseTableOrPartitionRequest() {
    }

    public OpenCloseTableOrPartitionRequest(TableIdent tableIdent,
                                            @Nullable String partitionIndexName,
                                            boolean openTable) {
        this.tableIdent = tableIdent;
        this.partitionIndexName = partitionIndexName;
        this.openTable = openTable;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    @Nullable
    public String partitionIndexName() {
        return partitionIndexName;
    }

    boolean isOpenTable() {
        return openTable;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (tableIdent == null) {
            validationException = addValidationError("table ident must not be null", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readTimeout(in);
        tableIdent = new TableIdent(in);
        partitionIndexName = in.readOptionalString();
        openTable = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeTimeout(out);
        tableIdent.writeTo(out);
        out.writeOptionalString(partitionIndexName);
        out.writeBoolean(openTable);
    }
}
