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

package io.crate.execution.ddl.tables;

import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.metadata.RelationName;

public class DropTableRequest extends AcknowledgedRequest<DropTableRequest> {

    private final RelationName relationName;
    private final int tableOid;

    public DropTableRequest(RelationName relationName, int tableOid) {
        this.relationName = relationName;
        this.tableOid = tableOid;
    }

    public RelationName tableIdent() {
        return relationName;
    }

    public int tableOid() {
        return tableOid;
    }

    public DropTableRequest(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            tableOid = in.readVInt();
        } else {
            tableOid = OID_UNASSIGNED;
        }
        if (in.getVersion().before(Version.V_5_8_0)) {
            in.readBoolean(); // isPartitioned
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeVInt(tableOid);
        }
        if (out.getVersion().before(Version.V_5_8_0)) {
            // sets isPartitioned to true,
            // which if wrong shouldn't cause much harm, as it will only result in a
            // templates.remove() call for an item that is missing
            // The opposite (setting it to false, and not deleting a template) would be worse
            out.writeBoolean(true);
        }
    }
}
