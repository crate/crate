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

package io.crate.execution.ddl;

import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.metadata.RelationName;

public final class RelationNameSwap implements Writeable {

    private final RelationName source;
    private final int sourceOid;
    private final RelationName target;
    private final int targetOid;

    public RelationNameSwap(RelationName source, int sourceOid, RelationName target, int targetOid) {
        this.source = source;
        this.sourceOid = sourceOid;
        this.target = target;
        this.targetOid = targetOid;
    }

    public RelationNameSwap(StreamInput in) throws IOException {
        this.source = new RelationName(in);
        this.target = new RelationName(in);
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            this.sourceOid = in.readVInt();
            this.targetOid = in.readVInt();
        } else {
            this.sourceOid = OID_UNASSIGNED;
            this.targetOid = OID_UNASSIGNED;
        }
    }

    public RelationName source() {
        return source;
    }

    public int sourceOid() {
        return sourceOid;
    }

    public RelationName target() {
        return target;
    }

    public int targetOid() {
        return targetOid;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source.writeTo(out);
        target.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeVInt(sourceOid);
            out.writeVInt(targetOid);
        }
    }
}
