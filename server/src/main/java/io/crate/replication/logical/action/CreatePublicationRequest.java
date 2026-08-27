/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.crate.metadata.RelationName;

public class CreatePublicationRequest extends AcknowledgedRequest<CreatePublicationRequest> {

    private final String owner;
    private final String name;
    private final boolean forAllTables;
    private final List<TableOrPartition> targets;

    public CreatePublicationRequest(String owner, String name, boolean forAllTables, List<TableOrPartition> targets) {
        this.owner = owner;
        this.name = name;
        this.forAllTables = forAllTables;
        this.targets = targets;
    }

    public CreatePublicationRequest(StreamInput in) throws IOException {
        super(in);
        this.owner = in.readString();
        this.name = in.readString();
        this.forAllTables = in.readBoolean();
        int size = in.readVInt();
        var t = new ArrayList<TableOrPartition>(size);
        for (var i = 0; i < size; i++) {
            if (in.getVersion().before(Version.V_6_5_0)) {
                t.add(new TableOrPartition(new RelationName(in), null));
            } else {
                t.add(new TableOrPartition(in));
            }
        }
        this.targets = List.copyOf(t);
    }

    public String owner() {
        return owner;
    }

    public String name() {
        return name;
    }

    public boolean isForAllTables() {
        return forAllTables;
    }

    public List<TableOrPartition> targets() {
        return targets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(owner);
        out.writeString(name);
        out.writeBoolean(forAllTables);
        if (out.getVersion().before(Version.V_6_5_0)) {
            for (var target : targets) {
                if (target.partitionIdent() != null) {
                    throw new IllegalStateException("Cannot write partition publication target to a node before " + Version.V_6_5_0);
                }
            }
        }
        out.writeVInt(targets.size());
        for (var target : targets) {
            if (out.getVersion().before(Version.V_6_5_0)) {
                target.table().writeTo(out);
            } else {
                target.writeTo(out);
            }
        }
    }
}
