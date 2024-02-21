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
import java.util.Collection;
import java.util.SequencedCollection;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class CreateForeignTableRequest extends AcknowledgedRequest<CreateForeignTableRequest> {

    private final RelationName tableName;
    private boolean ifNotExists;
    private final Collection<Reference> columns;
    private final String server;
    private final Settings options;

    public CreateForeignTableRequest(RelationName tableName,
                                     boolean ifNotExists,
                                     SequencedCollection<Reference> columns,
                                     String server,
                                     Settings options) {
        this.tableName = tableName;
        this.ifNotExists = ifNotExists;
        this.columns = columns;
        this.server = server;
        this.options = options;
    }

    public CreateForeignTableRequest(StreamInput in) throws IOException {
        this.tableName = new RelationName(in);
        this.ifNotExists = in.readBoolean();
        this.columns = in.readList(Reference::fromStream);
        this.server = in.readString();
        this.options = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        tableName.writeTo(out);
        out.writeBoolean(ifNotExists);
        out.writeCollection(columns, Reference::toStream);
        out.writeString(server);
        Settings.writeSettingsToStream(options, out);
    }

    public RelationName tableName() {
        return tableName;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Collection<Reference> columns() {
        return columns;
    }

    public String server() {
        return server;
    }

    public Settings options() {
        return options;
    }
}
