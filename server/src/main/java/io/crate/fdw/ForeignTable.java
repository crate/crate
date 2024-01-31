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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;

public record ForeignTable(RelationName name,
                           Map<ColumnIdent, Reference> references,
                           String server,
                           Map<String, Object> options) implements Writeable, ToXContent, RelationInfo {

    ForeignTable(StreamInput in) throws IOException {
        this(
            new RelationName(in),
            in.readMap(LinkedHashMap::new, ColumnIdent::new, Reference::fromStream),
            in.readString(),
            in.readMap(StreamInput::readString, StreamInput::readGenericValue)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeMap(references, (o, v) -> v.writeTo(o), Reference::toStream);
        out.writeString(server);
        out.writeMap(options, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO:
        //
        return builder;
    }

    @Override
    public Collection<Reference> columns() {
        return references.values();
    }

    @Override
    public Iterator<Reference> iterator() {
        return references.values().iterator();
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return name;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return List.of();
    }

    @Override
    public Settings parameters() {
        return Settings.EMPTY;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.READ_ONLY;
    }

    @Override
    public RelationType relationType() {
        return RelationType.FOREIGN;
    }
}
