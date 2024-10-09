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

package org.elasticsearch.cluster.metadata;

import java.io.IOException;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;

// TODO implement proper difff
public class SchemaMetadata extends AbstractDiffable<SchemaMetadata> {

    private final ImmutableOpenMap<String, RelationMetadata> relations;

    public SchemaMetadata(ImmutableOpenMap<String, RelationMetadata> relations) {
        this.relations = relations;
    }

    public static SchemaMetadata of(StreamInput in) throws IOException {
        ImmutableOpenMap<String, RelationMetadata> relations = in.readImmutableMap(
            StreamInput::readString,
            RelationMetadata::of
        );
        return new SchemaMetadata(relations);
    }

    public static Diff<SchemaMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SchemaMetadata::of, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(relations, StreamOutput::writeString, RelationMetadata::toStream);
    }

    public ImmutableOpenMap<String, RelationMetadata> relations() {
        return relations;
    }

    @Nullable
    public RelationMetadata get(RelationName relation) {
        return relations.get(relation.name());
    }

    @Override
    public int hashCode() {
        return relations.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SchemaMetadata other
            && relations.equals(other.relations);
    }
}
