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

package io.crate.replication.logical.metadata;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.RelationName;

public record RelationMetadata(RelationName name,
                               List<IndexMetadata> indices) implements Writeable {

    public RelationMetadata(StreamInput in) throws IOException {
        this(
            new RelationName(in),
            in.readList(IndexMetadata::readFrom)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeList(indices);
    }

    public static RelationMetadata fromMetadata(RelationName table, Metadata metadata, Predicate<String> filter) {
        String indexNameOrAlias = table.indexNameOrAlias();
        var indexMetadata = metadata.index(indexNameOrAlias);
        if (indexMetadata == null) {
            throw new RelationUnknown(table);
        }
        return new RelationMetadata(table, List.of(indexMetadata));
    }
}
