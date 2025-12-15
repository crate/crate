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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jspecify.annotations.Nullable;

import io.crate.metadata.RelationName;

/**
 * This class is deprecated and should not be used in new code. It only exists for backward compatibility to support
 * reading and writing Publication metadata in the old format (<6.0.0) to/from old nodes.
 */
@Deprecated
public record RelationMetadata(RelationName name,
                               List<IndexMetadata> indices,
                               @Nullable IndexTemplateMetadata template) implements Writeable {

    public RelationMetadata(StreamInput in) throws IOException {
        this(
            new RelationName(in),
            in.readList(IndexMetadata::readFrom),
            in.readOptionalWriteable(IndexTemplateMetadata::readFrom)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeList(indices);
        out.writeOptionalWriteable(template);
    }

    public static RelationMetadata fromMetadata(org.elasticsearch.cluster.metadata.RelationMetadata.Table table,
                                                Metadata metadata) {
        IndexTemplateMetadata templateMetadata = null;
        if (table.partitionedBy().isEmpty() == false) {
            templateMetadata = IndexTemplateMetadata.Builder.of(table);
        }
        List<IndexMetadata> indices = metadata.getIndices(table.name(), List.of(), false, im -> im);
        return new RelationMetadata(table.name(), indices, templateMetadata);
    }
}
