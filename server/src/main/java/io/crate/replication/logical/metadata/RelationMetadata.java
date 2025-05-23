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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public record RelationMetadata(RelationName name,
                               List<ReplicatedIndex> indices,
                               @Nullable IndexTemplateMetadata template) implements Writeable {

    public record ReplicatedIndex(IndexMetadata indexMetadata, boolean allPrimaryShardsActive) implements Writeable {

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexMetadata.writeTo(out);
            if (out.getVersion().after(Version.V_5_10_5)) {
                out.writeBoolean(allPrimaryShardsActive);
            }
        }

        public static ReplicatedIndex readFrom(StreamInput in) throws IOException {
            if (in.getVersion().after(Version.V_5_10_5)) {
                return new ReplicatedIndex(IndexMetadata.readFrom(in), in.readBoolean());
            } else {
                // Before 5.10.6 we didn't expose indices with non-active primary shards at all.
                return new ReplicatedIndex(IndexMetadata.readFrom(in), true);
            }
        }
    }

    public RelationMetadata(StreamInput in) throws IOException {
        this(
            new RelationName(in),
            in.readList(ReplicatedIndex::readFrom),
            in.readOptionalWriteable(IndexTemplateMetadata::readFrom)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeList(indices);
        out.writeOptionalWriteable(template);
    }

    public static RelationMetadata fromMetadata(RelationName table, Metadata metadata, Predicate<String> filter) {
        String indexNameOrAlias = table.indexNameOrAlias();
        var indexMetadata = metadata.index(indexNameOrAlias);
        if (indexMetadata == null) {
            String templateName = PartitionName.templateName(table.schema(), table.name());
            var templateMetadata = metadata.templates().get(templateName);
            String[] concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.LENIENT_EXPAND_OPEN,
                indexNameOrAlias
            );
            ArrayList<ReplicatedIndex> indicesMetadata = new ArrayList<>(concreteIndices.length);
            for (String concreteIndex : concreteIndices) {
                if (filter.test(concreteIndex)) {
                    indicesMetadata.add(new ReplicatedIndex(metadata.index(concreteIndex), true));
                } else {
                    indicesMetadata.add(new ReplicatedIndex(metadata.index(concreteIndex), false));
                }
            }
            return new RelationMetadata(table, indicesMetadata, templateMetadata);
        }
        boolean allPrimaryShardsActive = filter.test(indexNameOrAlias);
        return new RelationMetadata(table, List.of(new ReplicatedIndex(indexMetadata, allPrimaryShardsActive)), null);
    }
}
