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

package io.crate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public record Relation(RelationName relationName, List<String> partitionValues) {

    public static Relation fromPartitionName(PartitionName partitionName) {
        return new Relation(partitionName.relationName(), partitionName.values());
    }

    private static final Version STREAMED_RELATIONSET_VERSION = Version.V_6_0_0;

    public static List<Relation> readList(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(STREAMED_RELATIONSET_VERSION)) {
            return in.readList(s -> new Relation(new RelationName(s), s.readStringList()));
        } else {
            return readFromPre60(in);
        }
    }

    private static List<Relation> readFromPre60(StreamInput in) throws IOException {
        List<Relation> relations = new ArrayList<>();
        String[] indexes = in.readStringArray();
        IndicesOptions.readIndicesOptions(in);
        RelationName table = null;
        List<String> partitionValues = new ArrayList<>();
        for (String index : indexes) {
            IndexParts indexParts = IndexName.decode(index);
            if (table == null) {
                table = indexParts.toRelationName();
            } else {
                if (table.equals(indexParts.toRelationName()) == false) {
                    relations.add(new Relation(table, partitionValues));
                    table = null;
                    partitionValues = new ArrayList<>();
                }
            }
            if (indexParts.isPartitioned()) {
                partitionValues.add(indexParts.partitionIdent());
            }
        }
        relations.add(new Relation(table, partitionValues));
        return relations;
    }

    public static void writeList(StreamOutput out, List<Relation> relations) throws IOException {
        if (out.getVersion().onOrAfter(STREAMED_RELATIONSET_VERSION)) {
            out.writeVInt(relations.size());
            for (Relation r : relations) {
                r.relationName().writeTo(out);
                out.writeStringCollection(r.partitionValues());
            }
        } else {
            out.writeStringCollection(bwcIndicesNames(relations));
            IndicesOptions.LENIENT_EXPAND_OPEN.writeIndicesOptions(out);
        }
    }

    private static List<String> bwcIndicesNames(List<Relation> relations) {
        List<String> output = new ArrayList<>();
        for (Relation r : relations) {
            if (r.partitionValues().isEmpty()) {
                output.add(r.relationName().name());
            } else {
                for (String v : r.partitionValues()) {
                    output.add(IndexName.encode(r.relationName(), v));
                }
            }
        }
        return output;
    }


}
