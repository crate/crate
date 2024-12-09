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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.metadata.RelationName;

public sealed interface RelationMetadata extends Writeable permits
    RelationMetadata.BlobTable {

    short ord();

    RelationName name();

    public static RelationMetadata of(StreamInput in) throws IOException {
        short ord = in.readShort();
        return switch (ord) {
            case BlobTable.ORD -> RelationMetadata.BlobTable.of(in);
            default -> throw new IllegalArgumentException("Invalid RelationMetadata ord: " + ord);
        };
    }


    public static void toStream(StreamOutput out, RelationMetadata v) throws IOException {
        out.writeShort(v.ord());
        v.writeTo(out);
    }

    public static record BlobTable(RelationName name, String indexUUID) implements RelationMetadata {

        private static final short ORD = 0;

        static BlobTable of(StreamInput in) throws IOException {
            RelationName name = new RelationName(in);
            String indexUUID = in.readString();
            return new BlobTable(name, indexUUID);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            name.writeTo(out);
            out.writeString(indexUUID);
        }

        @Override
        public short ord() {
            return ORD;
        }
    }
}
