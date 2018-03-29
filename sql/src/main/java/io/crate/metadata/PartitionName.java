/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.types.StringType;
import org.apache.commons.codec.binary.Base32;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class PartitionName {

    private static final Base32 BASE32 = new Base32(true);

    private final RelationName relationName;

    private List<BytesRef> values;
    private String indexName;
    private String ident;

    public PartitionName(RelationName relationName, List<BytesRef> values) {
        this.relationName = relationName;
        this.values = values;
    }

    public PartitionName(String tableName, List<BytesRef> values) {
        this(Schemas.DOC_SCHEMA_NAME, tableName, values);
    }

    public PartitionName(String schemaName, String tableName, List<BytesRef> values) {
        this(new RelationName(schemaName, tableName), values);
    }

    public PartitionName(String schema, String table, String partitionIdent) {
        this(schema, table, (List<BytesRef>) null);
        this.ident = partitionIdent;
    }

    public static String templateName(String indexName) {
        RelationName relationName = PartitionName.fromIndexOrTemplate(indexName).relationName;
        return templateName(relationName.schema(), relationName.name());
    }

    /**
     * decodes an encoded ident into it's values
     */
    @Nullable
    public static List<BytesRef> decodeIdent(@Nullable String ident) {
        if (ident == null) {
            return ImmutableList.of();
        }
        byte[] inputBytes = BASE32.decode(ident.toUpperCase(Locale.ROOT));
        try (StreamInput in = StreamInput.wrap(inputBytes)) {
            int size = in.readVInt();
            List<BytesRef> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(StringType.INSTANCE.streamer().readValueFrom(in));
            }
            return values;
        } catch (IOException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid partition ident: %s", ident), e);
        }
    }

    @Nullable
    public static String encodeIdent(Collection<? extends BytesRef> values) {
        if (values.size() == 0) {
            return null;
        }

        BytesStreamOutput streamOutput = new BytesStreamOutput(estimateSize(values));
        try {
            streamOutput.writeVInt(values.size());
            for (BytesRef value : values) {
                StringType.INSTANCE.streamer().writeValueTo(streamOutput, value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] bytes = BytesReference.toBytes(streamOutput.bytes());
        String identBase32 = BASE32.encodeAsString(bytes).toLowerCase(Locale.ROOT);
        // decode doesn't need padding, remove it
        int idx = identBase32.indexOf('=');
        if (idx > -1) {
            return identBase32.substring(0, idx);
        }
        return identBase32;
    }

    /**
     * estimates the size the bytesRef values will take if written onto a StreamOutput using the String streamer
     */
    private static int estimateSize(Iterable<? extends BytesRef> values) {
        int expectedEncodedSize = 0;
        for (BytesRef value : values) {
            // 5 bytes for the value of the length itself using vInt
            expectedEncodedSize += 5 + (value != null ? value.length : 0);
        }
        return expectedEncodedSize;
    }

    public String asIndexName() {
        if (indexName == null) {
            indexName = IndexParts.toIndexName(this);
        }
        return indexName;
    }

    /**
     * @return the encoded values of a partition
     */
    @Nullable
    public String ident() {
        if (ident == null) {
            ident = encodeIdent(values);
        }
        return ident;
    }

    public List<BytesRef> values() {
        if (values == null) {
            if (ident == null) {
                return ImmutableList.of();
            } else {
                values = decodeIdent(ident);
            }
        }
        return values;
    }

    public RelationName tableIdent() {
        return relationName;
    }

    @Override
    public String toString() {
        return asIndexName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionName that = (PartitionName) o;
        if (!asIndexName().equals(that.asIndexName())) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return asIndexName().hashCode();
    }

    /**
     * creates a PartitionName from an index or template Name
     * <p>
     * an partition index has the format [&lt;schema&gt;.].partitioned.&lt;table&gt;.[&lt;ident&gt;]
     * a templateName has the same format but without the ident.
     */
    public static PartitionName fromIndexOrTemplate(String indexOrTemplate) {
        assert indexOrTemplate != null : "indexOrTemplate must not be null";

        IndexParts indexParts = new IndexParts(indexOrTemplate);
        if (!indexParts.isPartitioned()) {
            throw new IllegalArgumentException("Invalid index name: " + indexOrTemplate);
        }
        return indexParts.toPartitionName();
    }

    /**
     * compute the template name (used with partitioned tables) from a given schema and table name
     */
    public static String templateName(String schemaName, String tableName) {
        return IndexParts.toIndexName(schemaName, tableName, "");
    }

    /**
     * return the template prefix to match against index names for the given schema and table name
     */
    public static String templatePrefix(String schemaName, String tableName) {
        return templateName(schemaName, tableName) + "*";
    }
}
