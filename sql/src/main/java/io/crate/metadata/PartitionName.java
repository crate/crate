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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.crate.types.StringType;
import org.apache.commons.codec.binary.Base32;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class PartitionName {

    private static final Base32 BASE32 = new Base32(true);
    private static final Joiner DOT_JOINER = Joiner.on(".");
    private static final Splitter SPLITTER = Splitter.on(".").limit(5);

    private final TableIdent tableIdent;

    private List<BytesRef> values;
    private String indexName;
    private String ident;

    public static final String PARTITIONED_TABLE_PREFIX = ".partitioned";

    public PartitionName(TableIdent tableIdent, List<BytesRef> values) {
        this.tableIdent = tableIdent;
        this.values = values;
    }

    public PartitionName(String tableName, List<BytesRef> values) {
        this(null, tableName, values);
    }

    public PartitionName(@Nullable String schemaName, String tableName, List<BytesRef> values) {
        this(new TableIdent(schemaName, tableName), values);
    }

    public static String indexName(TableIdent tableIdent, String ident) {
        if (tableIdent.schema().equalsIgnoreCase(Schemas.DEFAULT_SCHEMA_NAME)) {
            return DOT_JOINER.join(PARTITIONED_TABLE_PREFIX, tableIdent.name(), ident);
        }
        return DOT_JOINER.join(tableIdent.schema(), PARTITIONED_TABLE_PREFIX, tableIdent.name(), ident);
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
            throw Throwables.propagate(e);
        }
        String identBase32 = BASE32.encodeAsString(streamOutput.bytes().toBytes()).toLowerCase(Locale.ROOT);
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
            indexName = indexName(tableIdent, ident());
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

    public TableIdent tableIdent(){
        return tableIdent;
    }

    @Nullable
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
     *
     * an partition index has the format [&lt;schema&gt;.].partitioned.&lt;table&gt;.[&lt;ident&gt;]
     * a templateName has the same format but without the ident.
     */
    public static PartitionName fromIndexOrTemplate(String indexOrTemplate) {
        assert indexOrTemplate != null;

        List<String> parts = SPLITTER.splitToList(indexOrTemplate);

        String schema;
        String partitioned;
        String table;
        String ident;
        switch(parts.size()) {
            case 4:
                // ""."partitioned"."table_name". ["ident"]
                schema = null;
                partitioned = parts.get(1);
                table = parts.get(2);
                ident = parts.get(3);
                break;
            case 5:
                // "schema".""."partitioned"."table_name". ["ident"]
                schema = parts.get(0);
                partitioned = parts.get(2);
                table = parts.get(3);
                ident = parts.get(4);
                break;
            default:
                throw new IllegalArgumentException("Invalid partition name: " + indexOrTemplate);
        }
        if (!partitioned.equals("partitioned")) {
            throw new IllegalArgumentException("Invalid partition name: " + indexOrTemplate);
        }

        PartitionName partitionName = new PartitionName(schema, table, null);
        partitionName.ident = ident;
        return partitionName;
    }

    public static boolean isPartition(String index) {
        return index.length() > PARTITIONED_TABLE_PREFIX.length() + 1 &&
                (index.startsWith(PARTITIONED_TABLE_PREFIX + ".") || index.contains("." + PARTITIONED_TABLE_PREFIX + "."));
    }

    /**
     * compute the template name (used with partitioned tables) from a given schema and table name
     */
    public static String templateName(@Nullable String schemaName, String tableName) {
        if (schemaName == null || schemaName.equals(Schemas.DEFAULT_SCHEMA_NAME)) {
            return DOT_JOINER.join(PARTITIONED_TABLE_PREFIX, tableName, "");
        } else {
            return DOT_JOINER.join(schemaName, PARTITIONED_TABLE_PREFIX, tableName, "");
        }
    }
}
