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
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import io.crate.types.StringType;
import org.apache.commons.codec.binary.Base32;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PartitionName {

    private static final Base32 BASE32 = new Base32(true);
    private static final Joiner DOT_JOINER = Joiner.on(".");

    private final List<BytesRef> values;
    @Nullable
    private String schemaName;
    private String tableName;

    private String partitionName;
    private String ident;

    public static final String PARTITIONED_TABLE_PREFIX = ".partitioned";
    private static final Predicate<List<String>> indexNamePartsPredicate = new Predicate<List<String>>() {
        @Override
        public boolean apply(List<String> input) {
            boolean result = false;
            switch(input.size()) {
                case 4:
                    // ""."partitioned"."table_name". ["ident"]
                    result = input.get(1).equals(PARTITIONED_TABLE_PREFIX.substring(1));
                    break;
                case 5:
                    // "schema".""."partitioned"."table_name". ["ident"]
                    result = input.get(2).equals(PARTITIONED_TABLE_PREFIX.substring(1));
                    break;
            }
            return result;
        }
    };

    public PartitionName(String tableName, List<BytesRef> values) {
        this(null, tableName, values);
    }

    public PartitionName(@Nullable String schemaName, String tableName, List<BytesRef> values) {
        this.schemaName = ReferenceInfos.DEFAULT_SCHEMA_NAME.equals(schemaName) ? null : schemaName;
        this.tableName = tableName;
        this.values = values;
    }

    private PartitionName(@Nullable String schemaName, String tableName) {
        this(schemaName, tableName, new ArrayList<BytesRef>());
    }

    public boolean isValid() {
        return values.size() > 0;
    }

    @Nullable
    public void decodeIdent(@Nullable String ident) throws IOException {
        if (ident == null) {
            return;
        }
        byte[] inputBytes = BASE32.decode(ident.toUpperCase(Locale.ROOT));
        BytesStreamInput in = new BytesStreamInput(inputBytes, true);
        int size = in.readVInt();
        for (int i=0; i < size; i++) {
            BytesRef value = StringType.INSTANCE.streamer().readValueFrom(in);
            if (value == null) {
                values.add(null);
            } else {
                values.add(value);
            }
        }
    }

    @Nullable
    public String encodeIdent() {
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
            //
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
    private int estimateSize(List<BytesRef> values) {
        int expectedEncodedSize = 0;
        for (BytesRef value : values) {
            // 5 bytes for the value of the length itself using vInt
            expectedEncodedSize += 5 + (value != null ? value.length : 0);
        }
        return expectedEncodedSize;
    }

    public String stringValue() {
        if (partitionName == null) {
            if (schemaName == null) {
                partitionName = DOT_JOINER.join(PARTITIONED_TABLE_PREFIX, tableName, ident());
            } else {
                partitionName = DOT_JOINER.join(schemaName, PARTITIONED_TABLE_PREFIX, tableName, ident());
            }
        }
        return partitionName;
    }

    @Nullable
    public String ident() {
        if (ident == null) {
            ident = encodeIdent();
        }
        return ident;
    }

    public String tableName() {
        return tableName;
    }

    public @Nullable String schemaName() {
        return schemaName;
    }

    @Nullable
    public String toString() {
        return stringValue();
    }

    public List<BytesRef> values() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionName that = (PartitionName) o;

        if (!stringValue().equals(that.stringValue())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return stringValue().hashCode();
    }

    public static PartitionName fromString(String partitionTableName, @Nullable String schemaName, String tableName) {
        assert partitionTableName != null;
        assert tableName != null;

        String[] splitted = split(partitionTableName);
        assert (splitted[0] == null && (schemaName == null || schemaName.equals(ReferenceInfos.DEFAULT_SCHEMA_NAME)))
                || (splitted[0] != null && splitted[0].equals(schemaName))
                || splitted[1].equals(tableName) : String.format(
                Locale.ENGLISH, "%s no partition of table %s", partitionTableName, tableName);

        return fromPartitionIdent(splitted[0], splitted[1], splitted[2]);
    }

    public static PartitionName fromStringSafe(String partitionTableName) {
        assert partitionTableName != null;
        String[] splitted = split(partitionTableName);
        return fromPartitionIdent(splitted[0], splitted[1], splitted[2]);
    }

    /**
     * decode a partitionTableName with all of its pre-splitted parts into
     * and instance of <code>PartitionName</code>
     * @param schemaName
     * @param tableName
     * @param partitionIdent
     * @return
     * @throws IOException
     */
    public static PartitionName fromPartitionIdent(@Nullable String schemaName, String tableName,
                                                   String partitionIdent) {
        PartitionName partitionName = new PartitionName(schemaName, tableName);
        partitionName.ident = partitionIdent;
        try {
            partitionName.decodeIdent(partitionIdent);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid partition ident: %s", partitionIdent), e);
        }
        return partitionName;
    }

    public static boolean isPartition(String index, @Nullable String schemaName, String tableName) {
        List<String> splitted = Splitter.on(".").splitToList(index);
        if (!indexNamePartsPredicate.apply(splitted)) {
            return false;
        } else if (splitted.size() == 4) {
            return (schemaName == null || schemaName.equals(ReferenceInfos.DEFAULT_SCHEMA_NAME)) && splitted.get(2).equals(tableName);
        } else if (splitted.size() == 5) {
            return schemaName != null && schemaName.equals(splitted.get(0)) && splitted.get(3).equals(tableName);
        }
        return false;
    }

    public static boolean isPartition(String index) {
        List<String> splitted = Splitter.on(".").splitToList(index);
        return indexNamePartsPredicate.apply(splitted);
    }

    /**
     * split a given partition name or template name into its parts
     * <code>schemaName</code>, <code>tableName</code> and <code>valuesString</code>.
     *
     * @param partitionOrTemplateName name of a partition or template
     * @return a String array with the following elements:
     *
     *         <ul>
     *             <li><code>schemaName</code> (possibly null) </li>
     *             <li><code>tableName</code></li>
     *             <li><code>valuesString</code></li>
     *         </ul>
     * @throws java.lang.IllegalArgumentException if <code>partitionName</code> is invalid
     */
    public static String[] split(String partitionOrTemplateName) {
        List<String> splitted = Splitter.on(".").splitToList(partitionOrTemplateName);
        if (!indexNamePartsPredicate.apply(splitted)) {
            throw new IllegalArgumentException("Invalid partition name");
        }
        switch(splitted.size()) {
            case 4:
                return new String[]{ null, splitted.get(2), DOT_JOINER.join(splitted.subList(3, splitted.size()))};
            case 5:
                return new String[]{ splitted.get(0), splitted.get(3), DOT_JOINER.join(splitted.subList(4, splitted.size())) };
            default:
                throw new IllegalArgumentException("Invalid partition name");
        }
    }

    /**
     * compute the template name (used with partitioned tables) from a given schema and table name
     */
    public static String templateName(@Nullable String schemaName, String tableName) {
        if (schemaName == null || schemaName.equals(ReferenceInfos.DEFAULT_SCHEMA_NAME)) {
            return DOT_JOINER.join(PARTITIONED_TABLE_PREFIX, tableName, "");
        } else {
            return DOT_JOINER.join(schemaName, PARTITIONED_TABLE_PREFIX, tableName, "");
        }
    }

    /**
     * extract the tableName from the name of a partition or template
     * @return the tableName as string
     */
    public static String tableName(String partitionOrTemplateName) {
        return PartitionName.split(partitionOrTemplateName)[1];
    }

    /**
     * extract the schemaName from the name of a partition or template
     * @return the schemaName as string
     */
    public static String schemaName(String partitionOrTemplateName) {
        String schema = PartitionName.split(partitionOrTemplateName)[0];
        return schema == null ? ReferenceInfos.DEFAULT_SCHEMA_NAME : schema;
    }

    public static Tuple<String, String> schemaAndTableName(String partitionOrTemplateName) {
        String[] splitted = PartitionName.split(partitionOrTemplateName);
        return new Tuple<>(
                MoreObjects.firstNonNull(splitted[0], ReferenceInfos.DEFAULT_SCHEMA_NAME),
                splitted[1]
        );
    }

    /**
     * extract the ident from the name of a partition or template
     * @return the ident as string
     */
    public static String ident(String partitionOrTemplateName) {
        return PartitionName.split(partitionOrTemplateName)[2];
    }
}
