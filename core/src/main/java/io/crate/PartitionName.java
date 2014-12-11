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

package io.crate;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import io.crate.core.StringUtils;
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
    private String tableName;

    private String partitionName;
    private String ident;

    private static final Predicate indexNamePartsPredicate = new Predicate<List<String>>() {
        @Override
        public boolean apply(List<String> input) {
            if (input.size() < 4 || !input.get(1).equals(Constants.PARTITIONED_TABLE_PREFIX.substring(1))) {
                return false;
            }
            return true;
        }
    };

    public PartitionName(String tableName, List<BytesRef> values) {
        this.tableName = tableName;
        this.values = values;
    }

    private PartitionName(String tableName) {
        this(tableName, new ArrayList<BytesRef>());
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
            partitionName = DOT_JOINER.join(Constants.PARTITIONED_TABLE_PREFIX, tableName, ident());
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

    public static PartitionName fromString(String partitionTableName, String tableName) {
        assert partitionTableName != null;
        assert tableName != null;

        Tuple<String, String> splitted = split(partitionTableName);
        assert splitted.v1().equals(tableName) : String.format(
                Locale.ENGLISH, "%s no partition of table %s", partitionTableName, tableName);

        return fromPartitionIdent(splitted.v1(), splitted.v2());
    }

    public static PartitionName fromStringSafe(String partitionTableName) {
        assert partitionTableName != null;
        Tuple<String, String> parts = split(partitionTableName);
        return fromPartitionIdent(parts.v1(), parts.v2());
    }

    /**
     * decode a partitionTableName with all of its pre-splitted parts into
     * and instance of <code>PartitionName</code>
     * @param tableName
     * @param partitionIdent
     * @return
     * @throws IOException
     */
    public static PartitionName fromPartitionIdent(String tableName,
                                                   String partitionIdent) {
        PartitionName partitionName = new PartitionName(tableName);
        partitionName.ident = partitionIdent;
        try {
            partitionName.decodeIdent(partitionIdent);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid partition ident: %s", partitionIdent), e);
        }
        return partitionName;
    }

    public static boolean isPartition(String index, String tableName) {
        List<String> splitted = Splitter.on(".").splitToList(index);
        if (!indexNamePartsPredicate.apply(splitted)) {
            return false;
        }
        return splitted.get(2).equals(tableName);
    }

    public static boolean isPartition(String index) {
        List<String> splitted = Splitter.on(".").splitToList(index);
        return indexNamePartsPredicate.apply(splitted);
    }

    /**
     * split a given partition nameor template name into its parts <code>tableName</code>
     * and <code>valuesString</code>.
     * @param partitionOrTemplateName name of a partition or template
     * @return a {@linkplain org.elasticsearch.common.collect.Tuple}
     *         whose first element is the <code>tableName</code>
     *         and whose second element is the <code>valuesString</code>
     * @throws java.lang.IllegalArgumentException if <code>partitionName</code> is no invalid
     */
    public static Tuple<String, String> split(String partitionOrTemplateName) {
        List<String> splitted = Splitter.on(".").splitToList(partitionOrTemplateName);
        if (!indexNamePartsPredicate.apply(splitted)) {
            throw new IllegalArgumentException("Invalid partition name");
        }
        return new Tuple<>(splitted.get(2),
                Joiner.on(".").join(splitted.subList(3, splitted.size())));
    }

    /**
     * compute the template name (used with partitioned tables) from a given table name
     */
    public static String templateName(String tableName) {
        return StringUtils.PATH_JOINER.join(Constants.PARTITIONED_TABLE_PREFIX, tableName, "");
    }

    /**
     * extract the tableName from the name of a partition or template
     * @return the tableName as string
     */
    public static String tableName(String partitionOrTemplateName) {
        return PartitionName.split(partitionOrTemplateName).v1();
    }

    /**
     * extract the ident from the name of a partition or template
     * @return the ident as string
     */
    public static String ident(String partitionOrTemplateName) {
        return PartitionName.split(partitionOrTemplateName).v2();
    }
}
