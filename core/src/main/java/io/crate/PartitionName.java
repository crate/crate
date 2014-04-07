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
import com.google.common.base.Splitter;
import org.apache.commons.codec.binary.Base32;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PartitionName implements Streamable {

    public static final String NULL_MARKER = "n";
    public static final String NOT_NULL_MARKER = "_";
    private static final Base32 BASE32 = new Base32(true);

    private final List<String> values = new ArrayList<>();
    private final String tableName;

    private String partitionName;

    public PartitionName(String tableName, List<String> columns, List<String> values) {
        this(tableName, columns, values, true);
    }

    public PartitionName(String tableName, List<String> columns, List<String> values,
                         boolean create) {
        this.tableName = tableName;
        if (columns.size() != values.size()) {
            // Key/Values count does not match, cannot compute partition name
            if (create) {
                throw new IllegalArgumentException("Missing required partitioned-by values");
            }
            return;
        }
        for (int i=0; i<columns.size(); i++)  {
            this.values.add(values.get(i));
        }
    }

    private PartitionName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isValid() {
        return values.size() > 0;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i=0; i < size; i++) {
            BytesRef value = (BytesRef)DataType.STRING.streamer().readFrom(in);
            if (value == null) {
                values.add(null);
            } else {
                values.add(value.utf8ToString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (String value : values) {
            DataType.STRING.streamer().writeTo(out,
                    value == null ? value : new BytesRef(value));
        }
    }

    @Nullable
    public BytesReference bytes() {
        if (values.size() == 0) {
            return null;
        }
        BytesStreamOutput out = new BytesStreamOutput();
        try {
            writeTo(out);
            out.close();
        } catch (IOException e) {
            //
        }
        return out.bytes();
    }

    @Nullable
    public String stringValue() {
        if (partitionName != null) {
            return partitionName;
        }
        return Joiner.on(".").join(Constants.PARTITIONED_TABLE_PREFIX, tableName, partitionIdent());
    }

    public String partitionIdent() {
        if (values.size() == 0) {
            return null;
        } else if (values.size() == 1) {
            String value = values.get(0);
            return (value == null) ? NULL_MARKER : NOT_NULL_MARKER + value;
        }
        if (bytes() == null) {
            return null;
        }
        BytesReference bytesReference = bytes();
        return BASE32.encodeAsString(bytesReference.toBytes()).toLowerCase(Locale.ROOT);
    }

    @Nullable
    public String toString() {
        return stringValue();
    }

    public List<String> values() {
        return values;
    }

    public static PartitionName fromString(String partitionTableName, String tableName,
                                int columnCount) throws IOException {
        assert partitionTableName != null;
        assert tableName != null;

        Tuple<String, String> splitted = split(partitionTableName);
        assert splitted.v1().equals(tableName) : String.format(
                Locale.ENGLISH, "%s no partition of table %s", partitionTableName, tableName);

        return decodePartitionName(partitionTableName, splitted.v1(), splitted.v2(), columnCount);
    }

    public static PartitionName fromStringSafe(String partitionTableName, int columnCount) throws IOException {
        assert partitionTableName != null;
        Tuple<String, String> parts = split(partitionTableName);
        return decodePartitionName(partitionTableName, parts.v1(), parts.v2(), columnCount);
    }

    /**
     * decode a partitionTableName with all of its pre-splitted parts into
     * and instance of <code>PartitionName</code>
     * @param partitionTableName
     * @param tableName
     * @param valuesString
     * @param columnCount
     * @return
     * @throws IOException
     */
    private static PartitionName decodePartitionName(String partitionTableName,
                                              String tableName,
                                              String valuesString,
                                              int columnCount) throws IOException {
        assert columnCount > 0 : "invalid column count";

        PartitionName partitionName = new PartitionName(tableName);
        if (columnCount > 1) {
            byte[] inputBytes = BASE32.decode(valuesString.toUpperCase(Locale.ROOT));
            BytesStreamInput in = new BytesStreamInput(inputBytes, true);
            partitionName.readFrom(in);
        } else {
            String marker = valuesString.substring(0, 1);
            if (marker.equals(NULL_MARKER)) {
                valuesString = null;
            } else if (marker.equals(NOT_NULL_MARKER)) {
                valuesString = valuesString.substring(1);
            } else {
                throw new IllegalArgumentException("Invalid given input string");
            }
            partitionName.values().add(valuesString);
        }
        partitionName.partitionName = partitionTableName;
        return partitionName;
    }

    public static boolean isPartition(String partitionName, String tableName) {
        try {
            return PartitionName.split(partitionName).v1().equals(tableName);
        } catch (IllegalArgumentException e) {
            return false;
        }
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
        if (splitted.size() < 4 || !splitted.get(1).equals(Constants.PARTITIONED_TABLE_PREFIX.substring(1))) {
            throw new IllegalArgumentException("Invalid partition name");
        }
        return new Tuple<>(splitted.get(2),
                Joiner.on(".").join(splitted.subList(3, splitted.size())));
    }

    /**
     * compute the template name (used with partitioned tables) from a given table name
     */
    public static String templateName(String tableName) {
        return Joiner.on('.').join(Constants.PARTITIONED_TABLE_PREFIX, tableName, "");
    }

    /**
     * extract the tableName from the name of a partition or template
     * @return the tableName as string
     */
    public static String tableName(String partitionOrTemplateName) {
        return PartitionName.split(partitionOrTemplateName).v1();
    }

}
