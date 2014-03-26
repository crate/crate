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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PartitionName implements Streamable {

    private final List<String> values = new ArrayList<>();
    private final String tableName;

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
            values.add(in.readBytesRef().utf8ToString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (String value : values) {
            out.writeBytesRef(new BytesRef(value));
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
        if (values.size() == 0) {
            return null;
        } else if (values.size() == 1) {
            if (values.get(0) == null) {
                return Joiner.on(".").join(Constants.PARTITIONED_TABLE_PREFIX, tableName) + ".";
            }
            return Joiner.on(".").join(Constants.PARTITIONED_TABLE_PREFIX, tableName, values.get(0));
        }
        BytesReference bytesReference = bytes();
        if (bytes() == null) {
            return null;
        }
        return Joiner.on(".").join(Constants.PARTITIONED_TABLE_PREFIX, tableName, Base64.encodeBytes(bytesReference.toBytes()));
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

        String currentPrefix = partitionTableName.substring(0, Constants.PARTITIONED_TABLE_PREFIX.length()+tableName.length()+2);
        String computedPrefix = Joiner.on(".").join(Constants.PARTITIONED_TABLE_PREFIX, tableName) + ".";
        if (!currentPrefix.equals(computedPrefix)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Given partition name '%s' belongs not to table '%s'",
                            partitionTableName, tableName));
        }

        String valuesString = partitionTableName.substring(Constants.PARTITIONED_TABLE_PREFIX.length()+tableName.length()+2);

        PartitionName partitionName = new PartitionName(tableName);
        if (columnCount > 1) {
            byte[] inputBytes = Base64.decode(valuesString);
            BytesStreamInput in = new BytesStreamInput(inputBytes, true);
            partitionName.readFrom(in);
        } else {
            partitionName.values().add(valuesString);
        }
        return partitionName;
    }

    public static String templateName(String tableName) {
        return Joiner.on('.').join(Constants.PARTITIONED_TABLE_PREFIX, tableName, "");
    }

}
