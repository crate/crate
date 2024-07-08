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

package io.crate.metadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.codec.binary.Base32;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.PartitionUnknownException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.types.DataTypes;

public class PartitionName {

    /**
     * Create a {@link PartitionName} for the given assignments.
     * Assignments usually represent a TABLE PARTITION (pcol1 = value [, ...]) clause.
     *
     * This doesn't check if the partition exists, and doesn't consider the partition column types.
     * Use {@link #ofAssignments(DocTableInfo, List, Metadata)} instead of possible.
     */
    public static PartitionName ofAssignments(RelationName relation, List<Assignment<Object>> assignments) {
        String[] values = new String[assignments.size()];
        int idx = 0;
        for (Assignment<Object> o : assignments) {
            values[idx++] = DataTypes.STRING.implicitCast(o.expression());
        }
        return new PartitionName(relation, List.of(values));
    }

    /**
     * Create a {@link PartitionName} for the given assignments.
     * Assignments usually represent a TABLE PARTITION (pcol1 = value [, ...]) clause.
     *
     * @throws PartitionUnknownException if the partition is missing from the table
     * @throws IllegalArgumentException if the table is not partitioned, or if the properties don't match the partitionBy clause
     */
    public static PartitionName ofAssignments(DocTableInfo table, List<Assignment<Object>> assignments, Metadata metadata) {
        PartitionName partitionName = ofAssignmentsUnsafe(table, assignments);
        if (table.getPartitionNames(metadata).contains(partitionName) == false) {
            throw new PartitionUnknownException(partitionName);
        }
        return partitionName;
    }

    /**
     * Like {@link #ofAssignments(RelationName, List)} but doesn't raise a
     * {@link PartitionUnknownException} in case the table doesn't contain the
     * partition.
     *
     * This should only be used if the action can create a new partition.
     */
    public static PartitionName ofAssignmentsUnsafe(DocTableInfo table, List<Assignment<Object>> assignments) {
        if (!table.isPartitioned()) {
            throw new IllegalArgumentException("table '" + table.ident().fqn() + "' is not partitioned");
        }
        List<ColumnIdent> partitionedBy = table.partitionedBy();
        if (assignments.size() != partitionedBy.size()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "The table \"%s\" is partitioned by %s columns but the PARTITION clause contains %s columns",
                table.ident().fqn(),
                partitionedBy.size(),
                assignments.size()
            ));
        }
        String[] values = new String[partitionedBy.size()];
        for (var assignment : assignments) {
            Object value = assignment.expression();
            ColumnIdent column = ColumnIdent.fromPath(assignment.columnName().toString());
            int idx = partitionedBy.indexOf(column);
            try {
                Reference reference = table.partitionedByColumns().get(idx);
                Object converted = reference.valueType().implicitCast(value);
                values[idx] = DataTypes.STRING.implicitCast(converted);
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "\"%s\" is no known partition column", column.sqlFqn()));
            }
        }
        return new PartitionName(table.ident(), Arrays.asList(values));
    }


    private static final Base32 BASE32 = new Base32(true);

    private final RelationName relationName;

    @Nullable
    private List<String> values;

    @Nullable
    private String indexName;

    @Nullable
    private String ident;


    public PartitionName(RelationName relationName, @NotNull List<String> values) {
        this.relationName = relationName;
        this.values = Objects.requireNonNull(values);
    }

    public PartitionName(RelationName relationName, @NotNull String partitionIdent) {
        this.relationName = relationName;
        this.ident = Objects.requireNonNull(partitionIdent);
    }

    public static String templateName(String indexName) {
        IndexParts indexParts = new IndexParts(indexName);
        if (!indexParts.isPartitioned()) {
            throw new IllegalArgumentException(
                "Cannot convert non-partitioned index name to templateName: " + indexName);
        }
        return templateName(indexParts.getSchema(), indexParts.getTable());
    }

    /**
     * decodes an encoded ident into it's values
     */
    @Nullable
    public static List<String> decodeIdent(@Nullable String ident) {
        if (ident == null) {
            return List.of();
        }
        byte[] inputBytes = BASE32.decode(ident.toUpperCase(Locale.ROOT));
        try (StreamInput in = StreamInput.wrap(inputBytes)) {
            int size = in.readVInt();
            List<String> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(readValueFrom(in));
            }
            return values;
        } catch (IOException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid partition ident: %s", ident), e);
        }
    }

    /**
     * Read utf8 bytes for bwc, with 0 as `null` indicator
     */
    private static String readValueFrom(StreamInput in) throws IOException {
        int length = in.readVInt() - 1;
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        in.readBytes(bytes, 0, length);
        char[] chars = new char[length];
        int len = UnicodeUtil.UTF8toUTF16(bytes, 0, length, chars);
        return new String(chars, 0, len);
    }

    /**
     * Write utf8 bytes for bwc, with 0 as `null` indicator
     */
    private static void writeValueTo(BytesStreamOutput out, @Nullable String value) throws IOException {
        // 1 is always added to the length so that
        // 0 is null
        // 1 is 0
        // ...
        if (value == null) {
            out.writeVInt(0);
        } else {
            byte[] v = value.getBytes(StandardCharsets.UTF_8);
            out.writeVInt(v.length + 1);
            out.writeBytes(v, 0, v.length);
        }
    }

    @Nullable
    public static String encodeIdent(Collection<? extends String> values) {
        if (values.size() == 0) {
            return null;
        }

        BytesStreamOutput streamOutput = new BytesStreamOutput(estimateSize(values));
        try {
            streamOutput.writeVInt(values.size());
            for (String value : values) {
                writeValueTo(streamOutput, value);
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
    private static int estimateSize(Iterable<? extends String> values) {
        int expectedEncodedSize = 0;
        for (String value : values) {
            // 5 bytes for the value of the length itself using vInt
            expectedEncodedSize += 5 + (value != null ? value.length() : 0);
        }
        return expectedEncodedSize;
    }

    public String asIndexName() {
        if (indexName == null) {
            indexName = IndexParts.toIndexName(relationName.schema(), relationName.name(), ident());
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

    public List<String> values() {
        if (values == null) {
            if (ident == null) {
                return List.of();
            } else {
                values = decodeIdent(ident);
            }
        }
        return values;
    }

    public RelationName relationName() {
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
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Trying to create partition name from the name of a non-partitioned table %s", indexOrTemplate)
            );
        }
        return new PartitionName(new RelationName(indexParts.getSchema(), indexParts.getTable()), indexParts.getPartitionIdent());
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
