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

import static io.crate.Constants.DB_NAME;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.sql.Identifiers;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;

public final class RelationName implements Writeable, Accountable {

    private static final Set<String> INVALID_NAME_CHARACTERS = Set.of(".");
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(RelationName.class);

    @Nullable
    private final String schema;
    private final String name;

    public static RelationName of(QualifiedName name, String defaultSchema) {
        List<String> parts = name.getParts();
        if (parts.size() > 3) {
            throw new IllegalArgumentException(
                "Table with more than 3 QualifiedName parts is not supported. Only <catalog>.<schema>.<tableName> works.");
        }
        if (parts.size() == 3) {
            ensureIsCrateCatalog(parts.get(0));
        }
        if (parts.size() >= 2) {
            return new RelationName(parts.get(parts.size() - 2), parts.get(parts.size() - 1));
        }
        return new RelationName(defaultSchema, parts.get(0));
    }

    public static RelationName fromBlobTable(Table<?> table) {
        List<String> tableNameParts = table.getName().getParts();

        if (tableNameParts.size() > 2) {
            throw new IllegalArgumentException("Invalid tableName \"" + table.getName() + "\"");
        }
        if (tableNameParts.size() == 2) {
            if (!tableNameParts.get(0).equals(BlobSchemaInfo.NAME)) {
                throw new IllegalArgumentException(
                    "The Schema \"" + tableNameParts.get(0) + "\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause");
            }
            return new RelationName(tableNameParts.get(0), tableNameParts.get(1));
        }
        assert tableNameParts.size() == 1 : "tableNameParts.size() must be 1";
        return new RelationName(BlobSchemaInfo.NAME, tableNameParts.get(0));
    }

    public static RelationName fromIndexName(String indexName) {
        return IndexName.decode(indexName).toRelationName();
    }

    public static String fqnFromIndexName(String indexName) {
        return IndexName.decode(indexName).toFullyQualifiedName();
    }

    public RelationName(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_4_2_0)) {
            schema = in.readString();
        } else {
            schema = in.readOptionalString();
        }
        name = in.readString();
    }

    public RelationName(@Nullable String schema, String name) {
        assert name != null : "table name must not be null";
        if (schema != null && (isInvalidSchemaOrRelationName(schema))) {
            throw new InvalidSchemaNameException(schema);
        }
        if (isInvalidSchemaOrRelationName(name)) {
            throw new InvalidRelationName(schema == null ? name : schema + "." + name);
        }
        this.schema = schema;
        this.name = name;
    }

    @Nullable
    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public String fqn() {
        if (schema == null) {
            return name;
        }
        return schema + "." + name;
    }

    public String sqlFqn() {
        if (schema == null) {
            return Identifiers.quoteIfNeeded(name);
        }
        return Identifiers.quoteIfNeeded(schema) + "." + Identifiers.quoteIfNeeded(name);
    }

    public QualifiedName toQualifiedName() {
        if (schema == null) {
            return new QualifiedName(name);
        } else {
            return new QualifiedName(List.of(schema, name));
        }
    }

    /**
     * @return The indexName for non-partitioned tables or the alias name for partitioned tables.
     */
    public String indexNameOrAlias() {
        if (schema == null) {
            throw new IllegalStateException(
                "indexNameOrAlias can only be generated from a RelationName that is fully qualified.");
        }
        if (schema.equals(Schemas.DOC_SCHEMA_NAME)) {
            return name;
        } else if (schema.equals(BlobSchemaInfo.NAME)) {
            return BlobIndex.fullIndexName(name);
        }
        return fqn();
    }

    public void ensureValidForRelationCreation() throws InvalidSchemaNameException, InvalidRelationName {
        // Because of TableFunctionRelations such as '_values', '_pg_expandarray', RelationName can start with "_".
        // But it is not allowed to be used as part of DDL statements.
        if (Schemas.READ_ONLY_SYSTEM_SCHEMAS.contains(schema)) {
            throw new IllegalArgumentException("Cannot create relation in read-only schema: " + schema);
        }
        if (schema.equalsIgnoreCase("_all") || name.equalsIgnoreCase("_all")) {
            throw new IllegalArgumentException("\"_all\" cannot be used as schema or table name");
        }
    }

    public static void ensureIsCrateCatalog(String catalogName) {
        if (DB_NAME.equals(catalogName) == false) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Unexpected catalog name: %s. Only available catalog is %s",
                              catalogName,
                              DB_NAME));
        }
    }

    private static boolean isInvalidSchemaOrRelationName(String name) {
        return name.isEmpty() || containsIllegalCharacters(name);
    }

    private static boolean containsIllegalCharacters(String name) {
        for (String illegalCharacter : INVALID_NAME_CHARACTERS) {
            if (name.contains(illegalCharacter)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return fqn();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_4_2_0)) {
            out.writeString(schema == null ? DocSchemaInfo.NAME : schema);
        } else {
            out.writeOptionalString(schema);
        }
        out.writeString(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RelationName that = (RelationName) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + RamUsageEstimator.sizeOf(schema)
            + RamUsageEstimator.sizeOf(name);
    }
}
