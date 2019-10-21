/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.Identifiers;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public final class RelationName implements Writeable {

    private static final Set<String> INVALID_NAME_CHARACTERS = ImmutableSet.of(".");
    private final String schema;
    private final String name;

    public static RelationName of(QualifiedName name, String defaultSchema) {
        List<String> parts = name.getParts();
        Preconditions.checkArgument(parts.size() < 3,
            "Table with more then 2 QualifiedName parts is not supported. only <schema>.<tableName> works.");
        if (parts.size() == 2) {
            return new RelationName(parts.get(0), parts.get(1));
        }
        return new RelationName(defaultSchema, parts.get(0));
    }

    public static RelationName fromBlobTable(Table<?> table) {
        List<String> tableNameParts = table.getName().getParts();
        Preconditions.checkArgument(tableNameParts.size() < 3, "Invalid tableName \"%s\"", table.getName());

        if (tableNameParts.size() == 2) {
            Preconditions.checkArgument(tableNameParts.get(0).equalsIgnoreCase(BlobSchemaInfo.NAME),
                                        "The Schema \"%s\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause",
                                        tableNameParts.get(0));

            return new RelationName(tableNameParts.get(0), tableNameParts.get(1));
        }
        assert tableNameParts.size() == 1 : "tableNameParts.size() must be 1";
        return new RelationName(BlobSchemaInfo.NAME, tableNameParts.get(0));
    }

    public static RelationName fromIndexName(String indexName) {
        IndexParts indexParts = new IndexParts(indexName);
        return indexParts.toRelationName();
    }

    public static String fqnFromIndexName(String indexName) {
        return new IndexParts(indexName).toFullyQualifiedName();
    }

    public RelationName(StreamInput in) throws IOException {
        schema = in.readString();
        name = in.readString();
    }

    public RelationName(String schema, String name) {
        assert schema != null : "schema name must not be null";
        assert name != null : "table name must not be null";
        this.schema = schema;
        this.name = name;
    }

    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public String fqn() {
        return schema + "." + name;
    }

    public String sqlFqn() {
        return Identifiers.quoteIfNeeded(schema) + "." + Identifiers.quoteIfNeeded(name);
    }

    /**
     * @return The indexName for non-partitioned tables or the alias name for partitioned tables.
     */
    public String indexNameOrAlias() {
        if (schema.equalsIgnoreCase(Schemas.DOC_SCHEMA_NAME)) {
            return name;
        } else if (schema.equalsIgnoreCase(BlobSchemaInfo.NAME)) {
            return BlobIndex.fullIndexName(name);
        }
        return fqn();
    }

    public void ensureValidForRelationCreation() throws InvalidSchemaNameException, InvalidRelationName {
        if (!isValidRelationOrSchemaName(schema)) {
            throw new InvalidSchemaNameException(schema);
        }
        if (!isValidRelationOrSchemaName(name)) {
            throw new InvalidRelationName(this);
        }
        if (Schemas.READ_ONLY_SCHEMAS.contains(schema)) {
            throw new IllegalArgumentException("Cannot create relation in read-only schema: " + schema);
        }
    }

    private static boolean isValidRelationOrSchemaName(String name) {
        for (String illegalCharacter : INVALID_NAME_CHARACTERS) {
            if (name.contains(illegalCharacter) || name.length() == 0) {
                return false;
            }
        }
        if (name.startsWith("_")) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        RelationName o = (RelationName) obj;
        return Objects.equal(schema, o.schema) &&
               Objects.equal(name, o.name);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return fqn();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(schema);
        out.writeString(name);
    }
}
