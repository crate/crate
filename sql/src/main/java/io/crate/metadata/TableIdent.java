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
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.exceptions.InvalidTableNameException;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TableIdent implements Streamable {

    private static final Set<String> INVALID_TABLE_NAME_CHARACTERS = ImmutableSet.of(".");

    @Nullable
    private String schema;
    private String name;


    public static TableIdent of(Table tableNode, @Nullable String fallbackSchema) {
        List<String> parts = tableNode.getName().getParts();
        Preconditions.checkArgument(parts.size() < 3,
                "Table with more then 2 QualifiedName parts is not supported. only <schema>.<tableName> works.");
        if (parts.size() == 2) {
            return new TableIdent(parts.get(0), parts.get(1));
        }
        return new TableIdent(fallbackSchema, parts.get(0));
    }

    public static TableIdent fromStream(StreamInput in) throws IOException {
        TableIdent tableIdent = new TableIdent();
        tableIdent.readFrom(in);
        return tableIdent;
    }

    public TableIdent(@Nullable String schema, String name) {
        assert name != null : "table name must not be null";

        this.schema = schema;
        this.name = name;
    }

    private TableIdent() {

    }

    @Nullable
    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public String fqn() {
        if (schema == null || schema.equalsIgnoreCase(Schemas.DEFAULT_SCHEMA_NAME)) {
            return name;
        }
        return String.format("%s.%s", schema, name);
    }

    public String esName() {
        return fqn();
    }

    public void validate() throws InvalidSchemaNameException, InvalidTableNameException {
        if (schema != null && !isValidTableOrSchemaName(schema)) {
            throw new InvalidSchemaNameException(schema);
        }
        if (!isValidTableOrSchemaName(name)) {
            throw new InvalidTableNameException(name);
        }
    }

    private static boolean isValidTableOrSchemaName(String name) {
        for (String illegalCharacter: INVALID_TABLE_NAME_CHARACTERS) {
            if (name.contains(illegalCharacter) || name.length() == 0) {
                return false;
            }
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
        TableIdent o = (TableIdent) obj;
        return Objects.equal(schema, o.schema) &&
                Objects.equal(name, o.name);
    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (schema == null) {
            return name;
        }
        return String.format("%s.%s", schema, name);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        schema = in.readOptionalString();
        name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(schema);
        out.writeString(name);
    }
}
