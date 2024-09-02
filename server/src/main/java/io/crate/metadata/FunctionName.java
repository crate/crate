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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.sql.Identifiers;

public record FunctionName(@Nullable String schema, String name)
        implements Writeable, Accountable {

    public FunctionName(String name) {
        this(null, name);
    }

    public FunctionName(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(schema);
        out.writeString(name);
    }

    @Override
    public long ramBytesUsed() {
        return (schema == null ? 0 : RamUsageEstimator.sizeOf(schema))
            + RamUsageEstimator.sizeOf(name);
    }

    @Override
    public String toString() {
        return "FunctionName{" +
               "schema='" + schema + '\'' +
               ", name='" + name + '\'' +
               '}';
    }

    public String displayName() {
        String functionName;
        if (isBuiltin()) {
            functionName = name;
        } else {
            functionName = Identifiers.quoteIfNeeded(name);
        }
        if (schema == null) {
            return functionName;
        }
        return Identifiers.quoteIfNeeded(schema) + "." + functionName;
    }

    public boolean isBuiltin() {
        return schema == null || InformationSchemaInfo.NAME.equals(schema) || PgCatalogSchemaInfo.NAME.equals(schema);
    }
}
