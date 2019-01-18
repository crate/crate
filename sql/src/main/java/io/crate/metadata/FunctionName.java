/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.google.common.collect.ComparisonChain;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public final class FunctionName implements Comparable<FunctionName>, Writeable {

    @Nullable
    private final String schema;
    private final String name;

    public FunctionName(@Nullable  String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public FunctionName(String name) {
        this(null, name);
    }

    FunctionName(StreamInput in) throws IOException {
        schema = in.readOptionalString();
        name = in.readString();
    }

    @Nullable
    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    @Override
    public int compareTo(FunctionName o) {
        return ComparisonChain.start()
            .compare(schema, o.schema)
            .compare(name, o.name)
            .result();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(schema);
        out.writeString(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionName that = (FunctionName) o;
        return Objects.equals(schema, that.schema) &&
               Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name);
    }

    @Override
    public String toString() {
        return "FunctionQualifiedName{" +
               "schema='" + schema + '\'' +
               ", name='" + name + '\'' +
               '}';
    }
}
