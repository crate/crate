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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class FunctionIdent implements Comparable<FunctionIdent>, Writeable {

    private final FunctionName fqnName;
    private final List<DataType> argumentTypes;

    public FunctionIdent(FunctionName functionName, List<DataType> argumentTypes) {
        this.fqnName = functionName;
        this.argumentTypes = argumentTypes;
    }

    public FunctionIdent(@Nullable String schema, String name, List<DataType> argumentTypes) {
        this(new FunctionName(schema, name), argumentTypes);
    }

    public FunctionIdent(String name, List<DataType> argumentTypes) {
        this(null, name, argumentTypes);
    }

    public String schema() {
        return fqnName.schema();
    }

    public List<DataType> argumentTypes() {
        return argumentTypes;
    }

    public String name() {
        return fqnName.name();
    }

    public FunctionName fqnName() {
        return fqnName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        FunctionIdent o = (FunctionIdent) obj;
        return Objects.equal(fqnName, o.fqnName) &&
               Objects.equal(argumentTypes, o.argumentTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fqnName, argumentTypes);
    }

    @Override
    public String toString() {
        return "FunctionIdent{" +
               fqnName.toString() +
               ", argumentTypes=" + argumentTypes +
               '}';
    }

    @Override
    public int compareTo(FunctionIdent o) {
        return ComparisonChain.start()
            .compare(fqnName, o.fqnName)
            .compare(argumentTypes, o.argumentTypes, Ordering.<DataType>natural().lexicographical())
            .result();
    }


    public FunctionIdent(StreamInput in) throws IOException {
        fqnName = new FunctionName(in);
        int numTypes = in.readVInt();
        argumentTypes = new ArrayList<>(numTypes);
        for (int i = 0; i < numTypes; i++) {
            argumentTypes.add(DataTypes.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fqnName.writeTo(out);
        out.writeVInt(argumentTypes.size());
        for (DataType argumentType : argumentTypes) {
            DataTypes.toStream(argumentType, out);
        }
    }
}
