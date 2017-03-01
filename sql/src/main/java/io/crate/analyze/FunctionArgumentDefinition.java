/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.analyze;

import com.google.common.base.MoreObjects;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Objects;

public class FunctionArgumentDefinition implements Streamable {

    private String name;
    private DataType type;

    private FunctionArgumentDefinition(@Nullable String name, DataType dataType) {
        this.name = name;
        this.type = dataType;
    }

    private FunctionArgumentDefinition() {
    }

    public static FunctionArgumentDefinition of(String name, DataType dataType) {
        return new FunctionArgumentDefinition(name, dataType);
    }

    public static FunctionArgumentDefinition of(DataType dataType) {
        return new FunctionArgumentDefinition(null, dataType);
    }

    public static FunctionArgumentDefinition fromStream(StreamInput in) throws IOException {
        FunctionArgumentDefinition argumentDefinition = new FunctionArgumentDefinition();
        argumentDefinition.readFrom(in);
        return argumentDefinition;
    }

    public void name(String name) {
        this.name = name;
    }

    public void type(DataType type) {
        this.type = type;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readOptionalString();
        type = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        DataTypes.toStream(type, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;

        final FunctionArgumentDefinition that = (FunctionArgumentDefinition) o;
        return Objects.equals(this.name, that.name)
            && Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("type", type).toString();
    }
}
