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

package io.crate.role;

import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

public class PrivilegeIdent implements Writeable {

    private final Privilege.Permission type;
    private final Privilege.Securable clazz;
    @Nullable
    private final String ident;  // for CLUSTER this will be always null, otherwise schemaName, tableName etc.

    public PrivilegeIdent(Privilege.Permission type, Privilege.Securable clazz, @Nullable String ident) {
        this.type = type;
        this.clazz = clazz;
        this.ident = ident;
    }

    PrivilegeIdent(StreamInput in) throws IOException {
        type = Privilege.Permission.VALUES.get(in.readInt());
        clazz = Privilege.Securable.VALUES.get(in.readInt());
        ident = in.readOptionalString();
    }

    public Privilege.Permission type() {
        return type;
    }

    public Privilege.Securable clazz() {
        return clazz;
    }

    @Nullable
    public String ident() {
        return ident;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(type.ordinal());
        out.writeInt(clazz.ordinal());
        out.writeOptionalString(ident);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrivilegeIdent that = (PrivilegeIdent) o;
        return type == that.type &&
               clazz == that.clazz &&
               Objects.equals(ident, that.ident);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, clazz, ident);
    }
}
