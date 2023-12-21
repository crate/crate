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
import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

public class Privilege implements Writeable {

    public enum Type {
        DQL,
        DML,
        DDL,
        /**
         * Administration Language
         */
        AL;

        public static final List<Type> VALUES = List.of(values());

        public static final List<Type> READ_WRITE_DEFINE = List.of(DQL, DML, DDL);
    }

    public enum Clazz {
        CLUSTER,
        SCHEMA,
        TABLE,
        VIEW;

        public static final List<Clazz> VALUES = List.of(values());
    }


    private final PrivilegeState state;
    private final PrivilegeIdent ident;
    private final String grantor;

    public Privilege(PrivilegeState state, Type type, Clazz clazz, @Nullable String ident, String grantor) {
        this.state = state;
        this.ident = new PrivilegeIdent(type, clazz, ident);
        this.grantor = grantor;
    }

    public Privilege(StreamInput in) throws IOException {
        state = PrivilegeState.VALUES.get(in.readInt());
        ident = new PrivilegeIdent(in);
        grantor = in.readString();
    }

    public PrivilegeState state() {
        return state;
    }

    public PrivilegeIdent ident() {
        return ident;
    }

    public String grantor() {
        return grantor;
    }

    /**
     * Equality validation.
     * <p>grantor</p> is left out by intend, this is just an information variable.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Privilege privilege = (Privilege) o;
        return state == privilege.state &&
               Objects.equals(ident, privilege.ident);
    }

    /**
     * Builds a hash code.
     * <p>grantor</p> is left out by intend, this is just an information variable.
     */
    @Override
    public int hashCode() {
        return Objects.hash(state, ident);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(state.ordinal());
        ident.writeTo(out);
        out.writeString(grantor);
    }
}
