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

package io.crate.analyze.user;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.Schemas;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Privilege implements Writeable {

    public enum State {
        GRANT,
        DENY,
        REVOKE;

        public static final List<State> VALUES = ImmutableList.copyOf(values());
    }

    public enum Type {
        DQL,
        DML,
        DDL;

        public static final List<Type> VALUES = ImmutableList.copyOf(values());
    }

    public enum Clazz {
        CLUSTER,
        SCHEMA,
        TABLE;

        public static final List<Clazz> VALUES = ImmutableList.copyOf(values());
    }


    private final State state;
    private final PrivilegeIdent ident;
    private final String grantor;

    public Privilege(State state, Type type, Clazz clazz, @Nullable String ident, String grantor) {
        this.state = state;
        this.ident = new PrivilegeIdent(type, clazz, ident);
        this.grantor = grantor;
    }

    public Privilege(StreamInput in) throws IOException {
        state = State.VALUES.get(in.readInt());
        ident = new PrivilegeIdent(in);
        grantor = in.readString();
    }

    public State state() {
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
