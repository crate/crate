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
import io.crate.metadata.TableIdent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

    /**
     * Try to match a privilege at the given collection ignoring the type.
     * Also see {@link Privilege#matchPrivilege(Collection, Type, Clazz, String)}.
     */
    public static boolean matchPrivilege(Collection<Privilege> privileges,
                                         Clazz clazz,
                                         @Nullable String ident) {
        return matchPrivilege(privileges, null, clazz, ident);
    }

    /**
     * Try to match a privilege at the given collection.
     * If none is found for the current {@link Clazz}, try to find one on the upper class.
     * If a privilege with a {@link State#DENY} state is found, false is returned.
     */
    public static boolean matchPrivilege(Collection<Privilege> privileges,
                                         @Nullable Type type,
                                         Clazz clazz,
                                         @Nullable String ident) {
        Privilege foundPrivilege = matchPrivilegeOfClass(privileges, type, clazz, ident);
        if (foundPrivilege == null) {
            switch (clazz) {
                case SCHEMA:
                    foundPrivilege = matchPrivilegeOfClass(privileges, type, Clazz.CLUSTER, ident);
                    break;
                case TABLE:
                    String schemaIdent = TableIdent.fromIndexName(ident).schema();
                    foundPrivilege = matchPrivilegeOfClass(privileges, type, Clazz.SCHEMA, schemaIdent);
                    if (foundPrivilege == null) {
                        foundPrivilege = matchPrivilegeOfClass(privileges, type, Clazz.CLUSTER, null);
                    }
            }
        }
        if (foundPrivilege == null) {
            return false;
        }
        switch (foundPrivilege.state()) {
            case GRANT:
                return true;
            case DENY:
            default:
                return false;
        }
    }

    @Nullable
    private static Privilege matchPrivilegeOfClass(Collection<Privilege> privileges,
                                                   @Nullable Type type,
                                                   Clazz clazz,
                                                   @Nullable String ident) {
        Optional<Privilege> matchedPrivilege = privileges.stream()
            .filter(p -> {
                String pIdent = p.ident();
                boolean matched = p.clazz().equals(clazz);
                if (matched && pIdent != null) {
                    matched = pIdent.equals(ident);
                }
                if (matched && type != null) {
                    matched = p.type().equals(type);
                }
                return matched;
            })
            .findFirst();
        return matchedPrivilege.orElse(null);
    }

    private final State state;
    private final Type type;
    private final Clazz clazz;
    @Nullable
    private final String ident;  // for CLUSTER this will be always null, otherwise schemaName, tableName etc.
    private final String grantor;

    public Privilege(State state, Type type, Clazz clazz, @Nullable String ident, String grantor) {
        this.state = state;
        this.type = type;
        this.clazz = clazz;
        this.ident = ident;
        this.grantor = grantor;
    }

    public Privilege(StreamInput in) throws IOException {
        state = State.VALUES.get(in.readInt());
        type = Type.VALUES.get(in.readInt());
        clazz = Clazz.VALUES.get(in.readInt());
        ident = in.readOptionalString();
        grantor = in.readString();
    }

    public State state() {
        return state;
    }

    public Type type() {
        return type;
    }

    public Clazz clazz() {
        return clazz;
    }

    @Nullable
    public String ident() {
        return ident;
    }

    public String grantor() {
        return grantor;
    }

    /**
     * Checks if both the current and the provided privilege point to the same object.
     */
    public boolean onSameObject(Privilege other) {
        if (this == other) {
            return true;
        }
        //noinspection SimplifiableIfStatement
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return type == other.type &&
               clazz == other.clazz &&
               Objects.equals(ident, other.ident);
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
               type == privilege.type &&
               clazz == privilege.clazz &&
               Objects.equals(ident, privilege.ident);
    }

    /**
     * Builds a hash code.
     * <p>grantor</p> is left out by intend, this is just an information variable.
     */
    @Override
    public int hashCode() {
        return Objects.hash(state, type, clazz, ident);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(state.ordinal());
        out.writeInt(type.ordinal());
        out.writeInt(clazz.ordinal());
        out.writeOptionalString(ident);
        out.writeString(grantor);
    }
}
