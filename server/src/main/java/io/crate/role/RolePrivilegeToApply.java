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
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

public class RolePrivilegeToApply implements Writeable {

    private final PrivilegeType state;
    private final Set<String> roleNames;
    private final String grantor;

    public RolePrivilegeToApply(PrivilegeType state, Set<String> roleNames, String grantor) {
        this.state = state;
        this.roleNames = roleNames;
        this.grantor = grantor;
    }

    public RolePrivilegeToApply(StreamInput in) throws IOException {
        state = PrivilegeType.VALUES.get(in.readInt());
        roleNames = in.readSet(StreamInput::readString);
        grantor = in.readString();
    }

    public PrivilegeType state() {
        return state;
    }

    public Set<String> roleNames() {
        return roleNames;
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
        RolePrivilegeToApply that = (RolePrivilegeToApply) o;
        return state == that.state && Objects.equals(roleNames, that.roleNames);
    }

    /**
     * Builds a hash code.
     * <p>grantor</p> is left out by intend, this is just an information variable.
     */
    @Override
    public int hashCode() {
        return Objects.hash(state, roleNames);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(state.ordinal());
        out.writeCollection(roleNames, StreamOutput::writeString);
        out.writeString(grantor);
    }
}
