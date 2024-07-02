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

import static org.elasticsearch.Version.V_5_6_0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

public class PrivilegesRequest extends AcknowledgedRequest<PrivilegesRequest> {

    private final Collection<String> roleNames;
    private final Collection<Privilege> privileges;

    @Nullable
    private final GrantedRolesChange grantedRolesChange;

    PrivilegesRequest(Collection<String> roleNames,
                      Collection<Privilege> privileges,
                      @Nullable GrantedRolesChange grantedRolesChange) {
        this.roleNames = roleNames;
        this.privileges = privileges;
        this.grantedRolesChange = grantedRolesChange;
    }

    Collection<String> roleNames() {
        return roleNames;
    }

    public Collection<Privilege> privileges() {
        return privileges;
    }

    @Nullable
    public GrantedRolesChange rolePrivilege() {
        return grantedRolesChange;
    }

    public PrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        int roleNamesSize = in.readVInt();
        roleNames = new ArrayList<>(roleNamesSize);
        for (int i = 0; i < roleNamesSize; i++) {
            roleNames.add(in.readString());
        }
        int privilegesSize = in.readVInt();
        privileges = new ArrayList<>(privilegesSize);
        for (int i = 0; i < privilegesSize; i++) {
            privileges.add(new Privilege(in));
        }
        if (in.getVersion().onOrAfter(V_5_6_0)) {
            grantedRolesChange = in.readOptionalWriteable(GrantedRolesChange::new);
        } else {
            grantedRolesChange = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(roleNames.size());
        for (String roleName : roleNames) {
            out.writeString(roleName);
        }
        out.writeVInt(privileges.size());
        for (Privilege privilege : privileges) {
            privilege.writeTo(out);
        }
        if (out.getVersion().onOrAfter(V_5_6_0)) {
            out.writeOptionalWriteable(grantedRolesChange);
        }
    }
}
