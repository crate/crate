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

package io.crate.role.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import io.crate.role.GrantedRole;
import io.crate.role.GrantedRolesChange;
import io.crate.role.JwtProperties;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;

public class RolesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "roles";

    private final Map<String, Role> roles;

    public RolesMetadata() {
        this.roles = new HashMap<>();
    }

    public RolesMetadata(Map<String, Role> roles) {
        this.roles = roles;
    }

    public static RolesMetadata newInstance(@Nullable RolesMetadata instance) {
        if (instance == null) {
            return new RolesMetadata();
        }
        return new RolesMetadata(new HashMap<>(instance.roles));
    }

    public static RolesMetadata ofOldUsersMetadata(@Nullable UsersMetadata usersMetadata,
                                                   @Nullable UsersPrivilegesMetadata usersPrivilegesMetadata) {
        if (usersMetadata == null) {
            return null;
        }
        Function<String, Set<Privilege>> getPrivileges = username -> Set.of();
        if (usersPrivilegesMetadata != null) {
            getPrivileges = usersPrivilegesMetadata::getUserPrivileges;
        }
        RolesMetadata rolesMetadata = new RolesMetadata();
        for (var user : usersMetadata.users().entrySet()) {
            var userName = user.getKey();
            var role = new Role(userName, true, getPrivileges.apply(userName), Set.of(), user.getValue(), null);
            rolesMetadata.roles().put(userName, role);
        }
        return rolesMetadata;
    }

    public boolean contains(String name) {
        return roles.containsKey(name);
    }

    /**
     * Combination of iss/username must be unique throughout all users.
     */
    public boolean contains(@Nullable JwtProperties jwtProperties) {
        if (jwtProperties == null) {
            // Short-circuit for CREATE/ALTER user statements without jwt property specified.
            return false;
        }
        for (Role role: roles.values()) {
            var jwtProps = role.jwtProperties();
            if (role.isUser() && jwtProps != null && jwtProps.match(jwtProperties.iss(), jwtProperties.username())) {
                return true;
            }
        }
        return false;
    }

    public Role remove(String name) {
        return roles.remove(name);
    }

    public List<String> roleNames() {
        return new ArrayList<>(roles.keySet());
    }

    public Map<String, Role> roles() {
        return roles;
    }

    public RolesMetadata(StreamInput in) throws IOException {
        int numRoles = in.readVInt();
        roles = HashMap.newHashMap(numRoles);
        for (int i = 0; i < numRoles; i++) {
            var role = new Role(in);
            roles.put(role.name(), role);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(roles.size());
        for (var role : roles.values()) {
            role.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (var role : roles.values()) {
            role.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * RolesMetadata has the form of:
     *
     * roles: {
     *   "role1": {
     *     ...
     *   },
     *   "role2": {
     *     ...
     *   },
     *   ...
     * }
     *
     * The format of each role can be found in {@link Role#fromXContent(XContentParser)}
     */
    public static RolesMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Role> roles = new HashMap<>();
        XContentParser.Token token = parser.nextToken();

        if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(TYPE)) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse roles, expected an object token but got {}", token);
            }
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                var role = Role.fromXContent(parser);
                roles.put(role.name(), role);
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                // each custom metadata is packed inside an object.
                // each custom must move the parser to the end otherwise possible following customs won't be read
                throw new ElasticsearchParseException("failed to parse roles, expected an object token at the end");
            }
        }
        return new RolesMetadata(roles);
    }

    public static RolesMetadata of(Metadata.Builder mdBuilder,
                                   @Nullable UsersMetadata oldUsersMetadata,
                                   @Nullable UsersPrivilegesMetadata oldUserPrivilegesMetadata,
                                   RolesMetadata oldRolesMetadata) {
        RolesMetadata newMetadata;
        // create a new instance of the metadata, to guarantee the cluster changed action
        // and use old UsersMetadata/UsersPrivilegesMetadata if exists
        if (oldUsersMetadata != null) {
            // could be after upgrade or when users have been restored from old snapshot,
            // and we want to override all existing users & roles
            newMetadata = RolesMetadata.ofOldUsersMetadata(oldUsersMetadata, oldUserPrivilegesMetadata);
            mdBuilder.removeCustom(UsersMetadata.TYPE);
            mdBuilder.removeCustom(UsersPrivilegesMetadata.TYPE);
        } else {
            newMetadata = RolesMetadata.newInstance(oldRolesMetadata);
        }
        return newMetadata;
    }

    /**
     * Applies the provided granted/revoked roles to the specified users.
     *
     * @return the number of affected role privileges
     *         (doesn't count no-ops e.g.: granting a role to a user which already has)
     */
    public long applyRolePrivileges(Collection<String> userNames, GrantedRolesChange newGrantedRolesChange) {
        long affectedPrivileges = 0L;
        for (String userName : userNames) {
            affectedPrivileges += applyRolePrivilegesToUser(userName, newGrantedRolesChange);
        }
        return affectedPrivileges;
    }

    private long applyRolePrivilegesToUser(String roleName, GrantedRolesChange newGrantedRolesChange) {
        Role role = roles.get(roleName);

        // Create a new set to avoid modifying in-place the granted roles as this will lead
        // to no difference in previous and new metadata and thus cluster state
        Set<GrantedRole> grantedRoles = new HashSet<>(role.grantedRoles());
        long affectedCount = 0L;
        for (var roleNameToApply : newGrantedRolesChange.roleNames()) {

            if (newGrantedRolesChange.policy() == Policy.GRANT) {
                if (grantedRoles.add(new GrantedRole(roleNameToApply, newGrantedRolesChange.grantor()))) {
                    affectedCount++;
                }
            } else if (newGrantedRolesChange.policy() == Policy.REVOKE) {
                if (grantedRoles.remove(new GrantedRole(roleNameToApply, newGrantedRolesChange.grantor()))) {
                    affectedCount++;
                }
            }
        }
        if (affectedCount > 0) {
            roles.put(role.name(), new Role(role.name(), role.isUser(), Set.of(), grantedRoles, role.password(), role.jwtProperties()));
        }
        return affectedCount;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RolesMetadata that = (RolesMetadata) o;
        return Objects.equals(roles, that.roles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roles);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_6_0;
    }
}
