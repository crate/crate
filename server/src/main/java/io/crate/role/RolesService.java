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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.jspecify.annotations.Nullable;

import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class RolesService implements Roles, ClusterStateListener {

    private volatile Map<String, Role> roles = Map.of(Role.CRATE_USER.name(), Role.CRATE_USER);

    public RolesService(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public Collection<Role> roles() {
        return roles.values();
    }

    @Override
    @Nullable
    public Role findRole(String roleName) {
        return roles.get(roleName);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Metadata prevMetadata = event.previousState().metadata();
        Metadata newMetadata = event.state().metadata();

        if (rolesChanged(prevMetadata, newMetadata)) {
            roles = getRoles(newMetadata);
        }
    }

    @SuppressWarnings("deprecation")
    public static boolean rolesChanged(Metadata prevMetadata, Metadata newMetadata) {
        return prevMetadata.custom(UsersMetadata.TYPE) != newMetadata.custom(UsersMetadata.TYPE)
            || prevMetadata.custom(RolesMetadata.TYPE) != newMetadata.custom(RolesMetadata.TYPE)
            || prevMetadata.custom(UsersPrivilegesMetadata.TYPE) != newMetadata.custom(UsersPrivilegesMetadata.TYPE);
    }

    @SuppressWarnings("deprecation")
    public static Map<String, Role> getRoles(Metadata metadata) {
        return getRoles(
            metadata.custom(UsersMetadata.TYPE),
            metadata.custom(RolesMetadata.TYPE),
            metadata.custom(UsersPrivilegesMetadata.TYPE)
        );
    }

    @SuppressWarnings("deprecation")
    static Map<String, Role> getRoles(@Nullable UsersMetadata usersMetadata,
                                      @Nullable RolesMetadata rolesMetadata,
                                      @Nullable UsersPrivilegesMetadata privilegesMetadata) {
        Map<String, Role> roles = new HashMap<>();
        roles.put(Role.CRATE_USER.name(), Role.CRATE_USER);
        if (usersMetadata != null) {
            for (Map.Entry<String, SecureHash> user: usersMetadata.users().entrySet()) {
                String userName = user.getKey();
                Set<Privilege> privileges = Set.of();
                if (privilegesMetadata != null) {
                    var oldPrivileges = privilegesMetadata.getUserPrivileges(userName);
                    if (oldPrivileges != null) {
                        privileges = oldPrivileges;
                    }
                }
                roles.put(userName, new Role(userName, true, privileges, Set.of(), user.getValue(), null, Map.of()));
            }
        } else if (rolesMetadata != null) {
            roles.putAll(rolesMetadata.roles());
        }
        return Collections.unmodifiableMap(roles);
    }
}
