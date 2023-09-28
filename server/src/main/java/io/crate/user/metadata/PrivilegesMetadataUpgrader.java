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


package io.crate.user.metadata;

import static io.crate.user.User.CRATE_USER;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;

import io.crate.user.Privilege;
import io.crate.metadata.CustomMetadataUpgrader;

/**
 * Migration code for existing users, adds all available privilege to each existing user.
 *
 * This class ensures that users created before the privileges support was available will still be able to issue
 * statements without interaction by an administrator. This is done by adding all available privileges to
 * these users. The code treats users with no privileges at all (privileges will be NULL) as old users required
 * to migrate and users with empty or existing privileges as (new) users were no migrations is needed nor wanted.
 */
public class PrivilegesMetadataUpgrader implements CustomMetadataUpgrader {

    @Override
    public Map<String, Metadata.Custom> apply(Settings settings, Map<String, Metadata.Custom> customMetadata) {
        UsersMetadata usersMetadata = (UsersMetadata) customMetadata.get(UsersMetadata.TYPE);
        if (usersMetadata == null) {
            return customMetadata;
        }
        List<String> users = usersMetadata.userNames();
        if (users.isEmpty()) {
            return customMetadata;
        }
        UsersPrivilegesMetadata privilegesMetadata =
            (UsersPrivilegesMetadata) customMetadata.get(UsersPrivilegesMetadata.TYPE);
        if (privilegesMetadata == null) {
            privilegesMetadata = new UsersPrivilegesMetadata();
            customMetadata.put(UsersPrivilegesMetadata.TYPE, privilegesMetadata);
        }
        for (String userName : usersMetadata.userNames()) {
            Set<Privilege> userPrivileges = privilegesMetadata.getUserPrivileges(userName);
            if (userPrivileges == null) {
                userPrivileges = new HashSet<>();
                privilegesMetadata.createPrivileges(userName, userPrivileges);

                // add GRANT privileges for all available types on the CLUSTER class
                for (Privilege.Type privilegeType : Privilege.Type.values()) {
                    userPrivileges.add(
                        new Privilege(
                            Privilege.State.GRANT,
                            privilegeType,
                            Privilege.Clazz.CLUSTER,
                            null,
                            CRATE_USER.name()));
                }
            }
        }
        return customMetadata;
    }
}
