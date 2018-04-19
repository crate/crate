/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */


package io.crate.metadata;

import io.crate.analyze.user.Privilege;
import io.crate.settings.SharedSettings;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.auth.user.User.CRATE_USER;

/**
 * Migration code for existing users, adds all available privilege to each existing user.
 *
 * This class ensures that users created before the privileges support was available will still be able to issue
 * statements without interaction by an administrator. This is done by adding all available privileges to
 * these users. The code treats users with no privileges at all (privileges will be NULL) as old users required
 * to migrate and users with empty or existing privileges as (new) users were no migrations is needed nor wanted.
 */
public class PrivilegesMetaDataUpgrader implements CustomMetaDataUpgrader {

    @Override
    public Map<String, MetaData.Custom> apply(Settings settings, Map<String, MetaData.Custom> customMetaData) {
        if (!SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings)) {
            return customMetaData;
        }

        UsersMetaData usersMetaData = (UsersMetaData) customMetaData.get(UsersMetaData.TYPE);
        if (usersMetaData != null) {
            List<String> users = usersMetaData.userNames();
            if (users.size() == 0) {
                return customMetaData;
            }

            UsersPrivilegesMetaData privilegesMetaData =
                (UsersPrivilegesMetaData) customMetaData.get(UsersPrivilegesMetaData.TYPE);
            if (privilegesMetaData == null) {
                privilegesMetaData = new UsersPrivilegesMetaData();
                customMetaData.put(UsersPrivilegesMetaData.TYPE, privilegesMetaData);
            }
            for (String userName : usersMetaData.userNames()) {
                Set<Privilege> userPrivileges = privilegesMetaData.getUserPrivileges(userName);
                if (userPrivileges == null) {
                    userPrivileges = new HashSet<>();
                    privilegesMetaData.createPrivileges(userName, userPrivileges);

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
        }

        return customMetaData;
    }
}
