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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class TransportAlterRoleAction extends TransportMasterNodeAction<AlterRoleRequest, WriteRoleResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<WriteRoleResponse> {
        private static String NAME = "internal:crate:sql/user/alter";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportAlterRoleAction(TransportService transportService,
                                    ClusterService clusterService,
                                    ThreadPool threadPool) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            AlterRoleRequest::new
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WriteRoleResponse read(StreamInput in) throws IOException {
        return new WriteRoleResponse(in);
    }

    @Override
    protected void masterOperation(AlterRoleRequest request,
                                   ClusterState state,
                                   ActionListener<WriteRoleResponse> listener) {
        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_6_0) == false) {
            throw new IllegalStateException("Cannot alter users/roles until all nodes are upgraded to 5.6");
        }

        clusterService.submitStateUpdateTask("alter_role [" + request.roleName() + "]",
                new AckedClusterStateUpdateTask<>(Priority.IMMEDIATE, request, listener) {

                    private boolean roleExists = true;

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Metadata currentMetadata = currentState.metadata();
                        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                        roleExists = alterRole(
                                mdBuilder,
                                request.roleName(),
                                request.secureHash(),
                                request.jwtProperties(),
                                request.resetPassword(),
                                request.resetJwtProperties(),
                                request.sessionSettingsChange()
                        );
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }

                    @Override
                    protected WriteRoleResponse newResponse(boolean acknowledged) {
                        return new WriteRoleResponse(acknowledged, roleExists);
                    }
                });
    }

    @VisibleForTesting
    static boolean alterRole(Metadata.Builder mdBuilder,
                             String roleName,
                             @Nullable SecureHash secureHash,
                             @Nullable JwtProperties jwtProperties,
                             boolean resetPassword,
                             boolean resetJwtProperties,
                             Map<Boolean, Map<String, Object>> sessionSettingsChange) {
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        UsersMetadata oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        if (oldUsersMetadata == null && oldRolesMetadata == null) {
            return false;
        }

        UsersPrivilegesMetadata oldUserPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        boolean exists = false;
        var role = newMetadata.roles().get(roleName);
        if (role != null) {
            if (role.isUser() == false) {
                if (secureHash != null || resetPassword) {
                    throw new UnsupportedFeatureException("Setting a password to a ROLE is not allowed");
                }
                if (jwtProperties != null || resetJwtProperties) {
                    throw new UnsupportedFeatureException("Setting JWT properties to a ROLE is not allowed");
                }
                if (sessionSettingsChange.isEmpty() == false) {
                    throw new UnsupportedFeatureException(
                        "Setting or resetting session settings to a ROLE is not allowed");
                }
            }

            var newSecureHash = secureHash != null ? secureHash : (resetPassword ? null : role.password());
            var newJwtProperties = jwtProperties != null ? jwtProperties : (resetJwtProperties ? null : role.jwtProperties());

            if (jwtProperties != null && newMetadata.contains(newJwtProperties)) {
                // If we have a clash it could be that we tried to keep jwt and update another property.
                // We throw only if we actually tried to update JWT properties.
                throw new RoleAlreadyExistsException(
                    "Another role with the same combination of iss/username jwt properties already exists"
                );
            }

            newMetadata.roles().put(roleName, role.with(
                    newSecureHash,
                    newJwtProperties,
                    getUpdatedSessionSettings(role.sessionSettings(), sessionSettingsChange)));
            exists = true;
        }
        if (newMetadata.equals(oldRolesMetadata)) {
            return exists;
        }

        assert !newMetadata.equals(oldRolesMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);

        return exists;
    }

    @Override
    protected ClusterBlockException checkBlock(AlterRoleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static Map<String, Object> getUpdatedSessionSettings(
        Map<String, Object> oldSessionSettings,
        Map<Boolean, Map<String, Object>> sessionSettingsChange) {

        Map<String, Object> updatedSessionSettings = new HashMap<>();
        for (var change : sessionSettingsChange.entrySet()) {
            boolean isReset = change.getKey();
            Map<String, Object> settingsToChange = change.getValue();
            if (isReset) { // reset
                if (settingsToChange.isEmpty()) { // reset all
                    return Map.of();
                } else { // reset some settings
                    for (var oldSetting : oldSessionSettings.entrySet()) {
                        if (settingsToChange.containsKey(oldSetting.getKey()) == false) {
                            updatedSessionSettings.put(oldSetting.getKey(), oldSetting.getValue());
                        }
                    }
                }
            } else { // add and overwrite existing settings
                updatedSessionSettings.putAll(oldSessionSettings);
                updatedSessionSettings.putAll(settingsToChange);
            }
        }
        return Collections.unmodifiableMap(updatedSessionSettings);
    }
}
