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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

@Singleton
public class TransportPrivilegesAction extends TransportMasterNodeAction<PrivilegesRequest, PrivilegesResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<PrivilegesResponse> {
        private static final String NAME = "internal:crate:sql/privileges/grant";

        private Action() {
            super(NAME);
        }
    }

    private final Roles roles;

    @Inject
    public TransportPrivilegesAction(TransportService transportService,
                                     ClusterService clusterService,
                                     Roles roles,
                                     ThreadPool threadPool) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            PrivilegesRequest::new
        );
        this.roles = roles;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PrivilegesResponse read(StreamInput in) throws IOException {
        return new PrivilegesResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PrivilegesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(PrivilegesRequest request,
                                   ClusterState state,
                                   ActionListener<PrivilegesResponse> listener) throws Exception {

        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_6_0) == false) {
            throw new IllegalStateException("Cannot grant/deny/revoke privileges until all nodes are upgraded to 5.6");
        }

        clusterService.submitStateUpdateTask("grant_privileges",
                new AckedClusterStateUpdateTask<>(Priority.IMMEDIATE, request, listener) {

                    ApplyPrivsResult result = null;

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Metadata currentMetadata = currentState.metadata();
                        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);

                        result = applyPrivileges(roles, mdBuilder, request);
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }

                    @Override
                    protected PrivilegesResponse newResponse(boolean acknowledged) {
                        return new PrivilegesResponse(acknowledged, result.affectedRows, result.unknownRoleNames);
                    }
                });

    }

    @VisibleForTesting
    static List<String> validateRoleNames(RolesMetadata rolesMetadata, Collection<String> roleNames) {
        if (rolesMetadata == null) {
            return new ArrayList<>(roleNames);
        }
        List<String> unknownRoleNames = null;
        for (String roleName : roleNames) {
            //noinspection PointlessBooleanExpression
            if (rolesMetadata.roleNames().contains(roleName) == false) {
                if (unknownRoleNames == null) {
                    unknownRoleNames = new ArrayList<>();
                }
                unknownRoleNames.add(roleName);
            }
        }
        if (unknownRoleNames == null) {
            return Collections.emptyList();
        }
        return unknownRoleNames;
    }

    @VisibleForTesting
    static ApplyPrivsResult applyPrivileges(Roles roles, Metadata.Builder mdBuilder, PrivilegesRequest request) {
        var oldPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        var oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        var oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);

        RolesMetadata newMetadata = RolesMetadata.of(
            mdBuilder, oldUsersMetadata, oldPrivilegesMetadata, oldRolesMetadata);

        List<String> unknownRoleNames = validateRoleNames(newMetadata, request.roleNames());
        long affectedRows = -1;
        if (unknownRoleNames.isEmpty()) {
            if (request.privileges().isEmpty() == false) {
                affectedRows = PrivilegesModifier.applyPrivileges(newMetadata, request.roleNames(), request.privileges());
            } else {
                unknownRoleNames = validateRoleNames(newMetadata, request.rolePrivilege().roleNames());
                if (unknownRoleNames.isEmpty()) {
                    validateIsNotUser(roles, request.rolePrivilege());
                    detectCyclesInRolesHierarchy(roles, request);
                    affectedRows = newMetadata.applyRolePrivileges(request.roleNames(), request.rolePrivilege());
                }
            }
        }

        if (newMetadata.equals(oldRolesMetadata) == false) {
            mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);
        }

        return new ApplyPrivsResult(affectedRows, unknownRoleNames);
    }

    private static void validateIsNotUser(Roles roles, GrantedRolesChange grantedRolesChange) {
        for (String roleNameToApply : grantedRolesChange.roleNames()) {
            if (roles.findRole(roleNameToApply).isUser()) {
                throw new IllegalArgumentException("Cannot " + grantedRolesChange.policy().name() + " a USER to a ROLE");
            }
        }
    }

    @VisibleForTesting
    static void detectCyclesInRolesHierarchy(Roles roles, PrivilegesRequest request) {
        if (request.rolePrivilege().policy() == Policy.GRANT) {
            for (var roleNameToGrant : request.rolePrivilege().roleNames()) {
                Set<String> parentsOfRoleToGrant = roles.findAllParents(roleNameToGrant);
                for (var grantee : request.roleNames()) {
                    if (parentsOfRoleToGrant.contains(grantee)) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Cannot grant role %s to %s, %s is a parent role of %s and a cycle will " +
                                "be created", roleNameToGrant, grantee, grantee, roleNameToGrant));
                    }
                }
            }
        }
    }

    record ApplyPrivsResult(long affectedRows, List<String> unknownRoleNames) {}
}
