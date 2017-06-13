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

package io.crate.operation.user;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.user.Privilege;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Singleton
public class TransportPrivilegesAction extends TransportMasterNodeAction<PrivilegesRequest, PrivilegesResponse> {

    private static final String ACTION_NAME = "crate/sql/grant_privileges";

    @Inject
    public TransportPrivilegesAction(Settings settings,
                                     TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PrivilegesRequest::new);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PrivilegesResponse newResponse() {
        return new PrivilegesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PrivilegesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(PrivilegesRequest request, ClusterState state, ActionListener<PrivilegesResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("grant_privileges",
            new ClusterStateUpdateTask() {

                long affectedRows = -1;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData currentMetaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
                    affectedRows = applyPrivileges(
                        mdBuilder,
                        currentMetaData.custom(UsersPrivilegesMetaData.TYPE),
                        request
                    );
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new PrivilegesResponse(true, affectedRows));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            });

    }

    @VisibleForTesting
    static long applyPrivileges(MetaData.Builder mdBuilder,
                                @Nullable UsersPrivilegesMetaData oldMetaData,
                                PrivilegesRequest request) {
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersPrivilegesMetaData newMetaData = UsersPrivilegesMetaData.copyOf(oldMetaData);

        long affectedRows = 0L;
        Collection<Privilege> privileges = request.privileges();

        for (String userName : request.userNames()) {
            Set<Privilege> userPrivileges = newMetaData.getUserPrivileges(userName);
            boolean create = false;
            if (userPrivileges == null) {
                create = true;
                userPrivileges = new HashSet<>();
            }
            for (Privilege privilege : privileges) {
                affectedRows += applyPrivilege(userPrivileges, privilege);
            }
            // no need to create empty privileges for a user
            if (create && userPrivileges.size() > 0) {
                newMetaData.createPrivileges(userName, userPrivileges);
            }
        }

        mdBuilder.putCustom(UsersPrivilegesMetaData.TYPE, newMetaData);
        return affectedRows;
    }

    private static long applyPrivilege(Set<Privilege> privileges, Privilege privilege) {
        if (privileges.contains(privilege)) {
            return 0L;
        }

        switch (privilege.state()) {
            case GRANT:
                privileges.add(privilege);
                return 1L;
            case REVOKE:
                Privilege grantPrivilege = Privilege.privilegeAsGrant(privilege);
                if (privileges.contains(grantPrivilege)) {
                    privileges.remove(grantPrivilege);
                    return 1L;
                }
                return 0L;
            default:
                return 0L;
        }
    }
}
