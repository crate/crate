/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.udf;

import com.google.common.annotations.VisibleForTesting;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class UserDefinedFunctionService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    @Inject
    public UserDefinedFunctionService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        clusterService.add(this);
    }

    /**
     * This method can only be called on master node
     * registers new function in the cluster
     * @param request function to register
     */
    void registerFunction(final RegisterUserDefinedFunctionRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (!clusterService.localNode().isMasterNode()) {
            return;
        }
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                UserDefinedFunctionsMetaData functions = putFunction(metaData.custom(UserDefinedFunctionsMetaData.TYPE),
                    request.metaData, request.replace);
                mdBuilder.putCustom(UserDefinedFunctionsMetaData.TYPE, functions);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

        });
    }

    @VisibleForTesting
    static UserDefinedFunctionsMetaData putFunction(@Nullable UserDefinedFunctionsMetaData functions, UserDefinedFunctionMetaData function,
                                                    boolean replace) {
        if (functions == null) {
            return UserDefinedFunctionsMetaData.of(function);
        } else {
            if (!replace && functions.contains(function)) {
                throw new UserDefinedFunctionAlreadyExistsException(function);
            }
            functions.put(function);
            return functions;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        UserDefinedFunctionsMetaData oldMetaData = event.previousState().getMetaData().custom(UserDefinedFunctionsMetaData.TYPE);
        UserDefinedFunctionsMetaData newMetaData = event.state().getMetaData().custom(UserDefinedFunctionsMetaData.TYPE);

        // Check if user defined functions got changed
        if ((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equals(newMetaData))) {
            return;
        }

        logger.trace("processing new index repositories for state version [{}]", event.state().version());

        // TODO: register new udfs
        // TODO: update changed udfs
        // TODO: remove udfs which are no longer here
    }

    static class RegisterUserDefinedFunctionRequest extends ClusterStateUpdateRequest<RegisterUserDefinedFunctionRequest> {

        UserDefinedFunctionMetaData metaData;
        final String cause;
        final String name;
        final boolean replace;

        RegisterUserDefinedFunctionRequest(String cause, String name, UserDefinedFunctionMetaData metaData, boolean replace) {
            this.cause = cause;
            this.name = name;
            this.metaData = metaData;
            this.replace = replace;
        }
    }
}
