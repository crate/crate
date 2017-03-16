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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class UserDefinedFunctionService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;
    private final Functions functions;

    @Inject
    public UserDefinedFunctionService(Settings settings, ClusterService clusterService, Functions functions) {
        super(settings);
        this.clusterService = clusterService;
        this.functions = functions;
        clusterService.add(this);
    }

    /**
     * This method can only be called on master node
     * registers new function in the cluster
     *
     * @param request function to register
     */
    void registerFunction(final RegisterUserDefinedFunctionRequest request,
                          final ActionListener<ClusterStateUpdateResponse> listener) {
        if (!clusterService.localNode().isMasterNode()) {
            return;
        }
        clusterService.submitStateUpdateTask(request.cause,
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(metaData);

                    UserDefinedFunctionsMetaData functions = putFunction(
                        metaData.custom(UserDefinedFunctionsMetaData.TYPE),
                        request.metaData,
                        request.replace
                    );
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
    static UserDefinedFunctionsMetaData putFunction(@Nullable UserDefinedFunctionsMetaData oldMetaData,
                                                    UserDefinedFunctionMetaData functionMetaData,
                                                    boolean replace) {
        if (oldMetaData == null) {
            return UserDefinedFunctionsMetaData.of(functionMetaData);
        } else {
            if (!replace && oldMetaData.contains(functionMetaData)) {
                throw new UserDefinedFunctionAlreadyExistsException(functionMetaData);
            }
            // create a new instance of the metadata, to guarantee the cluster changed action.
            UserDefinedFunctionsMetaData newMetaData = UserDefinedFunctionsMetaData.newInstance(oldMetaData);
            newMetaData.put(functionMetaData);
            return newMetaData;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        UserDefinedFunctionsMetaData oldMetaData = event.previousState().getMetaData()
            .custom(UserDefinedFunctionsMetaData.TYPE);
        UserDefinedFunctionsMetaData newMetaData = event.state().getMetaData()
            .custom(UserDefinedFunctionsMetaData.TYPE);

        // Check if user defined functions got changed
        if ((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equals(newMetaData))) {
            return;
        }

        Map<String, Map<FunctionIdent, FunctionImplementation>> schemaFunctions = newMetaData.functionsMetaData().stream()
            .map(UserDefinedFunctionFactory::of)
            .collect(groupingBy(f -> f.info().ident().schema(), toMap(f -> f.info().ident(), f -> f)));

        for (Map.Entry<String, Map<FunctionIdent, FunctionImplementation>> entry : schemaFunctions.entrySet()) {
            functions.registerSchemaFunctionResolvers(
                entry.getKey(),
                functions.generateFunctionResolvers(entry.getValue())
            );
        }
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
