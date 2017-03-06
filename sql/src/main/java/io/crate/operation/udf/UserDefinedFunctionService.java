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
import io.crate.types.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.operation.udf.UserDefinedFunctionsMetaData.PROTO;

@Singleton
public class UserDefinedFunctionService extends AbstractLifecycleComponent<UserDefinedFunctionService> implements ClusterStateListener {

    static {
        // register non plugin custom metadata
        MetaData.registerPrototype(UserDefinedFunctionsMetaData.TYPE, PROTO);
    }

    private final ClusterService clusterService;
    private final Functions functions;

    @Inject
    public UserDefinedFunctionService(Settings settings, ClusterService clusterService, Functions functions) {
        super(settings);
        this.clusterService = clusterService;
        this.functions = functions;
    }

    @Override
    protected void doStart() {
        clusterService.add(this);
    }

    @Override
    protected void doStop() {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() {
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
        }

        // create a new instance of the metadata, to guarantee the cluster changed action.
        UserDefinedFunctionsMetaData newMetaData = UserDefinedFunctionsMetaData.newInstance(oldMetaData);
        if (oldMetaData.contains(functionMetaData)) {
            if (!replace) {
                throw new UserDefinedFunctionAlreadyExistsException(functionMetaData);
            }
            newMetaData.replace(functionMetaData);
        } else {
            newMetaData.add(functionMetaData);
        }

        assert !newMetaData.equals(oldMetaData) : "must not be equal to guarantee the cluster change action";
        return newMetaData;
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
        functions.registerSchemaFunctionResolvers(
            functions.generateFunctionResolvers(createFunctionImplementations(newMetaData, logger))
        );
    }

    static Map<FunctionIdent, FunctionImplementation> createFunctionImplementations(UserDefinedFunctionsMetaData metaData,
                                                                                    ESLogger logger) {
        Map<FunctionIdent, FunctionImplementation> udfFunctions = new HashMap<>();
        for (UserDefinedFunctionMetaData function : metaData.functionsMetaData()) {
            try {
                FunctionImplementation impl = UserDefinedFunctionFactory.of(function);
                udfFunctions.put(impl.info().ident(), impl);
            } catch (javax.script.ScriptException e) {
                logger.warn(
                    String.format(Locale.ENGLISH, "Can't create user defined function '%s(%s)'",
                        function.name(),
                        function.argumentTypes().stream().map(DataType::getName).collect(Collectors.joining(", "))
                    ), e);
            }
        }
        return udfFunctions;
    }

    static class RegisterUserDefinedFunctionRequest extends ClusterStateUpdateRequest<RegisterUserDefinedFunctionRequest> {

        private final UserDefinedFunctionMetaData metaData;
        private final String cause;
        private final String name;
        private final boolean replace;

        RegisterUserDefinedFunctionRequest(String cause, String name, UserDefinedFunctionMetaData metaData, boolean replace) {
            this.cause = cause;
            this.name = name;
            this.metaData = metaData;
            this.replace = replace;
        }
    }
}
