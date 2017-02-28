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
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.settings.CrateSetting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.crate.operation.udf.UserDefinedFunctionsMetaData.PROTO;

@Singleton
public class UserDefinedFunctionService {

    static {
        // register non plugin custom metadata
        MetaData.registerPrototype(UserDefinedFunctionsMetaData.TYPE, PROTO);
    }

    public static final CrateSetting<Boolean> UDF_SETTING = CrateSetting.of(Setting.boolSetting(
        "udf.enabled", false,
        Setting.Property.NodeScope), DataTypes.BOOLEAN);

    private final ClusterService clusterService;
    private Map<String, UDFLanguage> languageRegistry = new HashMap<>();


    public UDFLanguage getLanguage(String languageName) throws IllegalArgumentException {
        UDFLanguage lang = languageRegistry.get(languageName);
        if (lang == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Can't find Language '%s'", languageName));
        }
        return lang;
    }

    public void registerLanguage(UDFLanguage language) {
        languageRegistry.put(language.name(), language);
    }

    @Inject
    public UserDefinedFunctionService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * This method can only be called on master node
     * registers new function in the cluster
     *
     * @param request function to register
     */
    void registerFunction(final RegisterUserDefinedFunctionRequest request,
                          final ActionListener<ClusterStateUpdateResponse> listener) {
        if (!isMaterNode()) {
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

    void dropFunction(final DropUserDefinedFunctionRequest request,
                      final ActionListener<ClusterStateUpdateResponse> listener) {
        if (!isMaterNode()) {
            return;
        }

        clusterService.submitStateUpdateTask(request.cause,
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    UserDefinedFunctionsMetaData functions = removeFunction(
                        metaData.custom(UserDefinedFunctionsMetaData.TYPE),
                        request.schema,
                        request.name,
                        request.argumentTypes,
                        request.ifExists
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

    private boolean isMaterNode() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        return nodes.getMasterNodeId().equals(nodes.getLocalNodeId());
    }

    @VisibleForTesting
    UserDefinedFunctionsMetaData putFunction(@Nullable UserDefinedFunctionsMetaData oldMetaData,
                                             UserDefinedFunctionMetaData functionMetaData,
                                             boolean replace) {
        if (oldMetaData == null) {
            return UserDefinedFunctionsMetaData.of(functionMetaData);
        }

        // create a new instance of the metadata, to guarantee the cluster changed action.
        UserDefinedFunctionsMetaData newMetaData = UserDefinedFunctionsMetaData.newInstance(oldMetaData);
        if (oldMetaData.contains(functionMetaData.schema(), functionMetaData.name(), functionMetaData.argumentTypes())) {
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

    @VisibleForTesting
    UserDefinedFunctionsMetaData removeFunction(@Nullable UserDefinedFunctionsMetaData functions,
                                                String schema,
                                                String name,
                                                List<DataType> argumentDataTypes,
                                                boolean ifExists) {
        if (!ifExists && (functions == null || !functions.contains(schema, name, argumentDataTypes))) {
            throw new UserDefinedFunctionUnknownException(schema, name, argumentDataTypes);
        } else if (functions == null) {
            return UserDefinedFunctionsMetaData.of();
        } else {
            // create a new instance of the metadata, to guarantee the cluster changed action.
            UserDefinedFunctionsMetaData newMetaData = UserDefinedFunctionsMetaData.newInstance(functions);
            newMetaData.remove(schema, name, argumentDataTypes);
            return newMetaData;
        }
    }

    static class RegisterUserDefinedFunctionRequest extends ClusterStateUpdateRequest<RegisterUserDefinedFunctionRequest> {

        private final UserDefinedFunctionMetaData metaData;
        private final String cause;
        private final boolean replace;

        RegisterUserDefinedFunctionRequest(String cause, UserDefinedFunctionMetaData metaData, boolean replace) {
            this.cause = cause;
            this.metaData = metaData;
            this.replace = replace;
        }
    }

    static class DropUserDefinedFunctionRequest extends ClusterStateUpdateRequest<DropUserDefinedFunctionRequest> {

        final String schema;
        final String cause;
        final String name;
        final List<DataType> argumentTypes;
        final boolean ifExists;

        DropUserDefinedFunctionRequest(String cause, String schema, String name, List<DataType> argumentTypes, boolean ifExists) {
            this.schema = schema;
            this.cause = cause;
            this.name = name;
            this.argumentTypes = argumentTypes;
            this.ifExists = ifExists;
        }
    }
}
