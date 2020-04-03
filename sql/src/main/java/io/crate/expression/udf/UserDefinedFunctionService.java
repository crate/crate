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

package io.crate.expression.udf;

import com.google.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.FuncResolver;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nullable;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;


@Singleton
public class UserDefinedFunctionService {

    private static final Logger LOGGER = LogManager.getLogger(UserDefinedFunctionService.class);

    private final ClusterService clusterService;
    private final Functions functions;
    private final Map<String, UDFLanguage> languageRegistry = new HashMap<>();

    @Inject
    public UserDefinedFunctionService(ClusterService clusterService, Functions functions) {
        this.clusterService = clusterService;
        this.functions = functions;
    }

    public UDFLanguage getLanguage(String languageName) throws IllegalArgumentException {
        UDFLanguage lang = languageRegistry.get(languageName);
        if (lang == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "'%s' is not a valid UDF language", languageName));
        }
        return lang;
    }

    public void registerLanguage(UDFLanguage language) {
        languageRegistry.put(language.name(), language);
    }

    void registerFunction(final UserDefinedFunctionMetaData metaData,
                          final boolean replace,
                          final ActionListener<AcknowledgedResponse> listener,
                          final TimeValue timeout) {
        clusterService.submitStateUpdateTask("put_udf [" + metaData.name() + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData currentMetaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentMetaData);
                    UserDefinedFunctionsMetaData functions = putFunction(
                        currentMetaData.custom(UserDefinedFunctionsMetaData.TYPE),
                        metaData,
                        replace
                    );
                    mdBuilder.putCustom(UserDefinedFunctionsMetaData.TYPE, functions);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                public TimeValue timeout() {
                    return timeout;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    void dropFunction(final String schema,
                      final String name,
                      final List<DataType> argumentTypes,
                      final boolean ifExists,
                      final ActionListener<AcknowledgedResponse> listener,
                      final TimeValue timeout) {
        clusterService.submitStateUpdateTask("drop_udf [" + schema + "." + name + " - " + argumentTypes + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    UserDefinedFunctionsMetaData functions = removeFunction(
                        metaData.custom(UserDefinedFunctionsMetaData.TYPE),
                        schema,
                        name,
                        argumentTypes,
                        ifExists
                    );
                    mdBuilder.putCustom(UserDefinedFunctionsMetaData.TYPE, functions);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                public TimeValue timeout() {
                    return timeout;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            });
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


    public void updateImplementations(String schema, Stream<UserDefinedFunctionMetaData> userDefinedFunctions) {
        final Map<FunctionName, List<FuncResolver>> implementations = new HashMap<>();
        Iterator<UserDefinedFunctionMetaData> it = userDefinedFunctions.iterator();
        while (it.hasNext()) {
            UserDefinedFunctionMetaData udf = it.next();
            FuncResolver resolver = buildFunctionResolver(udf);
            if (resolver == null) {
                continue;
            }
            var functionName = new FunctionName(udf.schema(), udf.name());
            var resolvers = implementations.computeIfAbsent(
                functionName, k -> new ArrayList<>());
            resolvers.add(resolver);
        }
        functions.registerUdfFunctionImplementationsForSchema(schema, implementations);
    }

    @Nullable
    public FuncResolver buildFunctionResolver(UserDefinedFunctionMetaData udf) {
        var functionName = new FunctionName(udf.schema(), udf.name());
        var signature = Signature.builder()
            .name(functionName)
            .kind(FunctionInfo.Type.SCALAR)
            .argumentTypes(
                Lists2.map(
                    udf.argumentTypes(),
                    DataType::getTypeSignature))
            .returnType(udf.returnType().getTypeSignature())
            .build();

        final Scalar<?, ?> scalar;
        try {
            scalar = getLanguage(udf.language()).createFunctionImplementation(udf, signature);
        } catch (ScriptException | IllegalArgumentException e) {
            LOGGER.warn("Can't create user defined function: " + udf.specificName(), e);
            return null;
        }

        return new FuncResolver(signature, (s, args) -> scalar);
    }
}
