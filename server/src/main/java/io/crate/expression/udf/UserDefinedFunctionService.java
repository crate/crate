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

package io.crate.expression.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import javax.script.ScriptException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;


public class UserDefinedFunctionService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(UserDefinedFunctionService.class);

    private final ClusterService clusterService;
    private final NodeContext nodeCtx;
    private final Map<String, UDFLanguage> languageRegistry = new HashMap<>();

    public UserDefinedFunctionService(ClusterService clusterService, NodeContext nodeCtx) {
        this.clusterService = clusterService;
        this.nodeCtx = nodeCtx;
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

    void registerFunction(final UserDefinedFunctionMetadata metadata,
                          final boolean replace,
                          final ActionListener<AcknowledgedResponse> listener,
                          final TimeValue timeout) {
        clusterService.submitStateUpdateTask("put_udf [" + metadata.name() + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata currentMetadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                    UserDefinedFunctionsMetadata functions = putFunction(
                        currentMetadata.custom(UserDefinedFunctionsMetadata.TYPE),
                        metadata,
                        replace
                    );
                    mdBuilder.putCustom(UserDefinedFunctionsMetadata.TYPE, functions);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
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
                      final List<DataType<?>> argumentTypes,
                      final boolean ifExists,
                      final ActionListener<AcknowledgedResponse> listener,
                      final TimeValue timeout) {
        clusterService.submitStateUpdateTask("drop_udf [" + schema + "." + name + " - " + argumentTypes + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    ensureFunctionIsUnused(schema, name, argumentTypes);
                    UserDefinedFunctionsMetadata functions = removeFunction(
                        metadata.custom(UserDefinedFunctionsMetadata.TYPE),
                        schema,
                        name,
                        argumentTypes,
                        ifExists
                    );
                    mdBuilder.putCustom(UserDefinedFunctionsMetadata.TYPE, functions);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
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
    UserDefinedFunctionsMetadata putFunction(@Nullable UserDefinedFunctionsMetadata oldMetadata,
                                             UserDefinedFunctionMetadata functionMetadata,
                                             boolean replace) {
        if (oldMetadata == null) {
            return UserDefinedFunctionsMetadata.of(functionMetadata);
        }

        // create a new instance of the metadata, to guarantee the cluster changed action.
        UserDefinedFunctionsMetadata newMetadata = UserDefinedFunctionsMetadata.newInstance(oldMetadata);
        if (oldMetadata.contains(functionMetadata.schema(), functionMetadata.name(), functionMetadata.argumentTypes())) {
            if (!replace) {
                throw new UserDefinedFunctionAlreadyExistsException(functionMetadata);
            }
            newMetadata.replace(functionMetadata);
        } else {
            newMetadata.add(functionMetadata);
        }

        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
        return newMetadata;
    }

    @VisibleForTesting
    UserDefinedFunctionsMetadata removeFunction(@Nullable UserDefinedFunctionsMetadata functions,
                                                String schema,
                                                String name,
                                                List<DataType<?>> argumentDataTypes,
                                                boolean ifExists) {
        if (!ifExists && (functions == null || !functions.contains(schema, name, argumentDataTypes))) {
            throw new UserDefinedFunctionUnknownException(schema, name, argumentDataTypes);
        } else if (functions == null) {
            return UserDefinedFunctionsMetadata.of();
        } else {
            // create a new instance of the metadata, to guarantee the cluster changed action.
            UserDefinedFunctionsMetadata newMetadata = UserDefinedFunctionsMetadata.newInstance(functions);
            newMetadata.remove(schema, name, argumentDataTypes);
            return newMetadata;
        }
    }

    public void updateImplementations(List<UserDefinedFunctionMetadata> userDefinedFunctions) {
        final Map<FunctionName, List<FunctionProvider>> implementations = new HashMap<>();
        for (var functionMetadata : userDefinedFunctions) {
            FunctionProvider provider = buildFunctionResolver(functionMetadata);
            if (provider == null) {
                continue;
            }
            FunctionName name = provider.signature().getName();
            var providers = implementations.computeIfAbsent(name, k -> new ArrayList<>());
            providers.add(provider);
        }
        nodeCtx.functions().setUDFs(implementations);
    }

    @Nullable
    public FunctionProvider buildFunctionResolver(UserDefinedFunctionMetadata udf) {
        var functionName = new FunctionName(udf.schema(), udf.name());
        List<TypeSignature> typeSignatures = new ArrayList<>(Lists.map(udf.argumentTypes(), DataType::getTypeSignature));
        typeSignatures.add(udf.returnType().getTypeSignature());
        var signature = Signature.scalar(functionName, Scalar.Feature.CONDITIONAL, typeSignatures.toArray(TypeSignature[]::new))
            .withFeature(Scalar.Feature.DETERMINISTIC);

        final Scalar<?, ?> scalar;
        try {
            UDFLanguage language = getLanguage(udf.language());
            scalar = language.createFunctionImplementation(
                udf,
                signature,
                new BoundSignature(udf.argumentTypes(), udf.returnType())
            );
        } catch (ScriptException | IllegalArgumentException e) {
            LOGGER.warn("Can't create user defined function: " + udf.specificName(), e);
            return null;
        }

        return new FunctionProvider(signature, (s, args) -> scalar);
    }

    /**
     * Verifies that the function is not used in:
     *
     * <ul>
     * <li>A generated column expression</li>
     * </ul>
     **/
    void ensureFunctionIsUnused(String schema, String functionName, List<DataType<?>> argTypes) {
        Schemas schemas = nodeCtx.schemas();
        FunctionName name = new FunctionName(schema, functionName);
        Predicate<Symbol> isFunction = s -> s instanceof Function fn
            && fn.signature().getName().equals(name)
            && fn.signature().getArgumentDataTypes().equals(argTypes);
        for (var schemaInfo : schemas) {
            for (var tableInfo : schemaInfo.getTables()) {
                if (!(tableInfo instanceof DocTableInfo docTable)) {
                    continue;
                }
                List<GeneratedReference> generatedColumns = docTable.generatedColumns();
                for (var genColumn : generatedColumns) {
                    if (SymbolVisitors.any(isFunction, genColumn.generatedExpression())) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "Cannot drop function '%s'. It is in use by column '%s' of table '%s'",
                            name.displayName(),
                            genColumn.column(),
                            docTable.ident()
                        ));
                    }
                }
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.changedCustomMetadataSet().contains(UserDefinedFunctionsMetadata.TYPE)) {
            Metadata newMetadata = event.state().metadata();
            UserDefinedFunctionsMetadata udfMetadata = newMetadata.custom(UserDefinedFunctionsMetadata.TYPE);
            updateImplementations(udfMetadata == null ? List.of() : udfMetadata.functionsMetadata());
        }
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() throws IOException {
    }
}
