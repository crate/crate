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

import static io.crate.metadata.doc.DocSchemaInfo.NO_BLOB_NOR_DANGLING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.script.ScriptException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.FunctionType;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import io.crate.types.DataType;


@Singleton
public class UserDefinedFunctionService {

    private static final Logger LOGGER = LogManager.getLogger(UserDefinedFunctionService.class);

    private final ClusterService clusterService;
    private final NodeContext nodeCtx;
    private final Map<String, UDFLanguage> languageRegistry = new HashMap<>();
    private final DocTableInfoFactory docTableFactory;

    @Inject
    public UserDefinedFunctionService(ClusterService clusterService,
                                      DocTableInfoFactory docTableFactory,
                                      NodeContext nodeCtx) {
        this.clusterService = clusterService;
        this.docTableFactory = docTableFactory;
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
                    UserDefinedFunctionsMetadata functions = removeFunction(
                        metadata.custom(UserDefinedFunctionsMetadata.TYPE),
                        schema,
                        name,
                        argumentTypes,
                        ifExists
                    );
                    mdBuilder.putCustom(UserDefinedFunctionsMetadata.TYPE, functions);
                    validateFunctionIsNotInUseByGeneratedColumn(
                        schema,
                        schema + "." + UserDefinedFunctionMetadata.specificName(name, argumentTypes),
                        functions,
                        currentState
                    );
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


    public void updateImplementations(String schema, Stream<UserDefinedFunctionMetadata> userDefinedFunctions) {
        updateImplementations(schema, userDefinedFunctions, nodeCtx);
    }

    public void updateImplementations(String schema,
                                      Stream<UserDefinedFunctionMetadata> userDefinedFunctions,
                                      NodeContext nodeCtx) {
        final Map<FunctionName, List<FunctionProvider>> implementations = new HashMap<>();
        Iterator<UserDefinedFunctionMetadata> it = userDefinedFunctions.iterator();
        while (it.hasNext()) {
            UserDefinedFunctionMetadata udf = it.next();
            FunctionProvider resolver = buildFunctionResolver(udf);
            if (resolver == null) {
                continue;
            }
            var functionName = new FunctionName(udf.schema(), udf.name());
            var resolvers = implementations.computeIfAbsent(
                functionName, k -> new ArrayList<>());
            resolvers.add(resolver);
        }
        nodeCtx.functions().registerUdfFunctionImplementationsForSchema(schema, implementations);
    }

    @Nullable
    public FunctionProvider buildFunctionResolver(UserDefinedFunctionMetadata udf) {
        var functionName = new FunctionName(udf.schema(), udf.name());
        var signature = Signature.builder()
            .name(functionName)
            .kind(FunctionType.SCALAR)
            .argumentTypes(
                Lists.map(
                    udf.argumentTypes(),
                    DataType::getTypeSignature))
            .returnType(udf.returnType().getTypeSignature())
            .build();

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

    void validateFunctionIsNotInUseByGeneratedColumn(String schema,
                                                             String functionName,
                                                             UserDefinedFunctionsMetadata functionsMetadata,
                                                             ClusterState currentState) {
        // The iteration of schemas/tables must happen on the node context WITHOUT the UDF already removed.
        // Otherwise the lazy table factories will already fail while evaluating generated functionsMetadata.
        // To avoid that, a copy of the node context with the removed UDF function is used on concrete expression evaluation.
        var nodeCtxWithRemovedFunction = new NodeContext(nodeCtx.functions().copyOf(), nodeCtx.roles());
        updateImplementations(schema, functionsMetadata.functionsMetadata().stream(), nodeCtxWithRemovedFunction);

        var metadata = currentState.metadata();
        var indices = Stream.of(metadata.getConcreteAllIndices()).filter(NO_BLOB_NOR_DANGLING)
            .map(IndexParts::new)
            .filter(indexParts -> !indexParts.isPartitioned())
            .collect(Collectors.toList());
        var templates = metadata.templates().keysIt();
        while (templates.hasNext()) {
            var indexParts = new IndexParts(templates.next());
            if (indexParts.isPartitioned()) {
                indices.add(indexParts);
            }
        }

        for (var indexParts : indices) {
            var tableInfo = docTableFactory.create(indexParts.toRelationName(), currentState.metadata());
            var functionParameters = getReferencedRefs(tableInfo.generatedColumns());
            TableReferenceResolver tableReferenceResolver = new TableReferenceResolver(
                functionParameters,
                tableInfo.ident()
            );
            CoordinatorTxnCtx coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
            ExpressionAnalyzer exprAnalyzer = new ExpressionAnalyzer(
                coordinatorTxnCtx, nodeCtxWithRemovedFunction, ParamTypeHints.EMPTY, tableReferenceResolver, null);
            for (var ref : tableInfo.columns()) {
                if (ref instanceof GeneratedReference genRef) {
                    Expression expression = SqlParser.createExpression(genRef.formattedGeneratedExpression());
                    try {
                        exprAnalyzer.convert(expression, new ExpressionAnalysisContext(coordinatorTxnCtx.sessionSettings()));
                    } catch (UnsupportedFunctionException e) {
                        throw new IllegalArgumentException(
                            "Cannot drop function '" + functionName + "', it is still in use by '" +
                                tableInfo + "." + genRef + "'"
                        );
                    }
                }
            }
        }
    }

    private static Map<ColumnIdent, Reference> getReferencedRefs(List<GeneratedReference> generatedReferences) {
        Map<ColumnIdent, Reference> referencedReferences = new HashMap<>();
        for (var generatedRef : generatedReferences) {
            for (var referencedRef : generatedRef.referencedReferences()) {
                referencedReferences.put(referencedRef.column(), referencedRef);
            }
        }
        return referencedReferences;
    }
}
