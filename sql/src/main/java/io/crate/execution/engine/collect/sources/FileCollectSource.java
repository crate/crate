/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.collect.sources;

import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import io.crate.execution.engine.collect.files.LineCollectorExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Singleton
public class FileCollectSource implements CollectSource {

    private final ClusterService clusterService;
    private final Map<String, FileInputFactory> fileInputFactoryMap;
    private final InputFactory inputFactory;
    private final Functions functions;

    @Inject
    public FileCollectSource(Functions functions, ClusterService clusterService, Map<String, FileInputFactory> fileInputFactoryMap) {
        this.fileInputFactoryMap = fileInputFactoryMap;
        this.functions = functions;
        this.inputFactory = new InputFactory(functions);
        this.clusterService = clusterService;
    }

    @Override
    public BatchIterator<Row> getIterator(TransactionContext txnCtx,
                                          CollectPhase collectPhase,
                                          CollectTask collectTask,
                                          boolean supportMoveToStart) {
        FileUriCollectPhase fileUriCollectPhase = (FileUriCollectPhase) collectPhase;
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(txnCtx, FileLineReferenceResolver::getImplementation);
        ctx.add(collectPhase.toCollect());

        List<String> fileUris = targetUriToStringList(txnCtx, functions, fileUriCollectPhase.targetUri());
        return FileReadingIterator.newInstance(
            fileUris,
            ctx.topLevelInputs(),
            ctx.expressions(),
            fileUriCollectPhase.compression(),
            fileInputFactoryMap,
            fileUriCollectPhase.sharedStorage(),
            fileUriCollectPhase.nodeIds().size(),
            getReaderNumber(fileUriCollectPhase.nodeIds(), clusterService.state().nodes().getLocalNodeId()),
            fileUriCollectPhase.inputFormat()
        );
    }

    private static int getReaderNumber(Collection<String> nodeIds, String localNodeId) {
        String[] readers = nodeIds.toArray(new String[0]);
        Arrays.sort(readers);
        return Arrays.binarySearch(readers, localNodeId);
    }

    private static List<String> targetUriToStringList(TransactionContext txnCtx,
                                                      Functions functions,
                                                      Symbol targetUri) {
        Object value = SymbolEvaluator.evaluate(txnCtx, functions, targetUri, Row.EMPTY, SubQueryResults.EMPTY);
        if (targetUri.valueType() == DataTypes.STRING) {
            String uri = (String) value;
            return Collections.singletonList(uri);
        } else if (targetUri.valueType() instanceof ArrayType && ((ArrayType) targetUri.valueType()).innerType() == DataTypes.STRING) {
            //noinspection unchecked
            return (List<String>) value;
        }

        // this case actually never happens because the check is already done in the analyzer
        throw CopyFromAnalyzedStatement.raiseInvalidType(targetUri.valueType());
    }
}
