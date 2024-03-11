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

package io.crate.execution.engine.collect.sources;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.CopyFromParserProperties;
import io.crate.analyze.SymbolEvaluator;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.SkippingBatchIterator;
import io.crate.exceptions.UnauthorizedException;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import io.crate.execution.engine.collect.files.LineCollectorExpression;
import io.crate.execution.engine.collect.files.LineProcessor;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

@Singleton
public class FileCollectSource implements CollectSource {

    private final ClusterService clusterService;
    private final Map<String, FileInputFactory> fileInputFactoryMap;
    private final InputFactory inputFactory;
    private final NodeContext nodeCtx;
    private final ThreadPool threadPool;
    private final Roles roles;

    @Inject
    public FileCollectSource(NodeContext nodeCtx,
                             ClusterService clusterService,
                             Map<String, FileInputFactory> fileInputFactoryMap,
                             ThreadPool threadPool,
                             Roles roles) {
        this.fileInputFactoryMap = fileInputFactoryMap;
        this.nodeCtx = nodeCtx;
        this.inputFactory = new InputFactory(nodeCtx);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.roles = roles;
    }

    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                             CollectPhase collectPhase,
                                                             CollectTask collectTask,
                                                             boolean supportMoveToStart) {
        FileUriCollectPhase fileUriCollectPhase = (FileUriCollectPhase) collectPhase;
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(txnCtx, FileLineReferenceResolver::getImplementation);
        ctx.add(collectPhase.toCollect());

        Role user = requireNonNull(roles.findUser(txnCtx.sessionSettings().userName()), "User who invoked a statement must exist");
        List<URI> fileUris = targetUriToStringList(txnCtx, nodeCtx, fileUriCollectPhase.targetUri()).stream()
            .map(s -> {
                var uri = FileReadingIterator.toURI(s);
                if (uri.getScheme().equals("file") && user.isSuperUser() == false) {
                    throw new UnauthorizedException("Only a superuser can read from the local file system");
                }
                return uri;
            })
            .toList();
        FileReadingIterator fileReadingIterator = new FileReadingIterator(
            fileUris,
            fileUriCollectPhase.compression(),
            fileInputFactoryMap,
            fileUriCollectPhase.sharedStorage(),
            fileUriCollectPhase.nodeIds().size(),
            getReaderNumber(fileUriCollectPhase.nodeIds(), clusterService.state().nodes().getLocalNodeId()),
            fileUriCollectPhase.withClauseOptions(),
            threadPool.scheduler()
        );
        CopyFromParserProperties parserProperties = fileUriCollectPhase.parserProperties();
        LineProcessor lineProcessor = new LineProcessor(
            parserProperties.skipNumLines() > 0
                ? new SkippingBatchIterator<>(fileReadingIterator, (int) parserProperties.skipNumLines())
                : fileReadingIterator,
            ctx.topLevelInputs(),
            ctx.expressions(),
            fileUriCollectPhase.inputFormat(),
            parserProperties,
            fileUriCollectPhase.targetColumns()
        );
        return CompletableFuture.completedFuture(lineProcessor);
    }

    @VisibleForTesting
    public static int getReaderNumber(Collection<String> nodeIds, String localNodeId) {
        String[] readers = nodeIds.toArray(new String[0]);
        Arrays.sort(readers);
        return Arrays.binarySearch(readers, localNodeId);
    }

    private static List<String> targetUriToStringList(TransactionContext txnCtx,
                                                      NodeContext nodeCtx,
                                                      Symbol targetUri) {
        Object value = SymbolEvaluator.evaluate(txnCtx, nodeCtx, targetUri, Row.EMPTY, SubQueryResults.EMPTY);
        if (targetUri.valueType().id() == DataTypes.STRING.id()) {
            String uri = (String) value;
            return Collections.singletonList(uri);
        } else if (DataTypes.STRING_ARRAY.equals(targetUri.valueType())) {
            return DataTypes.STRING_ARRAY.implicitCast(value);
        }

        // this case actually never happens because the check is already done in the analyzer
        throw AnalyzedCopyFrom.raiseInvalidType(targetUri.valueType());
    }
}
