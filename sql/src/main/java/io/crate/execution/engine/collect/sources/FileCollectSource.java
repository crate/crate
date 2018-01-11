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
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.ValueSymbolVisitor;
import io.crate.data.RowConsumer;
import io.crate.data.BatchIterator;
import io.crate.metadata.Functions;
import io.crate.expression.InputFactory;
import io.crate.execution.engine.collect.BatchIteratorCollectorBridge;
import io.crate.execution.engine.collect.CrateCollector;
import io.crate.execution.engine.collect.JobCollectContext;
import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import io.crate.execution.engine.collect.files.LineCollectorExpression;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.types.CollectionType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Singleton
public class FileCollectSource implements CollectSource {

    private final ClusterService clusterService;
    private final Map<String, FileInputFactory> fileInputFactoryMap;
    private final InputFactory inputFactory;

    @Inject
    public FileCollectSource(Functions functions, ClusterService clusterService, Map<String, FileInputFactory> fileInputFactoryMap) {
        this.fileInputFactoryMap = fileInputFactoryMap;
        inputFactory = new InputFactory(functions);
        this.clusterService = clusterService;
    }

    @Override
    public CrateCollector getCollector(CollectPhase collectPhase, RowConsumer consumer, JobCollectContext jobCollectContext) {
        FileUriCollectPhase fileUriCollectPhase = (FileUriCollectPhase) collectPhase;
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(FileLineReferenceResolver::getImplementation);
        ctx.add(collectPhase.toCollect());

        String[] readers = fileUriCollectPhase.nodeIds().toArray(
            new String[fileUriCollectPhase.nodeIds().size()]);
        Arrays.sort(readers);

        List<String> fileUris;
        fileUris = targetUriToStringList(fileUriCollectPhase.targetUri());
        BatchIterator fileReadingIterator = FileReadingIterator.newInstance(
            fileUris,
            ctx.topLevelInputs(),
            ctx.expressions(),
            fileUriCollectPhase.compression(),
            fileInputFactoryMap,
            fileUriCollectPhase.sharedStorage(),
            readers.length,
            Arrays.binarySearch(readers, clusterService.state().nodes().getLocalNodeId()),
            fileUriCollectPhase.inputFormat()
        );

        return BatchIteratorCollectorBridge.newInstance(fileReadingIterator, consumer);
    }

    private static List<String> targetUriToStringList(Symbol targetUri) {
        if (targetUri.valueType() == DataTypes.STRING) {
            return Collections.singletonList(ValueSymbolVisitor.STRING.process(targetUri));
        } else if (targetUri.valueType() instanceof CollectionType
                   && ((CollectionType) targetUri.valueType()).innerType() == DataTypes.STRING) {
            return ValueSymbolVisitor.STRING_LIST.process(targetUri);
        }

        // this case actually never happens because the check is already done in the analyzer
        throw CopyFromAnalyzedStatement.raiseInvalidType(targetUri.valueType());
    }
}
