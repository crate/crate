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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.metadata.Functions;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.files.FileCollectInputSymbolVisitor;
import io.crate.operation.collect.files.FileInputFactory;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.FileUriCollectPhase;
import io.crate.types.CollectionType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.*;

@Singleton
public class FileCollectSource implements CollectSource {

    private final ClusterService clusterService;
    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;
    private final Map<String, FileInputFactory> fileInputFactoryMap;

    @Inject
    public FileCollectSource(Functions functions, ClusterService clusterService, Map<String, FileInputFactory> fileInputFactoryMap) {
        this.fileInputFactoryMap = fileInputFactoryMap;
        fileInputSymbolVisitor = new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
        this.clusterService = clusterService;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        FileUriCollectPhase fileUriCollectPhase = (FileUriCollectPhase) collectPhase;
        FileCollectInputSymbolVisitor.Context context = fileInputSymbolVisitor.extractImplementations(collectPhase.toCollect());

        String[] readers = fileUriCollectPhase.executionNodes().toArray(
                new String[fileUriCollectPhase.executionNodes().size()]);
        Arrays.sort(readers);

        List<String> fileUris;
        fileUris = targetUriToStringList(fileUriCollectPhase.targetUri());
        return ImmutableList.<CrateCollector>of(new FileReadingCollector(
                fileUris,
                context.topLevelInputs(),
                context.expressions(),
                downstream,
                fileUriCollectPhase.fileFormat(),
                fileUriCollectPhase.compression(),
                fileInputFactoryMap,
                fileUriCollectPhase.sharedStorage(),
                readers.length,
                Arrays.binarySearch(readers, clusterService.state().nodes().localNodeId())
        ));
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
