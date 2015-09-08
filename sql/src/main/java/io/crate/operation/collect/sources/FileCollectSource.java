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
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Functions;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.collect.files.FileCollectInputSymbolVisitor;
import io.crate.operation.collect.files.FileInputFactory;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.FileUriCollectPhase;
import io.crate.planner.symbol.ValueSymbolVisitor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Arrays;
import java.util.Collection;

@Singleton
public class FileCollectSource implements CollectSource {

    private final ClusterService clusterService;
    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;

    @Inject
    public FileCollectSource(Functions functions, ClusterService clusterService) {
        fileInputSymbolVisitor = new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
        this.clusterService = clusterService;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {

        if (collectPhase.whereClause().noMatch()){
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }

        FileCollectInputSymbolVisitor.Context context = fileInputSymbolVisitor.extractImplementations(collectPhase);
        FileUriCollectPhase fileUriCollectPhase = (FileUriCollectPhase) collectPhase;

        String[] readers = fileUriCollectPhase.executionNodes().toArray(
                new String[fileUriCollectPhase.executionNodes().size()]);
        Arrays.sort(readers);
        return ImmutableList.<CrateCollector>of(new FileReadingCollector(
                    ValueSymbolVisitor.STRING.process(fileUriCollectPhase.targetUri()),
                    context.topLevelInputs(),
                    context.expressions(),
                    downstream,
                    fileUriCollectPhase.fileFormat(),
                    fileUriCollectPhase.compression(),
                    ImmutableMap.<String, FileInputFactory>of(),
                    fileUriCollectPhase.sharedStorage(),
                    jobCollectContext.keepAliveListener(),
                    readers.length,
                    Arrays.binarySearch(readers, clusterService.state().nodes().localNodeId())
        ));
    }
}
