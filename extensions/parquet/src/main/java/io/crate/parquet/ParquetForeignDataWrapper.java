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

package io.crate.parquet;

import static io.crate.types.DataTypes.STRING_ARRAY;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import dev.hardwood.InputFile;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.InputColumns.SourceSymbols;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.pipeline.InputRowProjector;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.InputFactory.Context;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationMetadata;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.expression.InputFactory;
import io.crate.role.Role;

public class ParquetForeignDataWrapper implements ForeignDataWrapper {

    private final InputFactory inputFactory;
    private final Setting<List<String>> inputFilesSetting = Setting.listSetting("inputfiles", new ArrayList<>(),
            s -> s.toString(), STRING_ARRAY);
    private final List<Setting<?>> mandatoryServerOptions = List.of(inputFilesSetting);

    public ParquetForeignDataWrapper(Settings settings, InputFactory inputFactory, Executor executor) {
        this.inputFactory = inputFactory;
    }

    @Override
    public boolean supportsQueryPushdown(Symbol query) {
        return true;
    }

    @Override
    public List<Setting<?>> mandatoryServerOptions() {
        return mandatoryServerOptions;
    }

    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(Role currentUser,
            Server server,
            RelationMetadata.ForeignTable foreignTable,
            TransactionContext txnCtx,
            List<Symbol> collect,
            Symbol query) {
        Settings options = server.options();
        List<String> paths = inputFilesSetting.get(options);
        List<Path> parquetFiles = paths.stream().map(Paths::get).toList();

        List<Reference> columns = new ArrayList<>();
        for (Symbol symbol : collect) {
            symbol.visit(Reference.class, columns::add);
        }

        BatchIterator<Row> it = new ParquetBatchIterator(InputFile.ofPaths(parquetFiles), columns, query);

        // Evaluate any expressions in `collect` that aren't plain column references
        // (i.e. select x * 2 from parquet_file)
        boolean allReferences = collect.stream().allMatch(s -> s instanceof Reference);
        if (!allReferences) {
            SourceSymbols sourceRefs = new InputColumns.SourceSymbols(columns);
            List<Symbol> inputColumns = InputColumns.create(collect, sourceRefs);
            Context<CollectExpression<Row, ?>> inputCtx = inputFactory.ctxForInputColumns(txnCtx, inputColumns);
            InputRowProjector inputRowProjector = new InputRowProjector(inputCtx.topLevelInputs(),
                    inputCtx.expressions());
            it = inputRowProjector.apply(it);
        }

        return CompletableFuture.completedFuture(it);

    }
}
