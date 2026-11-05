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

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig.S3;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import dev.hardwood.InputFile;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.InputColumns.SourceSymbols;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.pipeline.InputRowProjector;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.InputFactory.Context;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.metadata.TransactionContext;
import io.crate.opendal.S3Env;
import io.crate.opendal.S3URI;
import io.crate.opendal.SharedAsyncExecutor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationMetadata;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.expression.InputFactory;
import io.crate.role.Role;

public class ParquetForeignDataWrapper implements ForeignDataWrapper {

    private final InputFactory inputFactory;
    private final Setting<List<String>> filesSetting = Setting.listSetting("files", new ArrayList<>(), s -> s, STRING_ARRAY);
    private final Setting<String> protocolSetting = Setting.simpleString("protocol");
    private final Setting<String> regionSetting = Setting.simpleString("region");
    private final List<Setting<?>> mandatoryServerOptions = List.of(filesSetting);
    private final List<Setting<?>> optionalServerOptions = List.of(protocolSetting, regionSetting);
    private AsyncExecutor executor;

    public ParquetForeignDataWrapper(InputFactory inputFactory) {
        this.inputFactory = inputFactory;
    }

    @Override
    public boolean supportsQueryPushdown(Symbol query) {
        return false;
    }

    @Override
    public List<Setting<?>> mandatoryServerOptions() {
        return mandatoryServerOptions;
    }

    @Override
    public List<Setting<?>> optionalServerOptions() {
        return optionalServerOptions;
    }

    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(Role currentUser,
            Server server,
            RelationMetadata.ForeignTable foreignTable,
            TransactionContext txnCtx,
            List<Symbol> collect,
            Symbol query) {
        Settings options = server.options();
        List<String> pathsToFiles = filesSetting.get(options);
        if (pathsToFiles.isEmpty()) {
            throw new InvalidArgumentException("No files provided. Please provide paths to parquet files");
        }

        List<InputFile> inputFiles = new ArrayList<>();
        for (String p : pathsToFiles) {
            URI uri = URI.create(p);
            String scheme = uri.getScheme();
            if (scheme == null || scheme.equals("file")) {
                if (!currentUser.isSuperUser()) {
                    throw new UnauthorizedException("Only a superuser can read from the local file system");
                }
                inputFiles.add(InputFile.of(Path.of(uri.getPath())));
            } else if (scheme.equals("s3")) {
                if (executor == null) {
                    SharedAsyncExecutor sharedAsyncExecutor = SharedAsyncExecutor.getInstance(options);
                    executor = sharedAsyncExecutor.asyncExecutor();
                }
                S3URI s3uri = S3URI.of(URI.create(p));
                S3 s3 = S3Env.getServiceConfig(s3uri, options);
                Operator operator = AsyncOperator.of(s3, executor).blocking();
                inputFiles.add(new OpenDALInputFile(operator, s3uri.path()));
            } else {
                throw new InvalidArgumentException("Unsupported URI scheme: " + scheme);
            }
        }


        List<Reference> columns = new ArrayList<>();
        for (Symbol symbol : collect) {
            symbol.visit(Reference.class, ref -> {
                ColumnIdent col = ref.column();
                if (!col.isRoot()) {
                    Reference readRef = foreignTable.getReadReference(col.getRoot());
                    assert readRef != null;
                    columns.add(readRef);
                } else {
                    columns.add(ref);
                }
            });
        }

        BatchIterator<Row> it = new ParquetBatchIterator(inputFiles, columns);

        SourceSymbols sourceRefs = new InputColumns.SourceSymbols(columns);
        List<Symbol> inputColumns = InputColumns.create(collect, sourceRefs);
        Context<CollectExpression<Row, ?>> inputCtx = inputFactory.ctxForInputColumns(txnCtx, inputColumns);
        InputRowProjector inputRowProjector = new InputRowProjector(inputCtx.topLevelInputs(),
                inputCtx.expressions());
        it = inputRowProjector.apply(it);

        return CompletableFuture.completedFuture(it);
    }
}
