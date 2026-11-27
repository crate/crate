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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig.S3;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import dev.hardwood.InputFile;
import io.crate.common.io.IOUtils;
import io.crate.data.BatchIterator;
import io.crate.data.FilteringBatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.InputColumns.SourceSymbols;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.pipeline.InputRowProjector;
import io.crate.expression.InputFactory;
import io.crate.expression.RowFilter;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationMetadata;
import io.crate.metadata.TransactionContext;
import io.crate.opendal.S3Env;
import io.crate.opendal.S3URI;
import io.crate.opendal.SharedAsyncExecutor;
import io.crate.role.Role;
import io.crate.expression.symbol.Literal;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.expression.symbol.Function;

public class ParquetForeignDataWrapper implements ForeignDataWrapper {

    private final SharedAsyncExecutor sharedAsyncExecutor;
    private final InputFactory inputFactory;
    private final Setting<List<String>> filesSetting = Setting.listSetting("files", new ArrayList<>(), s -> s, STRING_ARRAY);
    private final Setting<String> protocolSetting = Setting.simpleString("protocol");
    private final Setting<String> regionSetting = Setting.simpleString("region");
    private final List<Setting<?>> mandatoryServerOptions = List.of(filesSetting);
    private final List<Setting<?>> optionalServerOptions = List.of(protocolSetting, regionSetting);

    private static final Set<String> SUPPORTED_PREDICATE_PUSHDOWN_TYPES = Set.of(LongType.NAME, IntegerType.NAME);

    private record Predicates(Symbol pushable, Symbol notPushable) {}

    public static List<InputFile> urisToInputFiles(AsyncExecutor asyncExecutor,
                                                   Role currentUser,
                                                   List<String> uris,
                                                   Settings options) {
        List<InputFile> inputFiles = new ArrayList<>();
        for (String p : uris) {
            URI uri = URI.create(p);
            String scheme = uri.getScheme();
            if (scheme == null || scheme.equals("file")) {
                if (!currentUser.isSuperUser()) {
                    throw new UnauthorizedException("Only a superuser can read from the local file system");
                }
                inputFiles.add(InputFile.of(Path.of(uri.getPath())));
            } else if (scheme.equals("s3")) {
                S3URI s3uri = S3URI.of(URI.create(p));
                S3 s3 = S3Env.getServiceConfig(s3uri, options);
                Operator operator = AsyncOperator.of(s3, asyncExecutor).blocking();
                inputFiles.add(new OpenDALInputFile(operator, s3uri.path()));
            } else {
                IOUtils.closeWhileHandlingException(inputFiles);
                throw new InvalidArgumentException("Unsupported URI scheme: " + scheme);
            }
        }
        return inputFiles;
    }

    public ParquetForeignDataWrapper(Settings settings, InputFactory inputFactory) {
        this.sharedAsyncExecutor = SharedAsyncExecutor.getInstance(settings);
        this.inputFactory = inputFactory;
    }

    @Override
    public void close() throws IOException {
        sharedAsyncExecutor.close();
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
    public List<Setting<?>> optionalServerOptions() {
        return optionalServerOptions;
    }

    private static boolean isPushable(Symbol conjunct) {
        if (conjunct instanceof Function function) {
            if (!ParquetBatchIterator.FILTER_PUSHDOWN_OPERATORS.containsKey(function.name())) {
                return false;
            }

            assert function.arguments().size() == 2;
            Symbol lhs = function.arguments().get(0);
            Symbol rhs = function.arguments().get(1);
            if (!(lhs instanceof Reference ref && rhs instanceof Literal)
                    || !SUPPORTED_PREDICATE_PUSHDOWN_TYPES.contains(ref.valueType().getName())) {
                return false;
            }
            return true;
        }
        return false;
    }

    private static Predicates classifyPredicates(Symbol query) {
        List<Symbol> pushable = new ArrayList<>();
        List<Symbol> notPushable = new ArrayList<>();
        if (query instanceof Function function) {
            if (function.name().equals(AndOperator.NAME)) {
                for (Symbol conjunct : AndOperator.split(query)) {
                    if (isPushable(conjunct)) {
                        pushable.add(conjunct);
                    } else {
                        notPushable.add(conjunct);
                    }
                }
            } else {
                if (isPushable(function)) {
                    pushable.add(function);
                } else {
                    notPushable.add(function);
                }
            }

        }
        return new Predicates(AndOperator.join(pushable), AndOperator.join(notPushable));
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

        List<Reference> columns = new ArrayList<>();
        List<Symbol> allSymbols = new ArrayList<>(collect);
        // A predicate may reference a column that isn't in the SELECT statement and that cannot be pushed down.
        // We must include those references in projection pushdown into Hardwood so that the FilteringBatchIterator later has the data to filter.
        allSymbols.add(query);
        for (Symbol symbol : allSymbols) {
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


        SourceSymbols sourceRefs = new InputColumns.SourceSymbols(columns);

        Predicates predicates = classifyPredicates(query);
        Symbol retainedPredicate = InputColumns.create(predicates.notPushable, sourceRefs);

        List<Symbol> inputColumns = InputColumns.create(collect, sourceRefs);
        Context<CollectExpression<Row, ?>> inputCtx = inputFactory.ctxForInputColumns(txnCtx, inputColumns);
        InputRowProjector inputRowProjector = new InputRowProjector(
            inputCtx.topLevelInputs(),
            inputCtx.expressions()
        );

        AsyncExecutor asyncExecutor = sharedAsyncExecutor.asyncExecutor();
        List<InputFile> inputFiles = urisToInputFiles(asyncExecutor, currentUser, pathsToFiles, options);
        BatchIterator<Row> it = new ParquetBatchIterator(inputFiles, columns, predicates.pushable);
        it = new FilteringBatchIterator<>(it, RowFilter.create(txnCtx, inputFactory, retainedPredicate));
        it = inputRowProjector.apply(it);
        return CompletableFuture.completedFuture(it);
    }
}
