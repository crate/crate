/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.action.sql;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;
import static io.crate.action.sql.SQLRequest.EMPTY_ARGS;

@Singleton
public class SQLOperations {

    private final static ESLogger LOGGER = Loggers.getLogger(SQLOperations.class);

    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<Executor> executorProvider;

    @Inject
    public SQLOperations(Analyzer analyzer,
                         Planner planner,
                         Provider<Executor> executorProvider) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
    }

    public Session createSession(@Nullable String defaultSchema) {
        return new Session(executorProvider.get(), defaultSchema);
    }

    /**
     * Stateful Session
     * In the PSQL case there is one session per connection.
     *
     *
     * Methods are usually called in the following order:
     *
     * <pre>
     * parse(...)
     * bind(...)
     * describe(...) // optional
     * execute(...)
     * sync()
     * </pre>
     *
     * Or:
     *
     * <pre>
     * parse(...)
     * loop:
     *      bind(...)
     *      execute(...)
     * sync()
     * </pre>
     *
     * If during one of the operation an error occurs the error will be raised and all subsequent methods will become
     * no-op operations until sync() is called which will again raise an error and "clear" it. (to be able to process new statements)
     *
     * This is done for compatibility with the postgres extended spec:
     *
     * > The purpose of Sync is to provide a resynchronization point for error recovery.
     * > When an error is detected while processing any extended-query message, the backend issues ErrorResponse,
     * > then reads and discards messages until a Sync is reached,
     * > then issues ReadyForQuery and returns to normal message processing.
     * > (But note that no skipping occurs if an error is detected while processing Sync —
     * > this ensures that there is one and only one ReadyForQuery sent for each Sync.)
     *
     * (https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
     */
    public class Session {

        private final Executor executor;
        private final String defaultSchema;

        private UUID jobId;
        private List<DataType> paramTypes;
        private Statement statement;
        private Analysis analysis;
        private List<DataType> outputTypes;
        private String query;

        private Throwable throwable = null;
        private List<List<Object>> bulkParams = new ArrayList<>();
        private List<ResultReceiver> resultReceivers = new ArrayList<>();

        private Session(Executor executor, String defaultSchema) {
            this.executor = executor;
            this.defaultSchema = defaultSchema;
        }

        public void parse(String statementName, String query, List<DataType> paramTypes) {
            LOGGER.debug("method=parse stmtName={} query={} paramTypes={}", statementName, query, paramTypes);

            try {
                this.jobId = UUID.randomUUID();
                this.query = query;
                this.statement = SqlParser.createStatement(query);
                this.paramTypes = paramTypes;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public DataType getParamType(int idx) {
            return paramTypes.get(idx);
        }

        public void bind(String portalName, String statementName, List<Object> params) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);

            if (throwable != null) {
                return;
            }
            bulkParams.add(params);
        }

        public List<Field> describe(char type, String portalOrStatement) {
            LOGGER.debug("method=describe type={} portalOrStatement={}", type, portalOrStatement);

            if (throwable != null) {
                return null;
            }
            if (analysis == null) {
                if (statement == null) {
                    throwable = new IllegalStateException("describe was called without prior parse call");
                    throw (RuntimeException) throwable;
                }
                analysis = analyzer.analyze(statement, new ParameterContext(getArgs(), EMPTY_BULK_ARGS, defaultSchema));
            }
            if (analysis.rootRelation() == null) {
                return null;
            }
            List<Field> fields = analysis.rootRelation().fields();
            outputTypes = Symbols.extractTypes(fields);
            return fields;
        }

        private Object[] getArgs() {
            if (bulkParams.size() == 1) {
                return bulkParams.get(0).toArray(new Object[0]);
            } else {
                return EMPTY_ARGS;
            }
        }

        public void execute(String portalName, int maxRows, ResultReceiver rowReceiver) {
            LOGGER.debug("method=describe portalName={} maxRows={}", portalName, maxRows);

            if (throwable != null) {
                return;
            }
            resultReceivers.add(rowReceiver);
        }

        public void sync(CompletionListener listener) {
            LOGGER.debug("method=sync");

            if (throwable == null) {
                if (resultReceivers.isEmpty()) {
                    throw new IllegalStateException("sync called, but there was no execute");
                }
                if (resultReceivers.size() == 1) {
                    execute(listener);
                } else {
                    executeBulk(listener);
                }
            } else {
                Throwable t = this.throwable;
                this.throwable = null;
                throw Throwables.propagate(t);
            }
        }

        private void execute(CompletionListener listener) {
            Plan plan = planner.plan(analysis, jobId);
            ResultReceiver resultReceiver = resultReceivers.get(0);
            resultReceiver.addListener(listener);
            executor.execute(plan, resultReceiver);
            cleanup();
        }

        private void executeBulk(final CompletionListener listener) {
            Object[][] bulkArgs = toBulkArgs(bulkParams);
            analysis = analyzer.analyze(statement, new ParameterContext(new Object[0], bulkArgs, defaultSchema));
            Plan plan = planner.plan(analysis, jobId);

            List<? extends ListenableFuture<TaskResult>> futures = executor.executeBulk(plan);
            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<TaskResult>>() {
                @Override
                public void onSuccess(@Nullable List<TaskResult> result) {
                    assert result != null && result.size() == resultReceivers.size()
                        : "number of result must match number of rowReceivers";

                    for (int i = 0; i < result.size(); i++) {
                        ResultReceiver resultReceiver = resultReceivers.get(i);
                        resultReceiver.setNextRow(result.get(i).rows().iterator().next());
                        resultReceiver.finish();
                    }
                    listener.onSuccess(null);
                    cleanup();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    for (ResultReceiver resultReceiver : resultReceivers) {
                        resultReceiver.fail(t);
                    }
                    listener.onFailure(t);
                    cleanup();
                }
            });
        }

        private void cleanup() {
            analysis = null;
            bulkParams.clear();
            resultReceivers.clear();
            statement = null;
        }

        public List<? extends DataType> outputTypes() {
            return outputTypes;
        }

        public String query() {
            return query;
        }
    }

    private static Object[][] toBulkArgs(List<List<Object>> bulkParams) {
        Object[][] bulkArgs = new Object[bulkParams.size()][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = bulkParams.get(i).toArray(new Object[0]);
        }
        return bulkArgs;
    }
}
