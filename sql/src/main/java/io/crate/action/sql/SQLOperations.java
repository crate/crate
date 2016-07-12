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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionState;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.ReadOnlyException;
import io.crate.executor.Executor;
import io.crate.executor.TaskResult;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;
import static io.crate.action.sql.SQLRequest.EMPTY_ARGS;
import static io.crate.action.sql.TransportBaseSQLAction.NODE_READ_ONLY_SETTING;

@Singleton
public class SQLOperations {

    private final static ESLogger LOGGER = Loggers.getLogger(SQLOperations.class);

    private final Analyzer analyzer;
    private final Planner planner;
    private final Provider<Executor> executorProvider;
    private final StatsTables statsTables;
    private final boolean isReadOnly;

    @Inject
    public SQLOperations(Analyzer analyzer,
                         Planner planner,
                         Provider<Executor> executorProvider,
                         StatsTables statsTables,
                         Settings settings) {
        this.analyzer = analyzer;
        this.planner = planner;
        this.executorProvider = executorProvider;
        this.statsTables = statsTables;
        this.isReadOnly = settings.getAsBoolean(NODE_READ_ONLY_SETTING, false);
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
        private int maxRows = 0;

        private Throwable throwable = null;
        private List<List<Object>> bulkParams = new ArrayList<>();
        private List<ResultReceiver> resultReceivers = new ArrayList<>();
        private Map<String, String> settings = new HashMap<>();

        @Nullable
        private FormatCodes.FormatCode[] resultFormatCodes;

        private Session(Executor executor, String defaultSchema) {
            this.executor = executor;
            this.defaultSchema = defaultSchema;
        }

        public void simpleQuery(String query,
                                Function<List<Field>, ResultReceiver> resultReceiverFactory,
                                CompletionListener doneListener) {
            // TODO: support multiple statements
            if (query.endsWith(";")) {
                query = query.substring(0, query.length() - 1);
            }

            Statement statement = SqlParser.createStatement(query);
            statsTables.jobStarted(jobId, query);
            Analysis analysis = analyzer.analyze(statement, new ParameterContext(EMPTY_ARGS, EMPTY_BULK_ARGS, defaultSchema));
            validateReadOnly(analysis);
            Plan plan = planner.plan(analysis, UUID.randomUUID(), 0, 0);

            ResultReceiver resultReceiver;
            if (analysis.rootRelation() == null) {
                resultReceiver = resultReceiverFactory.apply(null);
            } else {
                resultReceiver = resultReceiverFactory.apply(analysis.rootRelation().fields());
            }
            assert resultReceiver != null : "resultReceiver must not be null";
            resultReceiver.addListener(doneListener);
            resultReceiver.addListener(new StatsTablesUpdateListener(jobId, statsTables));
            applySessionSettings(plan);
            executor.execute(plan, resultReceiver);
        }

        private Object[] getArgs() {
            if (bulkParams.size() == 1) {
                return bulkParams.get(0).toArray(new Object[0]);
            } else {
                return EMPTY_ARGS;
            }
        }

        private void checkError() {
            if (throwable != null) {
                throw Throwables.propagate(throwable);
            }
        }

        public void parse(String statementName, String query, List<DataType> paramTypes) {
            LOGGER.debug("method=parse stmtName={} query={} paramTypes={}", statementName, query, paramTypes);
            checkError();

            try {
                Statement statement = SqlParser.createStatement(query);
                if (this.statement == null) {
                    this.jobId = UUID.randomUUID();
                    statsTables.jobStarted(jobId, query);
                } else if (!statement.equals(this.statement)) {
                    // different query -> no bulk operation -> execute previous query
                    sync(CompletionListener.NO_OP);
                    this.jobId = UUID.randomUUID();
                    statsTables.jobStarted(jobId, query);
                }
                this.statement = statement;
                this.query = query;
                this.paramTypes = paramTypes;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void bind(String portalName, String statementName, List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
            LOGGER.debug("method=bind portalName={} statementName={} params={}", portalName, statementName, params);
            checkError();
            bulkParams.add(params);
            this.resultFormatCodes = resultFormatCodes;
            if (analysis == null) {
                // analyze only once in a batch operation where there is
                // Parse -> Bind -> Describe -> Execute -> Parse -> Bind -> Describe -> Execute -> ... -> Sync
                analysis = analyzer.analyze(statement, new ParameterContext(getArgs(), EMPTY_BULK_ARGS, defaultSchema));
            }
        }

        public List<Field> describe(char type, String portalOrStatement) {
            LOGGER.debug("method=describe type={} portalOrStatement={}", type, portalOrStatement);
            checkError();

            try {
                if (analysis.rootRelation() == null) {
                    return null;
                }
                List<Field> fields = analysis.rootRelation().fields();
                outputTypes = Symbols.extractTypes(fields);
                return fields;
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public void execute(String portalName, int maxRows, ResultReceiver rowReceiver) {
            LOGGER.debug("method=describe portalName={} maxRows={}", portalName, maxRows);
            checkError();
            validateReadOnly(analysis);

            resultReceivers.add(rowReceiver);
            this.maxRows = maxRows;
        }

        public void sync(CompletionListener listener) {
            LOGGER.debug("method=sync");

            if (throwable == null) {
                /**
                 * Execution is delayed to sync() to be able to make efficient bulk requests.
                 *
                 * JDBC batch execution can cause the following flow:
                 *
                 * Parse -> Bind -> Execute
                 * Parse -> Bind -> Execute
                 * Sync
                 *
                 * If the second parse is called with a *different* statement, the parse call will trigger sync.
                 * Cleanup needs to happen before parse returns because clients don't wait for parseOk but follow up
                 * with bind/execute immediately.
                 *
                 * This is why the global state is assigned to local variables and the cleanup is called immediately.
                 * Otherwise there would be concurrency/async state modification.
                 */
                if (resultReceivers.size() == 1) {
                    Plan plan = planner.plan(analysis, jobId, maxRows, maxRows);
                    ResultReceiver resultReceiver = resultReceivers.get(0);
                    cleanup();
                    applySessionSettings(plan);
                    execute(plan, resultReceiver, listener);
                } else {
                    Object[][] bulkArgs = toBulkArgs(bulkParams);
                    Analysis analysis = analyzer.analyze(statement, new ParameterContext(new Object[0], bulkArgs, defaultSchema));
                    ImmutableList<ResultReceiver> resultReceivers = ImmutableList.copyOf(this.resultReceivers);
                    Plan plan = planner.plan(analysis, jobId, maxRows, maxRows);
                    cleanup();
                    executeBulk(plan, resultReceivers, listener);
                }
            } else {
                cleanup();
                Throwable t = this.throwable;
                this.throwable = null;
                throw Throwables.propagate(t);
            }
        }

        private void execute(Plan plan, ResultReceiver resultReceiver, CompletionListener listener) {
            resultReceiver.addListener(listener);
            resultReceiver.addListener(new StatsTablesUpdateListener(jobId, statsTables));
            executor.execute(plan, resultReceiver);
        }

        private void executeBulk(Plan plan, final List<ResultReceiver> resultReceivers, final CompletionListener listener) {
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
                    statsTables.jobFinished(jobId, null);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    for (ResultReceiver resultReceiver : resultReceivers) {
                        resultReceiver.fail(t);
                    }
                    listener.onFailure(t);
                    statsTables.jobFinished(jobId, Exceptions.messageOf(t));
                }
            });
        }

        private void applySessionSettings(Plan plan) {
            if (plan instanceof SetSessionPlan) {
                settings.putAll(((SetSessionPlan) plan).settings().getAsMap());
            }
        }

        private void validateReadOnly(Analysis analysis) {
            if (analysis.analyzedStatement().isWriteOperation() && isReadOnly) {
                throw new ReadOnlyException();
            }
        }

        private void cleanup() {
            analysis = null;
            bulkParams.clear();
            maxRows = 0;
            resultReceivers.clear();
            statement = null;
            resultFormatCodes = null;
            outputTypes = null;
        }

        public List<? extends DataType> outputTypes() {
            return outputTypes;
        }

        public String query() {
            return query;
        }

        public DataType getParamType(int idx) {
            return paramTypes.get(idx);
        }

        @Nullable
        public FormatCodes.FormatCode[] resultFormatCodes() {
            return resultFormatCodes;
        }
    }

    private static Object[][] toBulkArgs(List<List<Object>> bulkParams) {
        Object[][] bulkArgs = new Object[bulkParams.size()][];
        for (int i = 0; i < bulkArgs.length; i++) {
            bulkArgs[i] = bulkParams.get(i).toArray(new Object[0]);
        }
        return bulkArgs;
    }

    private static class StatsTablesUpdateListener implements CompletionListener {

        private final UUID jobId;
        private final StatsTables statsTables;

        StatsTablesUpdateListener(UUID jobId, StatsTables statsTables) {
            this.jobId = jobId;
            this.statsTables = statsTables;
        }
        @Override
        public void onSuccess(@Nullable CompletionState result) {
            statsTables.jobFinished(jobId, null);
        }

        @Override
        public void onFailure(Throwable t) {
            statsTables.jobFinished(jobId, Exceptions.messageOf(t));
        }
    }
}
