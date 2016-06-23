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
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.executor.Executor;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

import static io.crate.action.sql.SQLBulkRequest.EMPTY_BULK_ARGS;

@Singleton
public class SQLOperations {

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
        private final UUID jobId;

        private List<DataType> paramTypes;
        private Statement statement;
        private Analysis analysis;
        private Plan plan;
        private ResultReceiver rowReceiver;
        private List<DataType> outputTypes;
        private String query;

        private Throwable throwable = null;

        private Session(Executor executor, String defaultSchema) {
            this.executor = executor;
            this.defaultSchema = defaultSchema;
            this.jobId = UUID.randomUUID();
        }

        public void parse(String statementName, String query, List<DataType> paramTypes) {
            try {
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
            if (throwable != null) {
                return;
            }
            try {
                analysis = analyzer.analyze(statement, new ParameterContext(params.toArray(new Object[0]), EMPTY_BULK_ARGS, defaultSchema));
                plan = planner.plan(analysis, jobId);
            } catch (Throwable t) {
                throwable = t;
                throw t;
            }
        }

        public List<Field> describe(char type, String portalOrStatement) {
            if (throwable != null) {
                return null;
            }
            if (analysis == null) {
                throw new IllegalStateException("describe called, but there was no bind");
            }
            if (analysis.rootRelation() == null) {
                return null;
            }
            List<Field> fields = analysis.rootRelation().fields();
            outputTypes = Symbols.extractTypes(fields);
            return fields;
        }

        public void execute(String portalName, int maxRows, ResultReceiver rowReceiver) {
            if (throwable != null) {
                return;
            }
            this.rowReceiver = rowReceiver;
        }

        public void sync() {
            if (throwable == null) {
                if (plan == null || analysis == null || rowReceiver == null) {
                    throw new IllegalStateException("sync called, but there was no bind or execute");
                }
                executor.execute(plan, rowReceiver);
            } else {
                Throwable t = this.throwable;
                this.throwable = null;
                throw Throwables.propagate(t);
            }
        }

        public List<? extends DataType> outputTypes() {
            return outputTypes;
        }

        public String query() {
            return query;
        }
    }
}
