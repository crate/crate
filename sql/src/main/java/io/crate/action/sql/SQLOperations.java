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

import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.executor.Executor;
import io.crate.operation.projectors.RowReceiver;
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

    public class Session {

        private final Executor executor;
        private final String defaultSchema;
        private final UUID jobId;

        private List<DataType> paramTypes;
        private Statement statement;
        private Analysis analysis;
        private Plan plan;
        private RowReceiver rowReceiver;
        private List<DataType> outputTypes;
        private String query;

        private Session(Executor executor, String defaultSchema) {
            this.executor = executor;
            this.defaultSchema = defaultSchema;
            this.jobId = UUID.randomUUID();
        }

        public void parse(String statementName, String query, List<DataType> paramTypes) {
            this.query = query;
            this.statement = SqlParser.createStatement(query);
            this.paramTypes = paramTypes;
        }

        public DataType getParamType(int idx) {
            return paramTypes.get(idx);
        }

        public void bind(String portalName, String statementName, List<Object> params) {
            if (statement == null) {
                throw new IllegalStateException("bind called without having a parsed statement");
            }
            analysis = analyzer.analyze(statement, new ParameterContext(params.toArray(new Object[0]), EMPTY_BULK_ARGS, defaultSchema));
            plan = planner.plan(analysis, jobId);
        }

        public List<Field> describe(char type, String portalOrStatement) {
            if (analysis == null) {
                throw new IllegalStateException("describe called, but there was no bind() call");
            }
            if (analysis.rootRelation() == null) {
                return null;
            }
            List<Field> fields = analysis.rootRelation().fields();
            outputTypes = Symbols.extractTypes(fields);
            return fields;
        }

        public void execute(String portalName, int maxRows, RowReceiver rowReceiver) {
            this.rowReceiver = rowReceiver;
            if (plan == null || analysis == null) {
                throw new IllegalStateException("execute called without plan/analysis");
            }
        }

        public void sync() {
            if (plan == null || analysis == null || rowReceiver == null) {
                throw new IllegalStateException("sync called, but there was no bind or execute");
            }
            executor.execute(plan, rowReceiver);
        }

        public List<? extends DataType> outputTypes() {
            return outputTypes;
        }

        public String query() {
            return query;
        }
    }
}
