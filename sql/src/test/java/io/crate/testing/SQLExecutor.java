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

package io.crate.testing;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.TypeSettings;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.RepositoryService;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.sql.parser.SqlParser;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.crate.analyze.BaseAnalyzerTest.*;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.mock;

/**
 * Lightweight alternative to {@link SQLTransportExecutor}.
 *
 * Can be used for unit-tests tests which don't require the full execution-layer/nodes to be started.
 */
public class SQLExecutor {

    private final Analyzer analyzer;

    public static class Builder {

        private final ClusterService clusterService;
        private final Map<String, SchemaInfo> schemas = new HashMap<>();
        private final Map<TableIdent, DocTableInfo> docTables = new HashMap<>();

        public Builder(ClusterService clusterService) {
            this.clusterService = clusterService;
            schemas.put("sys", new SysSchemaInfo(clusterService));
            schemas.put("information_schema", new InformationSchemaInfo(clusterService));
        }

        public Builder enableDefaultTables() {
            // we should try to reduce the number of tables here eventually...
            addDocTable(USER_TABLE_INFO);
            addDocTable(USER_TABLE_INFO_CLUSTERED_BY_ONLY);
            addDocTable(USER_TABLE_INFO_MULTI_PK);
            addDocTable(DEEPLY_NESTED_TABLE_INFO);
            addDocTable(TEST_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_MULTIPLE_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_DOC_TRANSACTIONS_TABLE_INFO);
            addDocTable(TEST_DOC_LOCATIONS_TABLE_INFO);
            addDocTable(TEST_CLUSTER_BY_STRING_TABLE_INFO);
            addDocTable(T3.T1_INFO);
            addDocTable(T3.T2_INFO);
            addDocTable(T3.T3_INFO);
            return this;
        }

        public SQLExecutor build() {
            schemas.put("doc", new DocSchemaInfo(clusterService, new TestingDocTableInfoFactory(docTables)));
            return new SQLExecutor(new Analyzer(
                Settings.EMPTY,
                new ReferenceInfos(
                    schemas,
                    clusterService,
                    new DocSchemaInfoFactory(new TestingDocTableInfoFactory(Collections.<TableIdent, DocTableInfo>emptyMap()))
                ),
                getFunctions(),
                clusterService,
                new IndicesAnalysisService(Settings.EMPTY),
                new RepositoryService(
                    clusterService,
                    mock(TransportDeleteRepositoryAction.class),
                    mock(TransportPutRepositoryAction.class)
                ),
                new RepositoryParamValidator(Collections.<String, TypeSettings>emptyMap())
            ));
        }

        public Builder addSchema(SchemaInfo schema) {
            schemas.put(schema.name(), schema);
            return this;
        }

        public Builder addDocTable(DocTableInfo table) {
            docTables.put(table.ident(), table);
            return this;
        }
    }

    public static Builder builder(ClusterService clusterService) {
        return new Builder(clusterService);
    }

    private SQLExecutor(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public <T> T analyze(String statement) {
        return analyze(statement, new Object[0]);
    }

    public <T> T analyze(String statement, Object[] arguments) {
        ParameterContext parameterContext = arguments.length == 0 ? ParameterContext.EMPTY
            : new ParameterContext(new RowN(arguments), Collections.<Row>emptyList());
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(statement), SessionContext.SYSTEM_SESSION, parameterContext);
        //noinspection unchecked
        return (T) analysis.analyzedStatement();
    }
}
