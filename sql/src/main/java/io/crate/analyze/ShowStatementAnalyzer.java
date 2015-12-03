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

package io.crate.analyze;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class ShowStatementAnalyzer {

    private final SelectStatementAnalyzer selectStatementAnalyzer;

    @Inject
    public ShowStatementAnalyzer(SelectStatementAnalyzer selectStatementAnalyzer) {
        this.selectStatementAnalyzer = selectStatementAnalyzer;
    }

    public AnalyzedStatement analyze(ShowSchemas node, Analysis analysis) {
        /**
         * <code>
         *     SHOW SCHEMAS [ LIKE 'pattern' ];
         * </code>
         * needs to be rewritten to select query:
         * <code>
         *     SELECT schema_name
         *     FROM information_schema.schemata
         *     [ WHERE schema_name LIKE 'pattern' ]
         *     ORDER BY schema_name;
         * </code>
         */
        StringBuilder sb = new StringBuilder("SELECT schema_name ");
        sb.append("FROM information_schema.schemata ");
        if (node.likePattern().isPresent()) {
            sb.append(String.format("WHERE schema_name LIKE '%s' ", node.likePattern().get()));
        } else if (node.whereExpression().isPresent()) {
            sb.append(String.format("WHERE %s ", node.whereExpression().get().toString()));
        }
        sb.append("ORDER BY schema_name");
        Statement statement = SqlParser.createStatement(sb.toString());
        return selectStatementAnalyzer.visitQuery((Query) statement, analysis);
    }

}
