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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.Node;
import org.elasticsearch.common.inject.Singleton;

import java.util.UUID;

@Singleton
public class KillStatementAnalyzer extends AstVisitor<KillAnalyzedStatement, Analysis> {

    @Override
    public KillAnalyzedStatement visitKillStatement(KillStatement node, Analysis context) {
        KillAnalyzedStatement killAnalyzedStatement;
        if (node.jobId().isPresent()) {
            UUID jobId;
            try {
                jobId = UUID.fromString(ExpressionToStringVisitor
                        .convert(node.jobId().get(), context.parameterContext().parameters()));
            } catch (Exception e) {
                throw new IllegalArgumentException("Can not parse job ID", e);
            }
                killAnalyzedStatement = new KillAnalyzedStatement(jobId);
        } else {
            killAnalyzedStatement = new KillAnalyzedStatement();
        }
        return killAnalyzedStatement;
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return process(node, analysis);
    }
}
