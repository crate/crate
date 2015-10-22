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

import io.crate.exceptions.RepositoryUnknownException;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.DropRepository;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.common.inject.Inject;

public class DropRepositoryStatementAnalyzer extends AstVisitor<DropRepositoryAnalyzedStatement, Analysis> {

    private final ClusterService clusterService;

    @Inject
    public DropRepositoryStatementAnalyzer(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public DropRepositoryAnalyzedStatement visitDropRepository(DropRepository node, Analysis context) {
        String repositoryName = node.repository();
        failIfRepositoryDoesNotExist(repositoryName);
        return new DropRepositoryAnalyzedStatement(repositoryName);
    }

    private void failIfRepositoryDoesNotExist(String repositoryName) {
        RepositoriesMetaData repositories = clusterService.state().metaData().custom(RepositoriesMetaData.TYPE);
        if (repositories == null || repositories.repository(repositoryName) == null) {
            throw new RepositoryUnknownException(repositoryName);
        }
    }

    public AnalyzedStatement analyze(DropRepository node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return process(node, analysis);
    }
}
