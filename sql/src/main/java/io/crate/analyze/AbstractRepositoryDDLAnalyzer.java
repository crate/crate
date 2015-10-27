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

import io.crate.exceptions.RepositoryAlreadyExistsException;
import io.crate.exceptions.RepositoryUnknownException;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Node;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Nullable;

public class AbstractRepositoryDDLAnalyzer<R extends AbstractDDLAnalyzedStatement, N extends Node> extends DefaultTraversalVisitor<R, Analysis> {

    private final ClusterService clusterService;

    public AbstractRepositoryDDLAnalyzer(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Nullable
    protected RepositoryMetaData repository(String repositoryName) {
        RepositoriesMetaData repositories = clusterService.state().metaData().custom(RepositoriesMetaData.TYPE);
        if (repositories != null) {
            return repositories.repository(repositoryName);
        }
        return null;
    }

    protected boolean repositoryExists(String repositoryName) {
        return repository(repositoryName) != null;
    }

    protected void failIfRepositoryDoesNotExist(String repositoryName) {
        if (!repositoryExists(repositoryName)) {
            throw new RepositoryUnknownException(repositoryName);
        }
    }

    protected void failIfRepositoryExists(String repositoryName) {
        if (repositoryExists(repositoryName)) {
            throw new RepositoryAlreadyExistsException(repositoryName);
        }
    }

    public AnalyzedStatement analyze(N node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return process(node, analysis);
    }
}
