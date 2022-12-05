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

package io.crate.analyze;

import io.crate.execution.ddl.RepositoryService;
import io.crate.sql.tree.DropSnapshot;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;

@Singleton
class DropSnapshotAnalyzer {

    private final RepositoryService repositoryService;

    DropSnapshotAnalyzer(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public AnalyzedDropSnapshot analyze(DropSnapshot node) {
        List<String> parts = node.name().getParts();
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Snapshot name not supported, only <repository>.<snapshot> works.");
        }
        String repositoryName = parts.get(0);
        repositoryService.failIfRepositoryDoesNotExist(repositoryName, true);

        return new AnalyzedDropSnapshot(repositoryName, parts.get(1));
    }
}
