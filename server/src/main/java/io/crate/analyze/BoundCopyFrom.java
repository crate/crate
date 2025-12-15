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

import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.function.Predicate;

public class BoundCopyFrom {

    private final DocTableInfo tableInfo;
    @Nullable
    private final String partitionIdent;
    private final List<String> targetColumns;
    private final Settings settings;
    private final Symbol uri;
    private final FileUriCollectPhase.InputFormat inputFormat;
    private final Predicate<DiscoveryNode> nodeFilters;

    public BoundCopyFrom(DocTableInfo tableInfo,
                         @Nullable String partitionIdent,
                         List<String> targetColumns,
                         Settings settings,
                         Symbol uri,
                         FileUriCollectPhase.InputFormat inputFormat,
                         Predicate<DiscoveryNode> nodeFilters) {
        this.tableInfo = tableInfo;
        this.partitionIdent = partitionIdent;
        this.targetColumns = targetColumns;
        this.settings = settings;
        this.uri = uri;
        this.inputFormat = inputFormat;
        this.nodeFilters = nodeFilters;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    @Nullable
    public String partitionIdent() {
        return partitionIdent;
    }

    public List<String> targetColumns() {
        return targetColumns;
    }

    public Settings settings() {
        return settings;
    }

    public FileUriCollectPhase.InputFormat inputFormat() {
        return inputFormat;
    }

    public Symbol uri() {
        return uri;
    }

    public Predicate<DiscoveryNode> nodePredicate() {
        return nodeFilters;
    }
}
