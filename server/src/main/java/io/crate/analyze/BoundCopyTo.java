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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.settings.Settings;
import org.jspecify.annotations.Nullable;

import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;

public class BoundCopyTo {

    private final List<Symbol> outputs;
    private final DocTableInfo table;
    private final WhereClause whereClause;
    private final Symbol uri;
    private final boolean columnsDefined;
    private final WriterProjection.@Nullable CompressionType compressionType;
    private final WriterProjection.@Nullable OutputFormat outputFormat;
    @Nullable
    private final List<String> outputNames;
    /*
     * add values that should be added or overwritten
     * all symbols must normalize to literals on the shard level.
     */
    private final Map<ColumnIdent, Symbol> overwrites;

    private final Settings withClauseOptions;

    public BoundCopyTo(List<Symbol> outputs,
                       DocTableInfo table,
                       WhereClause whereClause,
                       Symbol uri,
                       WriterProjection.@Nullable CompressionType compressionType,
                       WriterProjection.@Nullable OutputFormat outputFormat,
                       @Nullable List<String> outputNames,
                       boolean columnsDefined,
                       @Nullable Map<ColumnIdent, Symbol> overwrites,
                       Settings withClauseOptions) {
        this.outputs = outputs;
        this.table = table;
        this.whereClause = whereClause;
        this.uri = uri;
        this.columnsDefined = columnsDefined;
        this.compressionType = compressionType;
        this.outputNames = outputNames;
        this.outputFormat = outputFormat;
        this.overwrites = Objects.requireNonNullElse(overwrites, Map.of());
        this.withClauseOptions = withClauseOptions;
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    public DocTableInfo table() {
        return table;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public Symbol uri() {
        return uri;
    }

    public boolean columnsDefined() {
        return columnsDefined;
    }

    public WriterProjection.@Nullable CompressionType compressionType() {
        return compressionType;
    }

    public WriterProjection.@Nullable OutputFormat outputFormat() {
        return outputFormat;
    }

    @Nullable
    public List<String> outputNames() {
        return outputNames;
    }

    public Map<ColumnIdent, Symbol> overwrites() {
        return overwrites;
    }

    public Settings withClauseOptions() {
        return withClauseOptions;
    }
}
