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
import java.util.Set;
import java.util.function.Consumer;

import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

public class AnalyzedRestoreSnapshot implements DDLStatement {

    private final String repository;
    private final String snapshot;
    private final List<Table<Symbol>> tables;
    private final boolean includeTables;
    private final boolean includeCustomMetadata;
    private final Set<String> customMetadataTypes;
    private final boolean includeGlobalSettings;
    private final List<String> globalSettings;
    private final GenericProperties<Symbol> properties;

    AnalyzedRestoreSnapshot(String repository,
                            String snapshot,
                            List<Table<Symbol>> tables,
                            boolean includeTables,
                            boolean includeCustomMetadata,
                            Set<String> customMetadataTypes,
                            boolean includeGlobalSettings,
                            List<String> globalSettings,
                            GenericProperties<Symbol> properties) {
        this.repository = repository;
        this.snapshot = snapshot;
        this.tables = tables;
        this.includeTables = includeTables;
        this.includeCustomMetadata = includeCustomMetadata;
        this.customMetadataTypes = customMetadataTypes;
        this.includeGlobalSettings = includeGlobalSettings;
        this.globalSettings = globalSettings;
        this.properties = properties;
    }

    public String repository() {
        return repository;
    }

    public String snapshot() {
        return snapshot;
    }

    public List<Table<Symbol>> tables() {
        return tables;
    }

    public boolean includeTables() {
        return includeTables;
    }

    public boolean includeCustomMetadata() {
        return includeCustomMetadata;
    }

    public Set<String> customMetadataTypes() {
        return customMetadataTypes;
    }

    public boolean includeGlobalSettings() {
        return includeGlobalSettings;
    }

    public List<String> globalSettings() {
        return globalSettings;
    }

    public GenericProperties<Symbol> properties() {
        return properties;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var table : tables) {
            for (Assignment<Symbol> partitionProperty : table.partitionProperties()) {
                partitionProperty.expressions().forEach(consumer);
            }
        }
        properties.forValues(consumer);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitRestoreSnapshotAnalyzedStatement(this, context);
    }
}
