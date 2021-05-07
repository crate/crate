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

import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class BoundAddColumn {

    private final DocTableInfo tableInfo;
    private final AnalyzedTableElements<Object> analyzedTableElements;
    private final Settings settings;
    private final Map<String, Object> mapping;
    private final boolean newPrimaryKeys;
    private final boolean hasNewGeneratedColumns;

    public BoundAddColumn(DocTableInfo tableInfo,
                          AnalyzedTableElements<Object> analyzedTableElements,
                          Settings settings,
                          Map<String, Object> mapping,
                          boolean newPrimaryKeys,
                          boolean hasNewGeneratedColumns) {
        this.tableInfo = tableInfo;
        this.analyzedTableElements = analyzedTableElements;
        this.settings = settings;
        this.mapping = mapping;
        this.newPrimaryKeys = newPrimaryKeys;
        this.hasNewGeneratedColumns = hasNewGeneratedColumns;
    }

    public DocTableInfo table() {
        return this.tableInfo;
    }

    public AnalyzedTableElements<Object> analyzedTableElements() {
        return analyzedTableElements;
    }

    public boolean newPrimaryKeys() {
        return this.newPrimaryKeys;
    }

    public boolean hasNewGeneratedColumns() {
        return hasNewGeneratedColumns;
    }

    public Map<String, Object> mapping() {
        return mapping;
    }

    public Settings settings() {
        return settings;
    }
}
