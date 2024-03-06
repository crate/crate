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

package io.crate.metadata.view;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.common.settings.Settings;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.table.Operation;

public class ViewInfo implements RelationInfo {

    private final RelationName ident;
    private final String definition;
    private final List<Reference> columns;
    private final String owner;
    private final SearchPath searchPath;

    @VisibleForTesting
    public ViewInfo(RelationName ident, String definition, List<Reference> columns, @Nullable String owner, SearchPath searchPath) {
        this.ident = ident;
        this.definition = definition;
        this.columns = columns;
        this.owner = owner;
        this.searchPath = searchPath;
    }

    @Override
    public Collection<Reference> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return ident;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return Collections.emptyList();
    }

    @Override
    public Settings parameters() {
        return Settings.EMPTY;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.READ_ONLY;
    }

    @Override
    public RelationType relationType() {
        return RelationType.VIEW;
    }

    @Override
    public Iterator<Reference> iterator() {
        return columns.iterator();
    }

    @Override
    public String toString() {
        return ident.fqn();
    }

    public String definition() {
        return definition;
    }

    @Nullable
    public String owner() {
        return owner;
    }

    public SearchPath searchPath() {
        return searchPath;
    }
}
