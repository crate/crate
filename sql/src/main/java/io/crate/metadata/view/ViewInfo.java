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

package io.crate.metadata.view;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.Operation;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ViewInfo implements RelationInfo {

    private final TableIdent ident;
    private final List<Reference> columns;

    ViewInfo(TableIdent ident, List<Reference> columns) {
        this.ident = ident;
        this.columns = columns;
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
    public TableIdent ident() {
        return ident;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> parameters() {
        return Collections.emptyMap();
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.READ_ONLY;
    }

    @Override
    public RelationType relationType() {
        return RelationType.BASE_VIEW;
    }

    @Override
    public Iterator<Reference> iterator() {
        return columns.iterator();
    }

    @Override
    public String toString() {
        return ident.fqn();
    }
}
