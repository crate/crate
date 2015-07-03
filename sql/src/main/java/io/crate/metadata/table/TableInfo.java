/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.table;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public interface TableInfo extends Iterable<ReferenceInfo> {

    /**
     * Because {@link java.util.TreeMap} does not support <code>null</code> keys,
     * we use a placeholder(empty) string instead.
     */
    public static final String NULL_NODE_ID = "";
    public static final Predicate<String> IS_NOT_NULL_NODE_ID = Predicates.not(Predicates.equalTo(TableInfo.NULL_NODE_ID));

    /**
     * the schemaInfo for the schema that contains this table.
     */
    public SchemaInfo schemaInfo();

    /**
     * returns information about a column with the given ident.
     * returns null if this table contains no such column.
     */
    @Nullable
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent);

    /**
     * returns the top level columns of this table with predictable order
     */
    public Collection<ReferenceInfo> columns();

    public RowGranularity rowGranularity();

    public TableIdent ident();

    public Routing getRouting(WhereClause whereClause, @Nullable String preference);

    public List<ColumnIdent> primaryKey();

    public ImmutableMap<String, Object> tableParameters();

}
