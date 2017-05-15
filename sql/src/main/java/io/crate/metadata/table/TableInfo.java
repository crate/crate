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

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.operation.user.User;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TableInfo extends Iterable<Reference> {

    /**
     * returns information about a column with the given ident.
     * returns null if this table contains no such column.
     */
    @Nullable
    Reference getReference(ColumnIdent columnIdent);

    /**
     * returns the top level columns of this table with predictable order
     */
    Collection<Reference> columns();

    RowGranularity rowGranularity();

    TableIdent ident();

    /**
     * Retrieve the routing for the table
     * <p>
     * The result of this method is non-deterministic for two reasons:
     * <p>
     * 1. Shard selection is randomized for load distribution.
     * <p>
     * 2. The underlying clusterState might change between calls.
     */
    Routing getRouting(WhereClause whereClause, @Nullable String preference);

    List<ColumnIdent> primaryKey();

    Map<String, Object> tableParameters();

    Set<Operation> supportedOperations();

    default Set<User.Role> requiredUserRoles() {
        return ImmutableSet.of();
    }
}
