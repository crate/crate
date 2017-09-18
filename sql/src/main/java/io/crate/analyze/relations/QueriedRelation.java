/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.relations;

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;

public interface QueriedRelation extends AnalyzedRelation {

    /**
     * The long-/midterm goal is to deprecate the QuerySpec;
     *
     * The other properties should be used instead.
     */
    QuerySpec querySpec();

    /**
     * @return The outputs of the relation
     */
    default List<Symbol> outputs() {
        return querySpec().outputs();
    }

    /**
     * @return WHERE clause of the relation.
     *         This is {@link WhereClause#MATCH_ALL} if there was no WhereClause in the statement
     */
    default WhereClause where() {
        return querySpec().where();
    }

    /**
     * @return The GROUP BY keys. Empty if there are none.
     */
    default List<Symbol> groupBy() {
        return querySpec().groupBy();
    }

    /**
     * @return The HAVING clause or null
     */
    @Nullable
    default HavingClause having() {
        return querySpec().having();
    }

    /**
     * @return ORDER BY clause or null if not present
     */
    @Nullable
    default OrderBy orderBy() {
        return querySpec().orderBy();
    }

    @Nullable
    default Symbol limit() {
        return querySpec().limit();
    }

    @Nullable
    default Symbol offset() {
        return querySpec().offset();
    }
}
