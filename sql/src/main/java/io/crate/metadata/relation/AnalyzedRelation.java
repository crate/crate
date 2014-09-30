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

package io.crate.metadata.relation;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexReferenceInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.DynamicReference;

import javax.annotation.Nullable;
import java.util.List;

public interface AnalyzedRelation {

    public List<AnalyzedRelation> children();

    /**
     * return the number of relations this one is made up
     * including itself
     */
    public int numRelations();

    /**
     * returns the referenceInfo for a given columnIdent or null if the table doesn't contain that column
     */
    @Nullable
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent);

    @Nullable
    public IndexReferenceInfo getIndexReferenceInfo(ColumnIdent columnIdent);

    /**
     * A list of tables this relation references.
     * If this is itself a table, it returns an empty list.
     */
    public List<TableInfo> tables();

    public <C, R> R accept(RelationVisitor<C, R> relationVisitor, C context);

    /**
     * <p>
     * This method returns true if a relation can be addressed by a tableName or alias
     * </p>
     *
     * <p>
     * <b>Example</b>:<br />
     *  join relation: <code>join(cross_join, a, b)</code> can resolve both tableNames "a" and "b"
     * </p>
     *
     * <p>
     * <b>Example</b>: <br />
     *   sub select: <code>select a.name from (select "firstName" as name from users) a</code>
     *   <br /><br />
     *   In this case the AliasedAnalyzedRelation (alias=a, child=...) can resolve to alias "a"
     * </p>
     *
     * Implementations should only resolve to one level (so the second example wouldn't resolve to users)
     *
     * @param relationName tableName or alias
     * @return true or false
     */
    boolean addressedBy(String relationName);

    /**
     * returns true if the relation resolves to schemaName and tableName
     *
     * See also {@link #addressedBy(String)}
     */
    boolean addressedBy(@Nullable String schemaName, String tableName);

    /**
     * returns a DynamicReference if the relation supports it
     *
     * @throws io.crate.exceptions.ColumnUnknownException in case the relation doesn't support DynamicReferences
     */
    DynamicReference dynamicReference(ColumnIdent columnIdent) throws ColumnUnknownException;
}
