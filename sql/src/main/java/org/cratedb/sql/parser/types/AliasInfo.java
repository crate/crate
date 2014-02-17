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

/* The original from which this derives bore the following: */

package org.cratedb.sql.parser.types;

/**
 *
 * An interface for describing an alias in Derby systems.
 * 
 * In a Derby system, an alias can be one of the following:
 * <ul>
 * <li>method alias
 * <li>UDT alias
 * <li>class alias
 * <li>synonym
 * <li>user-defined aggregate
 * </ul>
 *
 */
public interface AliasInfo
{
    public enum Type { UDT, PROCEDURE, FUNCTION, SYNONYM };

    /**
     * Get the name of the static method that the alias 
     * represents at the source database.    (Only meaningful for
     * method aliases )
     *
     * @return The name of the static method that the alias 
     * represents at the source database.
     */
    public String getMethodName();

    /**
     * Return true if this alias is a Table Function.
     */
    public boolean isTableFunction();

}
