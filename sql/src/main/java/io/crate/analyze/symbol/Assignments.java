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

package io.crate.analyze.symbol;

import io.crate.metadata.Reference;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nonnull;
import java.util.Map;

public class Assignments {

    /**
     * convert assignments into a tuple of fqn column names and the symbols.
     * <p>
     * <pre>
     *     {
     *         users.age:  users.age + 1,
     *         users.name: users.name || 'foo'
     *
     *     }
     * </pre>
     * becomes
     * <pre>
     *     ( [users.age, users.name], [users.age + 1, users.name || 'foo'] )
     * </pre>
     *
     * @return a tuple or null if the input is null.
     */
    public static Tuple<String[], Symbol[]> convert(@Nonnull Map<Reference, ? extends Symbol> assignments) {
        String[] assignmentColumns = new String[assignments.size()];
        Symbol[] assignmentSymbols = new Symbol[assignments.size()];
        int i = 0;
        for (Map.Entry<Reference, ? extends Symbol> entry : assignments.entrySet()) {
            Reference key = entry.getKey();
            assignmentColumns[i] = key.ident().columnIdent().fqn();
            assignmentSymbols[i] = entry.getValue();
            i++;
        }
        return new Tuple<>(assignmentColumns, assignmentSymbols);
    }
}
