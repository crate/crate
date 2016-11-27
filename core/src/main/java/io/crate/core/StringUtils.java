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

package io.crate.core;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class StringUtils {

    public static final Splitter PATH_SPLITTER = Splitter.on('.');
    public static final Joiner PATH_JOINER = Joiner.on('.');

    /**
     * Return the common ancestors of a list of fields.<br>
     * A field is a string that can use the dotted-notation to indicate nesting.<br>
     * <p>
     * <pre>
     * fields:  [ "a", "a.b", "b.c", "b.c.d"]
     * returns: [ "a", "b.c" ]
     * </pre>
     *
     * @param fields a list of strings where each string may contain dots as its separator
     * @return a list of strings with only the common ancestors.
     */
    public static Set<String> commonAncestors(List<String> fields) {
        int idx = 0;
        String previous = null;

        Collections.sort(fields);
        Set<String> result = new HashSet<>(fields.size());
        for (String field : fields) {
            if (idx > 0) {
                if (!field.startsWith(previous + '.')) {
                    previous = field;
                    result.add(field);
                }
            } else {
                result.add(field);
                previous = field;
            }
            idx++;
        }
        return result;
    }
}
