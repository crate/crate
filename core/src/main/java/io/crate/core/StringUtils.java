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
import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    public static final Splitter PATH_SPLITTER = Splitter.on('.');
    public static final Joiner PATH_JOINER = Joiner.on('.');

    private static final Pattern PATTERN = Pattern.compile("\\['([^\\]])*'\\]");
    private static final Pattern SQL_PATTERN = Pattern.compile("(.+?)(?:\\['([^\\]])*'\\])+");

    public static String dottedToSqlPath(String dottedPath) {
        Iterable<String> splitted = PATH_SPLITTER.split(dottedPath);
        Iterator<String> iter = splitted.iterator();
        StringBuilder builder = new StringBuilder(iter.next());
        while (iter.hasNext()) {
            builder.append("['").append(iter.next()).append("']");
        }
        return builder.toString();
    }

    public static String sqlToDottedPath(String sqlPath) {
        if (!SQL_PATTERN.matcher(sqlPath).find()) { return sqlPath; }
        List<String> s = new ArrayList<>();
        int idx = sqlPath.indexOf('[');
        s.add(sqlPath.substring(0, idx));
        Matcher m = PATTERN.matcher(sqlPath);
        while (m.find(idx)) {
            String group = m.group(1);
            if (group == null) { group = ""; }
            s.add(group);
            idx = m.end();
        }

        return PATH_JOINER.join(s);
    }

    public static boolean pathListContainsPrefix(Collection<String> pathList, String prefix) {
        for (String elem : pathList) {
            if (elem.startsWith(prefix) &&
                    (elem.length() == prefix.length() ||
                            (elem.length() > prefix.length() && elem.charAt(prefix.length()) == '.'))) {
                return true;
            }
        }
        return false;
    }

    public static @Nullable String getPathByPrefix(Collection<String> pathList, String prefix) {
        for (String elem : pathList) {
            if (elem.startsWith(prefix) &&
                    (elem.length() == prefix.length() ||
                            (elem.length() > prefix.length() && elem.charAt(prefix.length()) == '.'))) {
                return elem;
            }
        }
        return null;
    }

    /**
     * check if a collection of Strings containing dotted paths contains at least one element
     * beginning with <code>prefix</code>, which consists of one or more complete path elements
     *
     * e.g. <code>StringUtils.pathListContainsPrefix(["a", "ba.cv"], "ba")</code> returns <code>true</code>
     *      but <code>StringUtils.pathListContainsPrefix(["a", "ba.cv"], "b")</code> returns <code>false</code>
     * @param pathCollection a collection of Strings containing dotted paths
     * @param prefix the prefix to search for
     * @return true if at least one element in the list has <code>prefix</code> as prefix, false otherwise
     */
    public static boolean pathListContainsPrefix(Collection<String> pathCollection, String prefix) {
        for (String elem : pathCollection) {
            if (elem.startsWith(prefix) &&
                    (elem.length() == prefix.length() ||
                            (elem.length() > prefix.length() && elem.charAt(prefix.length()) == '.'))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the first path from a collection of Strings containing dotted paths
     * beginning with <code>prefix</code>, which consists of one or more complete path elements
     * @param pathCollection a collection of Strings containing dotted paths
     * @param prefix the prefix to search for
     * @return the found path or null
     */
    public static @Nullable
    String getPathByPrefix(Collection<String> pathCollection, String prefix) {
        for (String elem : pathCollection) {
            if (elem.startsWith(prefix) &&
                    (elem.length() == prefix.length() ||
                            (elem.length() > prefix.length() && elem.charAt(prefix.length()) == '.'))) {
                return elem;
            }
        }
        return null;
    }
}
