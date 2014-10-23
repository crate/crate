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

package io.crate.autocomplete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AutoCompleterTest {

    AutoCompleter completer;

    @Before
    public void setUp() throws Exception {
        completer = new AutoCompleter(new DummyDataProvider());
    }

    @Test
    public void testStartKeywordCompletion() throws Exception {
        List<String> completions = Lists.newArrayList(completer.complete("sel").get().completions());
        assertThat(completions.get(0), is("select"));

        completions = Lists.newArrayList(completer.complete("up").get().completions());
        assertThat(completions.get(0), is("update"));

        completions = Lists.newArrayList(completer.complete("del").get().completions());
        assertThat(completions.get(0), is("delete"));

        completions = Lists.newArrayList(completer.complete("d").get().completions());
        assertThat(completions, Matchers.containsInAnyOrder("delete", "drop"));

        completions = Lists.newArrayList(completer.complete("s").get().completions());
        assertThat(completions, Matchers.containsInAnyOrder("set", "select"));
    }

    @Test
    public void testColumnCompletion() throws Exception {
        Collection<String> completions = completer.complete("select in").get().completions();
        assertThat(completions, Matchers.contains("information_schema"));
    }

    @Test
    public void testTableCompletionWithoutSchema() throws Exception {
        Collection<String> completions = completer.complete("select name from use").get().completions();
        assertThat(completions, Matchers.contains("users"));
    }

    @Test
    public void testTableCompletionWithSchema() throws Exception {
        Collection<String> completions = completer.complete("select name from doc.t").get().completions();
        assertThat(completions, Matchers.contains("doc.tags"));
    }

    @Test
    public void testCompleteSchemaTableOrColumn() throws Exception {
        Collection<String> completions = completer.complete("select s").get().completions();
        assertThat(completions, Matchers.containsInAnyOrder("sys", "shards", "schema_name"));
    }

    @Test
    public void testCompleteColumnWithTable() throws Exception {
        Collection<String> completions = completer.complete("select posts.t").get().completions();
        assertThat(completions, Matchers.containsInAnyOrder("posts.title"));
    }

    @Test
    public void testCompleteColumnWithSchemaAndTable() throws Exception {
        CompletionResult completionResult = completer.complete("select sys.\"shards\".t").get();
        assertThat(completionResult.completions(), Matchers.containsInAnyOrder("sys.\"shards\".table_name"));
        assertThat(completionResult.startIdx(), is(7));
    }

    @Test
    public void testCompleteColumnInWhereWithSchemaAndTable() throws Exception {
        CompletionResult completionResult = completer.complete("select * from sys.nodes where p").get();
        assertThat(completionResult.completions(), Matchers.containsInAnyOrder(
            "port['http']", "port['transport']"));
    }

    @Test
    public void testColumnCompletionInWhereClauseWithTable() throws Exception {
        Collection<String> completions = completer.complete("select name from users where n").get().completions();
        assertThat(completions, Matchers.contains("name"));
    }

    @Test
    public void testCompleteSubscriptExpression() throws Exception {
        Collection<String> completions = completer.complete("select por").get().completions();
        assertThat(completions, Matchers.contains("port['http']", "port['transport']"));
    }

    @Test
    public void testCompleteSubscriptExpressionAtOpeningBracket() throws Exception {
        Collection<String> completions = completer.complete("select port[").get().completions();
        assertThat(completions, Matchers.contains("port['http']", "port['transport']"));
    }

    @Test
    public void testCompleteSubscriptExpressionAtSingleQuote() throws Exception {
        Collection<String> completions = completer.complete("select port['").get().completions();
        assertThat(completions, Matchers.contains("port['http']", "port['transport']"));
    }

    @Test
    public void testCompleteSubscriptExpressionAfterSingleQuote() throws Exception {
        Collection<String> completions = completer.complete("select port['h").get().completions();
        assertThat(completions, Matchers.contains("port['http']"));
    }

    @Test
    public void testCompleteSubscriptExpressionClosingSingleQuote() throws Exception {
        Collection<String> completions = completer.complete("select port['http'").get().completions();
        assertThat(completions, Matchers.contains("port['http']"));
    }

    @Test
    public void testCompleteNestedSubscriptSecondOpeningBracket() throws Exception {
        Collection<String> completions = completer.complete("select network['tcp'][").get().completions();
        assertThat(completions, Matchers.contains("network['tcp']['connections']"));
    }

    @Test
    public void testCompleteNestedSubscriptSecondOpeningSingleQuote() throws Exception {
        Collection<String> completions = completer.complete("select network['tcp']['").get().completions();
        assertThat(completions, Matchers.contains("network['tcp']['connections']"));
    }

    @Test
    public void testCompleteColumnNameInsideFunction() throws Exception {
        CompletionResult completionResult = completer.complete("select format(nam").get();
        assertThat(completionResult.completions(), Matchers.containsInAnyOrder("name"));
    }

    @Test
    public void testCompleteNestedSubscriptWithTable() throws Exception {
        CompletionResult completionResult = completer.complete("select nodes.network['tcp']['").get();
        assertThat(completionResult.completions(), Matchers.contains("nodes.network['tcp']['connections']"));
        assertThat(completionResult.startIdx(), is(7));
    }

    public static class DummyDataProvider implements DataProvider {

        public static final List<String> SCHEMAS = ImmutableList.<String>builder()
                .add("sys")
                .add("doc")
                .add("information_schema").build();

        public static final ImmutableMap<String, List<String>> TABLE_MAP = ImmutableMap.<String, List<String>>builder()
                .put("doc", Arrays.asList("users", "posts", "tags"))
                .put("sys", Arrays.asList("shards", "nodes"))
                .put("information_schema", Arrays.asList("tables")).build();

        public static final Map<String, Map<String, List<String>>> COLUMNS_MAP = ImmutableMap.<String, Map<String, List<String>>>builder()
                .put("doc", ImmutableMap.of(
                     "users", Arrays.asList("id", "name", "birthday"),
                     "posts", Arrays.asList("id", "title", "content"),
                     "tags", Arrays.asList("id", "name"))
                )
                .put("information_schema", ImmutableMap.of(
                        "tables", Arrays.asList("schema_name", "table_name"))
                )
                .put("sys", ImmutableMap.of(
                        "nodes", Arrays.asList("port['http']", "port['transport']", "network['tcp']['connections']"),
                        "shards", Arrays.asList("id", "table_name"))
                ).build();

        @Override
        public ListenableFuture<List<String>> schemas(@Nullable String prefix) {
            if (prefix == null) {
                return Futures.immediateFuture(SCHEMAS);
            }
            List<String> result = new ArrayList<>();
            for (String schema : SCHEMAS) {
                if (schema.startsWith(prefix)) {
                    result.add(schema);
                }
            }
            return Futures.immediateFuture(result);
        }

        @Override
        public ListenableFuture<List<String>> tables(@Nullable String schema, @Nullable String prefix) {
            Set<String> result = new HashSet<>();
            if (schema == null) {
                for (List<String> tables : TABLE_MAP.values()) {
                    for (String table : tables) {
                        if (prefix == null) {
                            result.add(table);
                        } else if (table.startsWith(prefix)) {
                            result.add(table);
                        }
                    }
                }
                return Futures.immediateFuture((List<String>) Lists.newArrayList(result));
            }
            List<String> tables = TABLE_MAP.get(schema);
            if (tables == null) {
                return Futures.immediateFuture((List<String>) ImmutableList.<String>of());
            }
            if (prefix == null) {
                for (String table : tables) {
                    result.add(table);
                }
                return Futures.immediateFuture(tables);
            }
            for (String table : tables) {
                if (table.startsWith(prefix)) {
                    result.add(table);
                }
            }
            return Futures.immediateFuture((List<String>) Lists.newArrayList(result));
        }

        @Override
        public ListenableFuture<List<String>> columns(@Nullable String schema, @Nullable String table, @Nullable String prefix) {
            List<String> columns = new ArrayList<>();

            Collection<Map<String, List<String>>> tableColumns;

            if (schema == null) {
                tableColumns = COLUMNS_MAP.values();
            } else {
                Map<String, List<String>> tableColumnMap = COLUMNS_MAP.get(schema);
                if (tableColumnMap == null) {
                    return Futures.immediateFuture(columns);
                }
                tableColumns = Arrays.asList(tableColumnMap);
            }

            if (table == null) {
                for (Map<String, List<String>> tableColumn : tableColumns) {
                    for (List<String> cols : tableColumn.values()) {
                        if (prefix == null) {
                            columns.addAll(cols);
                        } else {
                            for (String col : cols) {
                                if (col.startsWith(prefix)) {
                                    columns.add(col);
                                }
                            }
                        }
                    }
                }
            } else {
                for (Map<String, List<String>> tableColumn : tableColumns) {
                    List<String> cols = tableColumn.get(table);
                    if (cols == null) {
                        continue;
                    }
                    if (prefix == null) {
                        columns.addAll(cols);
                        continue;
                    }
                    for (String col : cols) {
                        if (col.startsWith(prefix)) {
                            columns.add(col);
                        }
                    }

                }
            }
            return Futures.immediateFuture(columns);
        }
    }
}