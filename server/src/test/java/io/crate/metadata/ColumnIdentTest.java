/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import io.crate.exceptions.InvalidColumnNameException;
import io.crate.metadata.doc.SysColumns;

public class ColumnIdentTest {

    @Test
    public void testSqlFqn() throws Exception {
        ColumnIdent ident = ColumnIdent.of("foo", Arrays.asList("x", "y", "z"));
        assertThat(ident.sqlFqn()).isEqualTo("foo['x']['y']['z']");

        ident = ColumnIdent.of("a");
        assertThat(ident.sqlFqn()).isEqualTo("a");

        ident = ColumnIdent.of("a", Collections.singletonList(""));
        assertThat(ident.sqlFqn()).isEqualTo("a['']");

        ident = ColumnIdent.of("a.b", Collections.singletonList("c"));
        assertThat(ident.sqlFqn()).isEqualTo("a.b['c']");
    }

    @Test
    public void testShiftRight() throws Exception {
        assertThat(ColumnIdent.of("foo", "bar").shiftRight()).isEqualTo(ColumnIdent.of("bar"));
        assertThat(ColumnIdent.of("foo", Arrays.asList("x", "y", "z")).shiftRight()).isEqualTo(ColumnIdent.of("x", Arrays.asList("y", "z")));
        assertThat(ColumnIdent.of("foo").shiftRight()).isNull();
    }

    @Test
    public void testIsChildOf() throws Exception {
        ColumnIdent root = ColumnIdent.of("root");
        ColumnIdent rootX = ColumnIdent.of("root", "x");
        ColumnIdent rootXY = ColumnIdent.of("root", Arrays.asList("x", "y"));
        ColumnIdent rootYX = ColumnIdent.of("root", Arrays.asList("y", "x"));

        assertThat(root.isChildOf(root)).isFalse();

        assertThat(rootX.isChildOf(root)).isTrue();
        assertThat(rootXY.isChildOf(root)).isTrue();
        assertThat(rootXY.isChildOf(rootX)).isTrue();

        assertThat(rootYX.isChildOf(root)).isTrue();
        assertThat(rootYX.isChildOf(rootX)).isFalse();
    }

    @Test
    public void testPrepend() throws Exception {
        ColumnIdent foo = ColumnIdent.of("foo");
        assertThat(foo.prepend(SysColumns.DOC.name())).isEqualTo(ColumnIdent.of(SysColumns.DOC.name(), "foo"));

        ColumnIdent fooBar = ColumnIdent.of("foo", "bar");
        assertThat(fooBar.prepend("x")).isEqualTo(ColumnIdent.of("x", Arrays.asList("foo", "bar")));
    }

    @Test
    public void test_get_parents() throws Exception {
        assertThat(ColumnIdent.of("foo").parents()).hasSize(0);
        assertThat(ColumnIdent.of("foo", "x").parents()).containsExactly(
            ColumnIdent.of("foo")
        );

        assertThat(ColumnIdent.of("foo", List.of("x", "y")).parents()).containsExactly(
            ColumnIdent.of("foo", "x"),
            ColumnIdent.of("foo")
        );

        assertThat(ColumnIdent.of("foo", List.of("x", "y", "z")).parents()).containsExactly(
            ColumnIdent.of("foo", List.of("x", "y")),
            ColumnIdent.of("foo", "x"),
            ColumnIdent.of("foo")
        );
    }

    @Test
    public void testValidColumnNameValidation() throws Exception {
        // Allowed.
        ColumnIdent.fromNameSafe("valid", List.of());
        ColumnIdent.fromNameSafe("field_name_", List.of());
        ColumnIdent.fromNameSafe("_Name", List.of());
        ColumnIdent.fromNameSafe("_name_", List.of());
        ColumnIdent.fromNameSafe("__name", List.of());
        ColumnIdent.fromNameSafe("____name", List.of());
        ColumnIdent.fromNameSafe("_name__", List.of());
        ColumnIdent.fromNameSafe("_name1", List.of());
        ColumnIdent.fromNameSafe("'index'", List.of());
        ColumnIdent.fromNameSafe("ident'index", List.of());
        ColumnIdent.fromNameSafe("1'", List.of());
    }

    @Test
    public void testIllegalColumnNameValidation() throws Exception {
        assertNameValidationThrows(".name", "\".name\" contains a dot");
        assertNameValidationThrows("column.name", "\"column.name\" contains a dot");
        assertNameValidationThrows(".", "\".\" contains a dot");
        assertNameValidationThrows("_a", "\"_a\" conflicts with system column pattern");
        assertNameValidationThrows("_name", "\"_name\" conflicts with system column pattern");
        assertNameValidationThrows("_field_name", "\"_field_name\" conflicts with system column pattern");
        assertNameValidationThrows("ident['index']", "\"ident['index']\" conflicts with subscript pattern, square brackets are not allowed");
        assertNameValidationThrows("ident['index]", "\"ident['index]\" conflicts with subscript pattern, square brackets are not allowed");
        assertNameValidationThrows("ident[0]", "\"ident[0]\" conflicts with subscript pattern, square brackets are not allowed");
        assertNameValidationThrows("\"a[1]\"", "\"\"a[1]\"\" conflicts with subscript pattern, square brackets are not allowed");
        assertNameValidationThrows("\"fda_32$@%^nf[ffDA&^\"", "\"\"fda_32$@%^nf[ffDA&^\"\" conflicts with subscript pattern, square brackets are not allowed");
        assertNameValidationThrows("[", "\"[\" conflicts with subscript pattern, square brackets are not allowed");
        assertNameValidationThrows("no\tpe", "\"no\tpe\" contains illegal whitespace character");
    }

    /**
     * Function that tests if validation is throwing an IllegalArgumentException with
     * given expected Message.
     *
     * @param columnName      the column name which causes an exception to be thrown
     * @param expectedMessage exception message that is expected
     */
    private void assertNameValidationThrows(String columnName, String expectedMessage) {
        assertThatThrownBy(() -> ColumnIdent.fromNameSafe(columnName, List.of()))
            .isExactlyInstanceOf(InvalidColumnNameException.class)
            .hasMessage(expectedMessage);
    }

    @Test
    public void test_replace_paths() {
        var a = ColumnIdent.of("a");
        var b = ColumnIdent.of("b");
        var aa = ColumnIdent.of("a", List.of("a"));
        var ba = ColumnIdent.of("b", List.of("a"));
        var ab = ColumnIdent.of("a", List.of("b"));
        var bb = ColumnIdent.of("b", List.of("b"));
        var aaa = ColumnIdent.of("a", List.of("a", "a"));
        var baa = ColumnIdent.of("b", List.of("a", "a"));
        var aba = ColumnIdent.of("a", List.of("b", "a"));
        var aab = ColumnIdent.of("a", List.of("a", "b"));

        assertThat(a.replacePrefix(a)).isEqualTo(a);
        assertThat(a.replacePrefix(b)).isEqualTo(b);
        assertThatThrownBy(() -> a.replacePrefix(ab)).isExactlyInstanceOf(AssertionError.class);

        assertThat(aa.replacePrefix(a)).isEqualTo(aa);
        assertThat(aa.replacePrefix(b)).isEqualTo(ba);
        assertThat(aa.replacePrefix(ab)).isEqualTo(ab);
        assertThatThrownBy(() -> aa.replacePrefix(ba)).isExactlyInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> aa.replacePrefix(bb)).isExactlyInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> aa.replacePrefix(aaa)).isExactlyInstanceOf(AssertionError.class);

        assertThat(aaa.replacePrefix(a)).isEqualTo(aaa);
        assertThat(aaa.replacePrefix(b)).isEqualTo(baa);
        assertThat(aaa.replacePrefix(aa)).isEqualTo(aaa);
        assertThat(aaa.replacePrefix(ab)).isEqualTo(aba);
        assertThat(aaa.replacePrefix(aab)).isEqualTo(aab);
    }
}
