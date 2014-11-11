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

package io.crate.analyze;

import io.crate.metadata.ColumnIdent;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

public class AnalyzedColumnDefinitionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMerge() throws Exception {
        AnalyzedColumnDefinition definition1 = new AnalyzedColumnDefinition(ColumnIdent.fromPath("root.child"), DataTypes.OBJECT, null);
        AnalyzedColumnDefinition definition2 = new AnalyzedColumnDefinition(ColumnIdent.fromPath("other"), DataTypes.OBJECT, null);
        AnalyzedColumnDefinition child = new AnalyzedColumnDefinition(ColumnIdent.fromPath("other.child"),DataTypes.TIMESTAMP, definition2);
        definition2.addChild(child);

        definition1.mergeChildren(definition2.children());
        assertThat(definition1.children(), hasItems(child));
    }

    @Test
    public void testMergeConflict() throws Exception {
        AnalyzedColumnDefinition definition1 = new AnalyzedColumnDefinition(ColumnIdent.fromPath("other.brother"), DataTypes.OBJECT, null);
        AnalyzedColumnDefinition child1 = new AnalyzedColumnDefinition(ColumnIdent.fromPath("other.brother.bro"), DataTypes.STRING, null);
        child1.index("analyzed");
        child1.analyzer("english");
        definition1.addChild(child1);


        AnalyzedColumnDefinition definition2 = new AnalyzedColumnDefinition(ColumnIdent.fromPath("other.brother"), DataTypes.OBJECT, null);
        AnalyzedColumnDefinition child2 = new AnalyzedColumnDefinition(ColumnIdent.fromPath("other.brother.bro"), DataTypes.STRING, null);
        definition2.addChild(child2);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The values for column 'other.brother.bro' differ in their types.");

        definition1.mergeChildren(definition2.children());
    }

    @Test
    public void testIdent() throws Exception {
        AnalyzedColumnDefinition columnDefinition = new AnalyzedColumnDefinition(null);
        columnDefinition.ident(ColumnIdent.fromPath("root"));
        assertThat(columnDefinition.name(), is("root"));

        AnalyzedColumnDefinition childDefinition = new AnalyzedColumnDefinition(columnDefinition);
        childDefinition.ident(ColumnIdent.fromPath("root.child"));
        assertThat(childDefinition.name(), is("child"));
        assertThat(childDefinition.ident().fqn(), is("root.child"));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("given ident 'child_too' is no child of parent 'root'");

        AnalyzedColumnDefinition childTooDefinition = new AnalyzedColumnDefinition(columnDefinition);
        childTooDefinition.ident(ColumnIdent.fromPath("child_too"));
    }

    @Test
    public void testName() throws Exception {
        AnalyzedColumnDefinition columnDefinition = new AnalyzedColumnDefinition(null);
        columnDefinition.name("root");
        assertThat(columnDefinition.ident().fqn(), is("root"));

        AnalyzedColumnDefinition childDefinition = new AnalyzedColumnDefinition(columnDefinition);
        childDefinition.name("child");
        assertThat(childDefinition.name(), is("child"));
        assertThat(childDefinition.ident().fqn(), is("root.child"));

        AnalyzedColumnDefinition childTooDefinition = new AnalyzedColumnDefinition(columnDefinition);
        childTooDefinition.name("child_too");
        assertThat(childTooDefinition.name(), is("child_too"));
        assertThat(childTooDefinition.ident().fqn(), is("root.child_too"));
    }
}
