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

package io.crate.analyze.relations;

import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;


public class DocTableRelationTest extends CrateUnitTest {

    public void testUpdatingPrimaryKeyThrowsCorrectException() {
        DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, "t1"), null)
            .add("i", DataTypes.INTEGER)
            .addPrimaryKey("i")
            .build();
        DocTableRelation rel = new DocTableRelation(tableInfo);

        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for i: Updating a primary key is not supported");
        rel.ensureColumnCanBeUpdated(new ColumnIdent("i"));
    }

    public void testUpdatingCompoundPrimaryKeyThrowsCorrectException() {
        DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, "t1"), null)
            .add("i", DataTypes.INTEGER)
            .addPrimaryKey("i")
            .add("j", DataTypes.INTEGER)
            .addPrimaryKey("j")
            .build();
        DocTableRelation rel = new DocTableRelation(tableInfo);

        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for i: Updating a primary key is not supported");
        rel.ensureColumnCanBeUpdated(new ColumnIdent("i"));
    }

    public void testUpdatingClusteredByColumnThrowsCorrectException() {
        DocTableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, "t1"), null)
            .add("i", DataTypes.INTEGER)
            .clusteredBy("i")
            .build();
        DocTableRelation rel = new DocTableRelation(tableInfo);

        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for i: Updating a clustered-by column is not supported");
        rel.ensureColumnCanBeUpdated(new ColumnIdent("i"));
    }
}
