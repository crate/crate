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

package io.crate.planner;

import io.crate.exceptions.ValidationException;
import org.junit.Test;

public class UnionPlannerTest extends AbstractPlannerTest {

    @Test
    public void testUnionAsPartOfJoin() {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("JOIN with sub queries is not supported");
        plan("select * from sys.cluster, " +
             "(select name from users " +
             "union all " +
             "select name from sys.nodes) b");
    }

    @Test
    public void testUnionAllWithPartitionedTableWrongOrderBy() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("cannot use partitioned column date in ORDER BY clause");
        plan("select date from parted " +
             "union all " +
             "select date from users " +
             "order by 1");
    }
}
