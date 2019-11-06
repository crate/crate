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

package io.crate.expression.reference.partitioned;

import io.crate.metadata.PartitionInfo;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class PartitionsSettingsExpressionTest {

    @Test
    public void test_no_npe_on_string_setting_that_is_missing() {
        var expression = new PartitionsSettingsExpression.StringPartitionTableParameterExpression("foo");
        PartitionName name = new PartitionName(new RelationName("doc", "dummy"), List.of("dummyValue"));
        expression.setNextRow(new PartitionInfo(
            name, 1, "0", Version.CURRENT, null, false, Map.of(), Map.of()));
        assertThat(expression.value(), Matchers.nullValue());
    }
}
