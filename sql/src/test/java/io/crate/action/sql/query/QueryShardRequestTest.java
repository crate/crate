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

package io.crate.action.sql.query;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.createFunction;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class QueryShardRequestTest {


    @Test
    public void testQueryShardRequestSerialization() throws Exception {
        Reference nameRef = createReference("name", DataTypes.STRING);
        Function query = createFunction(EqOperator.NAME, DataTypes.BOOLEAN, nameRef, Literal.newLiteral("Arthur"));
        WhereClause whereClause = new WhereClause(query);
        QueryShardRequest request = new QueryShardRequest(
                "dummyTable",
                1,
                ImmutableList.of(nameRef),
                ImmutableList.<Symbol>of(nameRef),
                new boolean[] { false },
                new Boolean[] { null },
                10,
                0,
                whereClause,
                ImmutableList.<ReferenceInfo>of(),
                Optional.of(TimeValue.timeValueHours(3))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        QueryShardRequest inRequest = new QueryShardRequest();
        inRequest.readFrom(in);
        assertThat(request, equalTo(inRequest));
    }
}