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

package io.crate.testing;

import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.TopNProjection;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.AllOf.allOf;

public class ProjectionMatchers {

    public static Matcher<Projection> isTopN(int expectedLimit, int expectedOffset) {
        FeatureMatcher<Projection, Integer> matchLimit = new FeatureMatcher<Projection, Integer>(
            equalTo(expectedLimit), "limit", "limit") {

            @Override
            protected Integer featureValueOf(Projection actual) {
                return ((TopNProjection) actual).limit();
            }
        };
        FeatureMatcher<Projection, Integer> matchOffset = new FeatureMatcher<Projection, Integer>(
            equalTo(expectedOffset), "offset", "offset") {

            @Override
            protected Integer featureValueOf(Projection actual) {
                return ((TopNProjection) actual).offset();
            }
        };
        return allOf(Matchers.<Projection>instanceOf(TopNProjection.class), matchLimit, matchOffset);
    }
}
