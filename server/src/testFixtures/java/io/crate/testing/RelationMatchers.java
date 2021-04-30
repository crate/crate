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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.RelationName;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import static io.crate.testing.MoreMatchers.withFeature;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;

public class RelationMatchers {

    public static Matcher<AnalyzedRelation> isDocTable(RelationName name) {
        return allOf(
            Matchers.instanceOf(DocTableRelation.class),
            withFeature(x -> ((DocTableRelation) x).tableInfo().ident(), "relationName", equalTo(name))
        );
    }

    public static Matcher<AnalyzedRelation> isSystemTable(RelationName name) {
        return allOf(
            Matchers.instanceOf(TableRelation.class),
            withFeature(x -> ((TableRelation) x).tableInfo().ident(), "relationName", equalTo(name))
        );
    }
}
