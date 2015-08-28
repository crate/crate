/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.types;

import org.apache.lucene.util.BytesRef;

public class BytesRefs2 {

    private BytesRefs2() {}

    public static final BytesRef LOWER_SHORT_TRUE = new BytesRef("t");
    public static final BytesRef UPPER_SHORT_TRUE= new BytesRef("T");
    public static final BytesRef LOWER_SHORT_FALSE = new BytesRef("f");
    public static final BytesRef UPPER_SHORT_FALSE = new BytesRef("F");

    public static final BytesRef LOWER_TRUE = new BytesRef("true");
    public static final BytesRef UPPER_TRUE= new BytesRef("TRUE");
    public static final BytesRef LOWER_FALSE = new BytesRef("false");
    public static final BytesRef UPPER_FALSE = new BytesRef("FALSE");
}
