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

package io.crate.executor;

import com.google.common.base.Preconditions;
import io.crate.core.collections.ArrayIterator;

import java.util.Iterator;

public class ObjectArrayPage implements Page {

    private final int start;
    private final int size;
    private final Object[][] pageSource;

    public ObjectArrayPage(Object[][] pageSource, int start, int size) {
        Preconditions.checkArgument(start <= pageSource.length, "start exceeds page");
        this.start = start;
        this.size = Math.min(size, pageSource.length - start);
        this.pageSource = pageSource;
    }

    public ObjectArrayPage(Object[][] pageSource) {
        this(pageSource, 0, pageSource.length);
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new ArrayIterator<>(pageSource, start, start +  size);
    }

    @Override
    public long size() {
        return size;
    }
}
