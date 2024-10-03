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

package io.crate.session;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.jetbrains.annotations.Nullable;

public final class Cursors implements Iterable<Cursor> {

    public static final Cursors EMPTY = new Cursors(Map.of());

    private final Map<String, Cursor> cursors;

    private Cursors(Map<String, Cursor> cursors) {
        this.cursors = cursors;
    }

    public Cursors() {
        this.cursors = new HashMap<>();
    }

    public void add(String cursorName, Cursor cursor) {
        Cursor previous = cursors.put(cursorName, cursor);
        if (previous != null) {
            throw new IllegalStateException("Cursor `" + cursorName + "` already exists");
        }
    }

    public Cursor get(String cursorName) {
        Cursor cursor = cursors.get(cursorName);
        if (cursor == null) {
            throw new IllegalArgumentException("No cursor named `" + cursorName + "` available");
        }
        return cursor;
    }

    public int size() {
        return cursors.size();
    }

    /**
     * Close and remove all cursors matching the predicate
     */
    public void close(Predicate<Cursor> predicate) {
        Iterator<Entry<String, Cursor>> it = cursors.entrySet().iterator();
        while (it.hasNext()) {
            var cursor = it.next().getValue();
            if (predicate.test(cursor)) {
                cursor.close();
                it.remove();
            }
        }
    }

    /**
     * @param cursorName name of cursor to close, if null all cursors are closed
     */
    public void close(@Nullable String cursorName) {
        if (cursorName == null) {
            close(c -> true);
        } else {
            Cursor cursor = cursors.remove(cursorName);
            if (cursor == null) {
                throw new IllegalArgumentException("No cursor named `" + cursorName + "` available");
            } else {
                cursor.close();
            }
        }
    }

    @Override
    public String toString() {
        return "Cursors{" + cursors.keySet() + "}";
    }

    @Override
    public Iterator<Cursor> iterator() {
        return cursors.values().iterator();
    }
}
