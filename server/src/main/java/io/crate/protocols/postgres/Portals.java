/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.protocols.postgres;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import io.crate.action.sql.PreparedStmt;
import io.crate.analyze.AnalyzedDeclareCursor;
import io.crate.analyze.AnalyzedFetchFromCursor;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;

public class Portals {

    private final Map<String, Portal> portals = new HashMap<>();

    public Portal create(String portalName,
                         PreparedStmt preparedStmt,
                         List<Object> params,
                         AnalyzedStatement analyzedStatement,
                         @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        var ctx = new Context(this, portalName, preparedStmt, params, resultFormatCodes);
        return analyzedStatement.accept(new PortalDecider(), ctx);
    }

    public Portal safeGet(String portalName) {
        Portal portal = portals.get(portalName);
        if (portal == null) {
            throw new IllegalArgumentException("Cannot find portal: " + portalName);
        }
        return portal;
    }

    public void put(String portalName, Portal portal) {
        Portal oldPortal = portals.put(portalName, portal);
        if (oldPortal != null && !(oldPortal instanceof Cursor)) {
            // According to the wire protocol spec named portals should be removed explicitly and only
            // unnamed portals are implicitly closed/overridden.
            // We don't comply with the spec because we allow batching of statements, see #execute
            oldPortal.closeActiveConsumer();
        }
    }

    public Portal remove(String name) {
        return portals.remove(name);
    }

    public Set<Map.Entry<String, Portal>> entrySet() {
        return portals.entrySet();
    }

    public void clear() {
        portals.clear();
    }

    public Collection<Portal> values() {
        return portals.values();
    }

    public boolean containsKey(String name) {
        return portals.containsKey(name);
    }

    public int size() {
        return portals.size();
    }

    private record Context(Portals portals,
                           String portalName,
                           PreparedStmt preparedStmt,
                           List<Object> params,
                           @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
    }

    private static class PortalDecider extends AnalyzedStatementVisitor<Context, Portal> {

        @Override
        protected Portal visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Context context) {
            return new Portal(context.portalName, context.preparedStmt, context.params, analyzedStatement, context.resultFormatCodes);
        }

        @Override
        public Portal visitDeclareCursor(AnalyzedDeclareCursor declareCursor, Context context) {
            return new Cursor(declareCursor.cursorName(), context.preparedStmt, context.params, declareCursor, context.resultFormatCodes);
        }

        @Override
        public Portal visitFetchFromCursor(AnalyzedFetchFromCursor fetchFromCursor, Context context) {
            var portal = context.portals.portals.get(fetchFromCursor.cursorName());
            if (portal instanceof Cursor cursor) {
                cursor.bindFetch();
                return cursor;
            }
            throw new IllegalArgumentException("Cursor '" + fetchFromCursor.cursorName() + "' has not been declared.");
        }
    }
}
