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

package io.crate.analyze.symbol.format;

import static io.crate.analyze.symbol.format.SymbolPrinter.Strings.ELLIPSIS;

class SymbolPrinterContext {

    public static final int DEFAULT_MAX_DEPTH = 50;
    public static final boolean DEFAULT_FAIL_IF_MAX_DEPTH_REACHED = false;
    public static final boolean DEFAULT_FULL_QUALIFIED = false;
    final StringBuilder builder = new StringBuilder();
    final int maxDepth;
    final boolean fullQualified;
    final boolean failIfMaxDepthReached;

    int depth = 0;

    public SymbolPrinterContext(int maxDepth, boolean fullQualified, boolean failIfMaxDepthReached) {
        this.maxDepth = maxDepth;
        this.fullQualified = fullQualified;
        this.failIfMaxDepthReached = failIfMaxDepthReached;
    }

    public SymbolPrinterContext down() {
        depth++;
        return this;
    }

    public SymbolPrinterContext up() {
        depth--;
        return this;
    }

    boolean verifyMaxDepthReached() {
        if (maxDepthReached()) {
            if (failIfMaxDepthReached) {
                throw new MaxDepthReachedException(maxDepth);
            } else {
                builder.append(ELLIPSIS);
                return true;
            }
        }
        return false;
    }

    boolean maxDepthReached() {
        return depth >= maxDepth;
    }

    String formatted() {
        return builder.toString();
    }

    public boolean isFullQualified() {
        return fullQualified;
    }
}
