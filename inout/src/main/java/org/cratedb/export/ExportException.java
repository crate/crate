/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.export;

import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Created with IntelliJ IDEA.
 * User: bd
 * Date: 9.4.13
 * Time: 14:19
 * To change this template use File | Settings | File Templates.
 */
public class ExportException extends SearchContextException {

    public ExportException(SearchContext context, String msg) {
        super(context, msg);
    }

    public ExportException(SearchContext context, String msg, Throwable t) {
        super(context, "Export Failed [" + msg + "]", t);
    }
}
