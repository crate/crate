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

package io.crate.analyze;

import io.crate.session.Cursors;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.CoordinatorSessionSettings;

public class Analysis {

    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final ParamTypeHints paramTypeHints;
    private final Cursors cursors;


    public Analysis(CoordinatorTxnCtx coordinatorTxnCtx,
                    ParamTypeHints paramTypeHints,
                    Cursors cursors) {
        this.paramTypeHints = paramTypeHints;
        this.coordinatorTxnCtx = coordinatorTxnCtx;
        this.cursors = cursors;
    }

    public Cursors cursors() {
        return cursors;
    }

    public CoordinatorTxnCtx transactionContext() {
        return coordinatorTxnCtx;
    }

    public CoordinatorSessionSettings sessionSettings() {
        return coordinatorTxnCtx.sessionSettings();
    }

    public ParamTypeHints paramTypeHints() {
        return paramTypeHints;
    }
}
