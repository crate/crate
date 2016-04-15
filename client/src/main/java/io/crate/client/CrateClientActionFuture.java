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

package io.crate.client;

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLBaseResponse;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;

public class CrateClientActionFuture<Response extends SQLBaseResponse> extends AdapterActionFuture<Response, Response> {

    @Override
    protected Response convert(Response listenerResponse) {
        return listenerResponse;
    }

    @Override
    public void onFailure(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof NotSerializableExceptionWrapper) {
            NotSerializableExceptionWrapper wrapper = ((NotSerializableExceptionWrapper) cause);
            SQLActionException sae = SQLActionException.fromSerializationWrapper(wrapper);
            if (sae != null) {
                e = sae;
            }
        }
        super.onFailure(e);
    }
}
