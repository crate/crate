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

package io.crate.exceptions;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.transport.RemoteTransportException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;

public class Exceptions {

    public static Throwable unwrap(@Nonnull Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof RemoteTransportException ||
                result instanceof UncheckedExecutionException ||
                result instanceof UncategorizedExecutionException ||
                result instanceof ExecutionException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter > 10) {
                return result;
            }
            counter++;
            result = result.getCause();
        }
        return result;
    }

    public static String messageOf(@Nullable Throwable t) {
        if (t == null) {
            return "Unknown";
        }
        @SuppressWarnings("all") // throwable not thrown
        Throwable unwrappedT = unwrap(t);
        return MoreObjects.firstNonNull(unwrappedT.getMessage(), unwrappedT.toString());
    }
}
