/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * SQL Parser exception class.
 *
 */

// TODO: The Derby exception handling coordinated localized messages
// and SQLSTATE values, which will be needed, but in the context of
// the new engine.

package org.cratedb.sql.parser;

public class StandardException extends Exception {
    public StandardException(String msg) {
        super(msg);
    }
    public StandardException(Throwable cause) {
        super(cause);
    }
    public StandardException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
