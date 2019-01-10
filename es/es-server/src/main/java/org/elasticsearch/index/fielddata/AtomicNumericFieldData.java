/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

/**
 * Specialization of {@link AtomicFieldData} for numeric data.
 */
public interface AtomicNumericFieldData extends AtomicFieldData {

    /**
     * Get an integer view of the values of this segment. If the implementation
     * stores floating-point numbers then these values will return the same
     * values but casted to longs.
     */
    SortedNumericDocValues getLongValues();

    /**
     * Return a floating-point view of the values in this segment. If the
     * implementation stored integers then the returned doubles would be the
     * same ones as you would get from casting to a double.
     */
    SortedNumericDoubleValues getDoubleValues();

}
