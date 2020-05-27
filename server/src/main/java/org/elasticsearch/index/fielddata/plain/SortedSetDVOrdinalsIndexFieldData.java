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

package org.elasticsearch.index.fielddata.plain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.NullValueOrder;
import org.elasticsearch.search.MultiValueMode;

public class SortedSetDVOrdinalsIndexFieldData extends DocValuesIndexFieldData implements IndexOrdinalsFieldData {

    private static final Logger LOGGER = LogManager.getLogger(SortedSetDVOrdinalsIndexFieldData.class);

    public SortedSetDVOrdinalsIndexFieldData(IndexSettings indexSettings, String fieldName) {
        super(indexSettings.getIndex(), fieldName);
    }

    @Override
    public SortField sortField(NullValueOrder nullValueOrder, MultiValueMode sortMode, boolean reverse) {
        SortField sortField = new SortedSetSortField(fieldName, reverse,
            sortMode == MultiValueMode.MAX ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
        sortField.setMissingValue(
            nullValueOrder == NullValueOrder.LAST ^ reverse
                ? SortedSetSortField.STRING_LAST
                : SortedSetSortField.STRING_FIRST);
        return sortField;
    }
}
