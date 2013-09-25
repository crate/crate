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

/* The original from which this derives bore the following: */

/*
   Derby - Class org.apache.derby.impl.sql.compile.ResultColumnList

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

/**
 * A ResultColumnList is the target list of a SELECT, INSERT, or UPDATE.
 *
 * @see ResultColumn
 */

public class ResultColumnList extends QueryTreeNodeList<ResultColumn>
{
    int orderBySelect = 0; // the number of result columns pulled up
    // from ORDERBY list
    /*
     * A comment on 'orderBySelect'. When we encounter a SELECT .. ORDER BY
     * statement, the columns (or expressions) in the ORDER BY clause may
     * or may not have been explicitly mentioned in the SELECT column list.
     * If the columns were NOT explicitly mentioned in the SELECT column
     * list, then the parsing of the ORDER BY clause implicitly generates
     * them into the result column list, because we'll need to have those
     * columns present at execution time in order to sort by them. Those
     * generated columns are added to the *end* of the ResultColumnList, and
     * we keep track of the *number* of those columns in 'orderBySelect',
     * so we can tell whether we are looking at a generated column by seeing
     * whether its position in the ResultColumnList is in the last
     * 'orderBySelect' number of columns. If the SELECT .. ORDER BY
     * statement uses the "*" token to select all the columns from a table,
     * then during ORDER BY parsing we redundantly generate the columns
     * mentioned in the ORDER BY clause into the ResultColumnlist, but then
     * later in getOrderByColumnToBind we determine that these are
     * duplicates and we take them back out again.
     */

    /*
    ** Is this ResultColumnList for a FromBaseTable for an index
    ** that is to be updated?
    */
    protected boolean forUpdate;

    // Number of RCs in this RCL at "init" time, before additional
    // ones were added internally.
    private int initialListSize = 0;

    public ResultColumnList() {
    }

    /**
     * Add a ResultColumn (at this point, ResultColumn or
     * AllResultColumn) to the list
     *
     * @param resultColumn The ResultColumn to add to the list
     */

    public void addResultColumn(ResultColumn resultColumn) {
        /* Lists are 0-based, ResultColumns are 1-based */
        resultColumn.setVirtualColumnId(size() + 1);
        add(resultColumn);
    }

    /**
     * Append a given ResultColumnList to this one, resetting the virtual
     * column ids in the appended portion.
     *
     * @param resultColumns The ResultColumnList to be appended
     * @param destructiveCopy Whether or not this is a destructive copy
     *              from resultColumns
     */
    public void appendResultColumns(ResultColumnList resultColumns,
                                    boolean destructiveCopy) {
        int oldSize = size();
        int newID = oldSize + 1;

        /*
        ** Set the virtual column ids in the list being appended.
        ** Lists are zero-based, and virtual column ids are one-based,
        ** so the new virtual column ids start at the original size
        ** of this list, plus one.
        */
        int otherSize = resultColumns.size();
        for (int index = 0; index < otherSize; index++) {
            /* ResultColumns are 1-based */
            resultColumns.get(index).setVirtualColumnId(newID);
            newID++;
        }

        if (destructiveCopy) {
            destructiveAddAll(resultColumns);
        }
        else {
            addAll(resultColumns);
        }
    }

    /**
     * Get a ResultColumn from a column position (1-based) in the list
     *
     * @param position The ResultColumn to get from the list (1-based)
     *
     * @return the column at that position.
     */

    public ResultColumn getResultColumn(int position) {
        /*
        ** First see if it falls in position x.  If not,
        ** search the whole shebang
        */
        if (position <= size()) {
            // this wraps the cast needed, 
            // and the 0-based nature of the Lists.
            ResultColumn rc = (ResultColumn)get(position-1);
            if (rc.getColumnPosition() == position) {
                return rc;
            }
        }

        /*
        ** Check each column
        */
        int size = size();
        for (int index = 0; index < size; index++) {
            ResultColumn rc = get(index);
            if (rc.getColumnPosition() == position) {
                return rc;
            }
        }
        return null;
    }

    /**
     * Get a ResultColumn from a column position (1-based) in the list,
     * null if out of range (for order by).
     *
     * @param position The ResultColumn to get from the list (1-based)
     *
     * @return the column at that position, null if out of range
     */
    public ResultColumn getOrderByColumn(int position) {
        // this wraps the cast needed, and the 0-based nature of the Lists.
        if (position == 0) 
            return null;

        return getResultColumn(position);
    }

    /**
     * Get a ResultColumn that matches the specified columnName. If requested
     * to, mark the column as referenced.
     *
     * @param columnName The ResultColumn to get from the list
     * @param markIfReferenced True if we should mark this column as referenced.
     *
     * @return the column that matches that name.
     */

    public ResultColumn getResultColumn(String columnName) {
        int size = size();
        for (int index = 0; index < size; index++) {
            ResultColumn resultColumn = get(index);

            if (columnName.equalsIgnoreCase(resultColumn.getName())) {
                return resultColumn;
            }
        }
        return null;
    }

    /**
     * Get an array of strings for all the columns
     * in this RCL.
     *
     * @return the array of strings
     */
    public String[] getColumnNames() {
        String strings[] = new String[size()];

        int size = size();

        for (int index = 0; index < size; index++) {
            ResultColumn resultColumn = get(index);
            strings[index] = resultColumn.getName();
        }
        return strings;
    }

    /**
     * Remove the columns which are join columns (in the
     * joinColumns RCL) from this list.  This is useful
     * for a JOIN with a USING clause.
     * 
     * @param joinColumns The list of join columns
     */
    public void removeJoinColumns(ResultColumnList joinColumns) {
        for (ResultColumn joinRC : joinColumns) {
            String columnName = joinRC.getName();
            ResultColumn rightRC = getResultColumn(columnName);
            // Remove the RC from this list.
            if (rightRC != null) {
                remove(rightRC);
            }
        }
    }

    /**
     * Get the join columns from this list.
     * This is useful for a join with a USING clause.  
     * (ANSI specifies that the join columns appear 1st.) 
     *
     * @param joinColumns A list of the join columns.
     *
     * @return A list of the join columns from this list
     */
    public ResultColumnList getJoinColumns(ResultColumnList joinColumns)
            throws StandardException {
        ResultColumnList newRCL = new ResultColumnList();

        /* Find all of the join columns and put them on the new RCL. */
        for (ResultColumn joinRC : joinColumns) {
            String columnName = joinRC.getName();
            ResultColumn xferRC = getResultColumn(columnName);

            if (xferRC == null) {
                throw new StandardException("Column not found: " + columnName);
            }

            // Add the RC to the new list.
            newRCL.add(xferRC);
        }
        return newRCL;
    }

    /* ****
     * Take note of the size of this RCL _before_ we start
     * processing/binding it.    This is so that, at bind time,
     * we can tell if any columns in the RCL were added
     * internally by us (i.e. they were not specified by the
     * user and thus will not be returned to the user).
     */
    protected void markInitialSize() {
        initialListSize = size();
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    public String toString() {
        return super.toString();
    }

}
