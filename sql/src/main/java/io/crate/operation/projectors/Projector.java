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

package io.crate.operation.projectors;

/**
 * executing a Projection
 */
public interface Projector extends Iterable<Object[]> {

    /**
     * set the Projector to hand over the projected rows.
     * If no downstream Projector was set, rows are simply gathered.
     *
     * @param downStream the Projector to hand over projected rows
     */
    public void setDownStream(Projector downStream);


    /**
     * initialize anything needed for proper projecting the projection
     */
    public void startProjection();


    /**
     * feed this Projector with the next input row.
     * If this projection does not need any more rows, it returns <code>false</code>,
     * <code>true</code> otherwise.
     *
     * This method must be thread safe.
     *
     * @param row
     * @return false if this projection does not need any more rows, true otherwise.
     */
    public boolean setNextRow(Object ... row);


    /**
     * finish the projection. if rows had to be gathered,
     * process them to the downStream Projectors.
     */
    public void finishProjection();

    /**
     * finally, return all collected rows.
     * @return
     * @throws java.lang.IllegalStateException if this Projector is not done yet.
     */
    public Object[][] getRows() throws IllegalStateException;
}
