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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.JoinNode.JoinType;

public class IndexConstraintDefinitionNode extends ConstraintDefinitionNode implements IndexDefinition
{
    private String indexName;
    private String indexMethod;
    private boolean indexOff;
    private boolean inlineColumnIndex;
    private IndexColumnList indexColumnList;
    private IndexProperties indexProperties;


    @Override
    public void init(Object tableName,
                     Object indexName,
                     Object indexMethod,
                     Object indexColumnList,
                     Object indexProperties,
                     Object inlineColumnIndex)
    {
        super.init(tableName,
                   ConstraintType.INDEX,
                   null, // column list? don't need. Use indexColumnList instead
                   null, // properties - none
                   null, // constrainText  - none
                   null, // conditionCheck  - none
                   StatementType.UNKNOWN, // behaviour? 
                   ConstraintType.INDEX);
        
        this.indexName = (String) indexName;
        this.indexMethod = (String) indexMethod;
        this.indexColumnList = (IndexColumnList) indexColumnList;
        this.indexProperties = (IndexProperties) indexProperties;
        this.indexOff = false;
        this.inlineColumnIndex = (Boolean)inlineColumnIndex;
    }

    /**
     * INITIALIZED AS "INDEX OFF"
     *
     * @param tableName the name of the constraint, nearly always null
     * @param indexName this is the column name
     * @param indexOff if field should not be indexed
     */
    @Override
    public void init(Object tableName, Object indexName, Object indexOff) {
        super.init(tableName,
                ConstraintType.INDEX,
                null,
                null,
                null,
                null,
                StatementType.UNKNOWN,
                ConstraintType.INDEX
                );
        this.indexOff = (Boolean)indexOff;
        this.indexName = (String)indexName;
        this.inlineColumnIndex = true;
    }

    public String getIndexName()
    {
        return indexName;
    }

    public String getIndexMethod() {
        return indexMethod;
    }
    
    public IndexColumnList getIndexColumnList()
    {
        return indexColumnList;
    }

    public IndexProperties getIndexProperties() {
        return indexProperties;
    }

    @Override
    public JoinType getJoinType()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isInlineColumnIndex() {
        return inlineColumnIndex;
    }
    
    // This is used for the non-unique "INDEX" defintions only
    public boolean getUniqueness() 
    {
        return false;
    }
    
    public TableName getObjectName()
    {
        return constraintName;
    }
    
    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException
    {
        super.copyFrom(node);
        
        IndexConstraintDefinitionNode other = (IndexConstraintDefinitionNode) node;
        this.indexName = other.indexName;
        this.indexMethod = other.indexMethod;
        this.indexColumnList = other.indexColumnList;
        this.indexProperties = other.indexProperties; // TODO: deepcopy?
    }
    
    @Override
    public String toString()
    {
        return super.toString()
                + "\nindexName: " + indexName
                + "\nindexOff: " + indexOff
                + "\nindexMethod: " + indexMethod
                + "\nindexProperties: " + indexProperties
                ;
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (indexColumnList != null) {
            printLabel(depth, "indexColumnList: ");
            indexColumnList.treePrint(depth + 1);
        }
    }

    public boolean isIndexOff() { return indexOff; }
    
}
