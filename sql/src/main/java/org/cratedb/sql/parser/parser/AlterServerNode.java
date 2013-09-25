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

public class AlterServerNode extends MiscellaneousStatementNode {


    public enum AlterType {
        SET_SERVER_VARIABLE,
        INTERRUPT_SESSION,
        DISCONNECT_SESSION,
        KILL_SESSION,
        SHUTDOWN
    }

    private Integer sessionID = null;
    private AlterType alterSessionType;
    private SetConfigurationNode scn = null;
    private boolean shutdownImmediate;
    
    
    
    public void init(Object config) {
      
        if (config instanceof SetConfigurationNode) {
            scn = (SetConfigurationNode)config;
            alterSessionType = AlterType.SET_SERVER_VARIABLE;
        } else if (config instanceof Boolean) {
            alterSessionType = AlterType.SHUTDOWN;
            shutdownImmediate = ((Boolean)config).booleanValue();
        }
    }
    
    public void init (Object interrupt, Object disconnect, Object kill, Object session)
    {
        if (interrupt != null) {
            alterSessionType = AlterType.INTERRUPT_SESSION;
        } else if (disconnect != null) {
            alterSessionType = AlterType.DISCONNECT_SESSION;
        } else if (kill != null) {
            alterSessionType = AlterType.KILL_SESSION;
        }
        if (session instanceof ConstantNode) {
            sessionID = (Integer)((ConstantNode)session).getValue();
        }
    }
    
    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        AlterServerNode other = (AlterServerNode)node;
        this.sessionID = other.sessionID;
        this.alterSessionType = other.alterSessionType;
        this.scn = (SetConfigurationNode)getNodeFactory().copyNode(other.scn, getParserContext());
        this.shutdownImmediate = other.shutdownImmediate;
    }
    
    @Override
    public String statementToString() {
        return "ALTER SERVER";
    }

    @Override
    public String toString() {
        String ret = null;
        switch (alterSessionType) {
        case SET_SERVER_VARIABLE:
            ret = scn.toString();
            break;
        case SHUTDOWN:
            ret = "shutdown immediate: " + shutdownImmediate;
            break;
        case INTERRUPT_SESSION:
        case DISCONNECT_SESSION:
        case KILL_SESSION:
            ret = "sessionType: " + alterSessionType.name() + "\n" +  
                    "sessionID: " + sessionID;
            break;
        }
        ret = super.toString() + ret;
        return ret;
    }
    
    public final Integer getSessionID() {
        return sessionID;
    }

    public final AlterType getAlterSessionType() {
        return alterSessionType;
    }

    public final boolean isShutdownImmediate() {
        return shutdownImmediate;
    }

    public String getVariable() {
        return scn.getVariable();
    }

    public String getValue() {
        return scn.getValue();
    }
    
}
