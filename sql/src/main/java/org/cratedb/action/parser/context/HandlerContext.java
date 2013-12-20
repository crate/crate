package org.cratedb.action.parser.context;


/**
 * {@link org.cratedb.action.parser.context.ParseContext}
 * that denotes that parsing happens on the Handler
 */
public class HandlerContext extends ParseContext {

    public static final HandlerContext INSTANCE = new HandlerContext();

    private HandlerContext() {
        super(null, null, null);
    }

    @Override
    public boolean onHandler() {
        return true;
    }
}
