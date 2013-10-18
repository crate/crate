package com.carrotsearch.junitbenchmarks;

import com.carrotsearch.junitbenchmarks.h2.H2Consumer;

/**
 * Shortcuts for known {@link com.carrotsearch.junitbenchmarks.IResultsConsumer}.
 */
public enum ConsumerName
{
    XML(XMLConsumer.class),
    H2(H2Consumer.class),
    CONSOLE(WriterConsumer.class);

    /** 
     * Consumer class.
     */
    public final Class<? extends IResultsConsumer> clazz;

    /*
     * 
     */
    private ConsumerName(Class<? extends IResultsConsumer> clazz)
    {
        this.clazz = clazz;
    }
}
