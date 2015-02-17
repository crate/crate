package io.crate.executor;

import com.google.common.collect.Iterables;

public class Pages {

    public static Page concat(Page firstPage, Page secondPage) {
        return new IteratorPage(
                Iterables.concat(firstPage, secondPage),
                firstPage.size() + secondPage.size(),
                secondPage.isLastPage()
        );
    }
}
