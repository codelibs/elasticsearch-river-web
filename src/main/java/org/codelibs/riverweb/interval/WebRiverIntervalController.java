package org.codelibs.riverweb.interval;

import org.codelibs.fess.crawler.interval.impl.DefaultIntervalController;

public class WebRiverIntervalController extends DefaultIntervalController {
    public void setDelayMillisForWaitingNewUrl(final long delayMillisForWaitingNewUrl) {
        this.delayMillisForWaitingNewUrl = delayMillisForWaitingNewUrl;
    }

    public long getDelayMillisForWaitingNewUrl() {
        return delayMillisForWaitingNewUrl;
    }
}
