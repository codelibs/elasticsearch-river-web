package org.codelibs.elasticsearch.web.interval;

import org.seasar.robot.interval.impl.DefaultIntervalController;

public class WebRiverIntervalController extends DefaultIntervalController {
    public void setDelayMillisForWaitingNewUrl(
            final long delayMillisForWaitingNewUrl) {
        this.delayMillisForWaitingNewUrl = delayMillisForWaitingNewUrl;
    }

    public long getDelayMillisForWaitingNewUrl() {
        return delayMillisForWaitingNewUrl;
    }
}
