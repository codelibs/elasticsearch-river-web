package org.codelibs.elasticsearch.web.robot.interval;

import org.codelibs.robot.interval.impl.DefaultIntervalController;

public class WebRiverIntervalController extends DefaultIntervalController {
    public void setDelayMillisForWaitingNewUrl(
            final long delayMillisForWaitingNewUrl) {
        this.delayMillisForWaitingNewUrl = delayMillisForWaitingNewUrl;
    }

    public long getDelayMillisForWaitingNewUrl() {
        return delayMillisForWaitingNewUrl;
    }
}
