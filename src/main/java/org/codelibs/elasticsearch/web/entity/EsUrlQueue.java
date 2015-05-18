package org.codelibs.elasticsearch.web.entity;

import java.io.IOException;

import org.codelibs.robot.entity.UrlQueueImpl;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class EsUrlQueue extends UrlQueueImpl implements ToXContent {

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field("id", id);
        }
        if (sessionId != null) {
            builder.field("sessionId", sessionId);
        }
        if (method != null) {
            builder.field("method", method);
        }
        if (url != null) {
            builder.field("url", url);
        }
        if (parentUrl != null) {
            builder.field("parentUrl", parentUrl);
        }
        if (depth != null) {
            builder.field("depth", depth);
        }
        if (lastModified != null) {
            builder.field("lastModified", lastModified);
        }
        if (createTime != null) {
            builder.field("createTime", createTime);
        }
        builder.endObject();
        return builder;
    }

}
