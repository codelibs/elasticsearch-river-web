package org.codelibs.elasticsearch.web.entity;

import java.io.IOException;

import org.codelibs.robot.entity.AccessResultDataImpl;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class EsAccessResultData extends AccessResultDataImpl implements ToXContent {

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field("id", id);
        }
        if (transformerName != null) {
            builder.field("transformerName", transformerName);
        }
        if (data != null) {
            builder.field("data", data);
        }
        if (encoding != null) {
            builder.field("encoding", encoding);
        }
        builder.endObject();
        return builder;
    }

}
