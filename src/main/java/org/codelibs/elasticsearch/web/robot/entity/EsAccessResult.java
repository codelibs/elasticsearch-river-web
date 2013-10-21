package org.codelibs.elasticsearch.web.robot.entity;

import java.io.IOException;
import java.sql.Timestamp;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.seasar.framework.beans.util.Beans;
import org.seasar.robot.entity.AccessResultData;
import org.seasar.robot.entity.AccessResultImpl;
import org.seasar.robot.entity.ResponseData;
import org.seasar.robot.entity.ResultData;

public class EsAccessResult extends AccessResultImpl implements ToXContent {

    @Override
    public void init(final ResponseData responseData,
            final ResultData resultData) {

        setCreateTime(new Timestamp(System.currentTimeMillis()));
        if (responseData != null) {
            Beans.copy(responseData, this).execute();
        }

        final AccessResultData accessResultData = new EsAccessResultData();
        if (resultData != null) {
            Beans.copy(resultData, accessResultData).execute();
        }
        setAccessResultData(accessResultData);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder,
            final Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field("id", id);
        }
        if (sessionId != null) {
            builder.field("sessionId", sessionId);
        }
        if (ruleId != null) {
            builder.field("ruleId", ruleId);
        }
        if (url != null) {
            builder.field("url", url);
        }
        if (parentUrl != null) {
            builder.field("parentUrl", parentUrl);
        }
        if (status != null) {
            builder.field("status", status);
        }
        if (httpStatusCode != null) {
            builder.field("httpStatusCode", httpStatusCode);
        }
        if (method != null) {
            builder.field("method", method);
        }
        if (mimeType != null) {
            builder.field("mimeType", mimeType);
        }
        if (createTime != null) {
            builder.field("createTime", createTime);
        }
        if (executionTime != null) {
            builder.field("executionTime", executionTime);
        }
        if (contentLength != null) {
            builder.field("contentLength", contentLength);
        }
        if (lastModified != null) {
            builder.field("lastModified", lastModified);
        }
        if (accessResultData instanceof ToXContent) {
            builder.field("accessResultData");
            ((ToXContent) accessResultData).toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

}
