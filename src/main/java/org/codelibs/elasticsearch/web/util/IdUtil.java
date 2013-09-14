package org.codelibs.elasticsearch.web.util;

import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.seasar.framework.beans.BeanDesc;
import org.seasar.framework.beans.PropertyDesc;
import org.seasar.framework.beans.factory.BeanDescFactory;
import org.seasar.framework.util.StringUtil;
import org.seasar.robot.RobotSystemException;

public class IdUtil {

    private static final Base64 base64 = new Base64(Integer.MAX_VALUE,
            new byte[0], true);

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private IdUtil() {
    }

    public static String getId(final Object obj) {
        final BeanDesc beanDesc = BeanDescFactory.getBeanDesc(obj.getClass());
        final PropertyDesc urlProp = beanDesc.getPropertyDesc("url");
        final Object url = urlProp.getValue(obj);
        if (url == null) {
            throw new RobotSystemException("url is null.");
        }
        return getId(url.toString());
    }

    public static String getId(final String url) {
        if (StringUtil.isBlank(url)) {
            throw new RobotSystemException("url is blank.");
        }
        return new String(base64.encode(url.getBytes(UTF_8)), UTF_8);
    }

    public static String getType(final Object target) {
        final BeanDesc beanDesc = BeanDescFactory
                .getBeanDesc(target.getClass());
        final PropertyDesc sessionIdProp = beanDesc
                .getPropertyDesc("sessionId");
        final Object sessionId = sessionIdProp.getValue(target);
        return sessionId == null ? null : sessionId.toString();
    }
}
