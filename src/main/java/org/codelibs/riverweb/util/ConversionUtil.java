package org.codelibs.riverweb.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ConversionUtil {

    public static final String ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final TimeZone TIMEZONE_UTC = TimeZone.getTimeZone("UTC");

    public static <T> T convert(Object value, Class<T> clazz) {
        if (value instanceof CharSequence) {
            final String text = value.toString();
            if (clazz.isAssignableFrom(Integer.class)) {
                return (T) Integer.valueOf(text);
            } else if (clazz.isAssignableFrom(Long.class)) {
                return (T) Long.valueOf(text);
            } else if (clazz.isAssignableFrom(Date.class)) {
                return (T) parseDate(text);
            } else if (clazz.isAssignableFrom(String.class)) {
                return (T) text;
            }
        } else if (value instanceof Number) {
            final Number v = (Number) value;
            if (clazz.isAssignableFrom(String.class)) {
                return (T) v.toString();
            } else if (clazz.isAssignableFrom(Integer.class)) {
                return (T) Integer.valueOf(v.intValue());
            } else if (clazz.isAssignableFrom(Long.class)) {
                return (T) Long.valueOf(v.intValue());
            } else if (clazz.isAssignableFrom(Date.class)) {
                return (T) new Date(v.longValue());
            }
        } else if (value instanceof Date) {
            final Date d = (Date) value;
            if (clazz.isAssignableFrom(String.class)) {
                return (T) formatDate(d);
            } else if (clazz.isAssignableFrom(Integer.class)) {
                return (T) Integer.valueOf((int) d.getTime());
            } else if (clazz.isAssignableFrom(Long.class)) {
                return (T) Long.valueOf(d.getTime());
            } else if (clazz.isAssignableFrom(Date.class)) {
                return (T) d;
            }
        }
        return null;
    }

    public static String formatDate(final Date date) {
        if (date == null) {
            return null;
        }

        final SimpleDateFormat sdf = new SimpleDateFormat(ISO_DATETIME_FORMAT);
        sdf.setTimeZone(TIMEZONE_UTC);
        return sdf.format(date);
    }

    public static Date parseDate(final String value) {
        if (value == null) {
            return null;
        }
        try {
            final SimpleDateFormat sdf = new SimpleDateFormat(ISO_DATETIME_FORMAT);
            sdf.setTimeZone(TIMEZONE_UTC);
            return sdf.parse(value);
        } catch (final ParseException e) {
            return null;
        }
    }
}
