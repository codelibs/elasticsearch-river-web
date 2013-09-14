package org.codelibs.elasticsearch.web.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.elasticsearch.index.query.QueryBuilders;
import org.seasar.robot.service.UrlFilterService;

public class EsUrlFilterService extends AbstractRobotService implements
        UrlFilterService {
    private static final String FILTER_TYPE = "filterType";

    private static final String INCLUDE = "include";

    private static final String EXCLUDE = "exclude";

    @Override
    public void addIncludeUrlFilter(final String sessionId, final String url) {
        final UrlFilter urlFilter = new UrlFilter();
        urlFilter.setSessionId(sessionId);
        urlFilter.setFilterType(INCLUDE);
        urlFilter.setUrl(url);
        insert(urlFilter);
    }

    @Override
    public void addIncludeUrlFilter(final String sessionId,
            final List<String> urlList) {
        final List<UrlFilter> urlFilterList = new ArrayList<UrlFilter>(
                urlList.size());
        for (final String url : urlList) {
            final UrlFilter urlFilter = new UrlFilter();
            urlFilter.setSessionId(sessionId);
            urlFilter.setFilterType(INCLUDE);
            urlFilter.setUrl(url);
            urlFilterList.add(urlFilter);
        }
        insertAll(urlFilterList);
    }

    @Override
    public void addExcludeUrlFilter(final String sessionId, final String url) {
        final UrlFilter urlFilter = new UrlFilter();
        urlFilter.setSessionId(sessionId);
        urlFilter.setFilterType(EXCLUDE);
        urlFilter.setUrl(url);
        insert(urlFilter);
    }

    @Override
    public void addExcludeUrlFilter(final String sessionId,
            final List<String> urlList) {
        final List<UrlFilter> urlFilterList = new ArrayList<UrlFilter>(
                urlList.size());
        for (final String url : urlList) {
            final UrlFilter urlFilter = new UrlFilter();
            urlFilter.setSessionId(sessionId);
            urlFilter.setFilterType(EXCLUDE);
            urlFilter.setUrl(url);
            urlFilterList.add(urlFilter);
        }
        insertAll(urlFilterList);
    }

    @Override
    public void delete(final String sessionId) {
        deleteBySessionId(sessionId);
    }

    @Override
    public void deleteAll() {
        deleteAll();
    }

    @Override
    public List<Pattern> getIncludeUrlPatternList(final String sessionId) {
        // TODO cache
        final List<UrlFilter> urlFilterList = getList(UrlFilter.class,
                sessionId, QueryBuilders.termQuery(FILTER_TYPE, INCLUDE), null,
                null, null);
        final List<Pattern> urlPatternList = new ArrayList<Pattern>();
        for (final UrlFilter urlFilter : urlFilterList) {
            urlPatternList.add(Pattern.compile(urlFilter.getUrl()));
        }
        return urlPatternList;
    }

    @Override
    public List<Pattern> getExcludeUrlPatternList(final String sessionId) {
        // TODO cache
        final List<UrlFilter> urlFilterList = getList(UrlFilter.class,
                sessionId, QueryBuilders.termQuery(FILTER_TYPE, EXCLUDE), null,
                null, null);
        final List<Pattern> urlPatternList = new ArrayList<Pattern>();
        for (final UrlFilter urlFilter : urlFilterList) {
            urlPatternList.add(Pattern.compile(urlFilter.getUrl()));
        }
        return urlPatternList;
    }

    public static class UrlFilter {
        private String sessionId;

        private String filterType;

        private String url;

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(final String sessionId) {
            this.sessionId = sessionId;
        }

        public String getFilterType() {
            return filterType;
        }

        public void setFilterType(final String filterType) {
            this.filterType = filterType;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(final String url) {
            this.url = url;
        }
    }

}
