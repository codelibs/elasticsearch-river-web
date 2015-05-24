package org.codelibs.elasticsearch.web.client;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.codelibs.core.lang.StringUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsRequestBuilder;
import org.elasticsearch.action.termvector.MultiTermVectorsResponse;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorRequestBuilder;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(EsClient.class);

    private Client client;

    protected List<OnConnectListener> onConnectListenerList = new ArrayList<>();

    public void addOnConnectListener(final OnConnectListener listener) {
        onConnectListenerList.add(listener);
    }

    @SuppressWarnings("resource")
    public void connect(final String clusterName, final String hostname, final int port) {
        destroy();
        final Settings settings =
                ImmutableSettings.settingsBuilder().put("cluster.name", StringUtil.isBlank(clusterName) ? "elasticsearch" : clusterName)
                        .build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostname, port));

        onConnectListenerList.forEach(l -> {
            try {
                l.onConnect();
            } catch (final Exception e) {
                logger.warn("Failed to invoke " + l, e);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(
            final Action<Request, Response, RequestBuilder, Client> action, final Request request) {
        return client.execute(action, request);
    }

    @Override
    public void close() throws ElasticsearchException {
        client.close();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(
            final Action<Request, Response, RequestBuilder, Client> action, final Request request, final ActionListener<Response> listener) {
        client.execute(action, request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> RequestBuilder prepareExecute(
            final Action<Request, Response, RequestBuilder, Client> action) {
        return client.prepareExecute(action);
    }

    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    @Override
    public AdminClient admin() {
        return client.admin();
    }

    @Override
    public ActionFuture<IndexResponse> index(final IndexRequest request) {
        return client.index(request);
    }

    @Override
    public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        client.index(request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return client.prepareIndex();
    }

    @Override
    public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return client.update(request);
    }

    @Override
    public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        client.update(request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return client.prepareUpdate();
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(final String index, final String type, final String id) {
        return client.prepareUpdate(index, type, id);
    }

    @Override
    public IndexRequestBuilder prepareIndex(final String index, final String type) {
        return client.prepareIndex(index, type);
    }

    @Override
    public IndexRequestBuilder prepareIndex(final String index, final String type, final String id) {
        return client.prepareIndex(index, type, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return client.delete(request);
    }

    @Override
    public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        client.delete(request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return client.prepareDelete();
    }

    @Override
    public DeleteRequestBuilder prepareDelete(final String index, final String type, final String id) {
        return client.prepareDelete(index, type, id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return client.bulk(request);
    }

    @Override
    public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        client.bulk(request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return client.prepareBulk();
    }

    @Override
    public ActionFuture<DeleteByQueryResponse> deleteByQuery(final DeleteByQueryRequest request) {
        return client.deleteByQuery(request);
    }

    @Override
    public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {
        client.deleteByQuery(request, listener);
    }

    @Override
    public DeleteByQueryRequestBuilder prepareDeleteByQuery(final String... indices) {
        return client.prepareDeleteByQuery(indices);
    }

    @Override
    public ActionFuture<GetResponse> get(final GetRequest request) {
        return client.get(request);
    }

    @Override
    public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        client.get(request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return client.prepareGet();
    }

    @Override
    public GetRequestBuilder prepareGet(final String index, final String type, final String id) {
        return client.prepareGet(index, type, id);
    }

    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript() {
        return client.preparePutIndexedScript();
    }

    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript(final String scriptLang, final String id, final String source) {
        return client.preparePutIndexedScript(scriptLang, id, source);
    }

    @Override
    public void deleteIndexedScript(final DeleteIndexedScriptRequest request, final ActionListener<DeleteIndexedScriptResponse> listener) {
        client.deleteIndexedScript(request, listener);
    }

    @Override
    public ActionFuture<DeleteIndexedScriptResponse> deleteIndexedScript(final DeleteIndexedScriptRequest request) {
        return client.deleteIndexedScript(request);
    }

    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript() {
        return client.prepareDeleteIndexedScript();
    }

    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(final String scriptLang, final String id) {
        return client.prepareDeleteIndexedScript(scriptLang, id);
    }

    @Override
    public void putIndexedScript(final PutIndexedScriptRequest request, final ActionListener<PutIndexedScriptResponse> listener) {
        client.putIndexedScript(request, listener);
    }

    @Override
    public ActionFuture<PutIndexedScriptResponse> putIndexedScript(final PutIndexedScriptRequest request) {
        return client.putIndexedScript(request);
    }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript() {
        return client.prepareGetIndexedScript();
    }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript(final String scriptLang, final String id) {
        return client.prepareGetIndexedScript(scriptLang, id);
    }

    @Override
    public void getIndexedScript(final GetIndexedScriptRequest request, final ActionListener<GetIndexedScriptResponse> listener) {
        client.getIndexedScript(request, listener);
    }

    @Override
    public ActionFuture<GetIndexedScriptResponse> getIndexedScript(final GetIndexedScriptRequest request) {
        return client.getIndexedScript(request);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return client.multiGet(request);
    }

    @Override
    public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        client.multiGet(request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return client.prepareMultiGet();
    }

    @Override
    public ActionFuture<CountResponse> count(final CountRequest request) {
        return client.count(request);
    }

    @Override
    public void count(final CountRequest request, final ActionListener<CountResponse> listener) {
        client.count(request, listener);
    }

    @Override
    public CountRequestBuilder prepareCount(final String... indices) {
        return client.prepareCount(indices);
    }

    @Override
    public ActionFuture<ExistsResponse> exists(final ExistsRequest request) {
        return client.exists(request);
    }

    @Override
    public void exists(final ExistsRequest request, final ActionListener<ExistsResponse> listener) {
        client.exists(request, listener);
    }

    @Override
    public ExistsRequestBuilder prepareExists(final String... indices) {
        return client.prepareExists(indices);
    }

    @Override
    public ActionFuture<SuggestResponse> suggest(final SuggestRequest request) {
        return client.suggest(request);
    }

    @Override
    public void suggest(final SuggestRequest request, final ActionListener<SuggestResponse> listener) {
        client.suggest(request, listener);
    }

    @Override
    public SuggestRequestBuilder prepareSuggest(final String... indices) {
        return client.prepareSuggest(indices);
    }

    @Override
    public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return client.search(request);
    }

    @Override
    public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        client.search(request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(final String... indices) {
        return client.prepareSearch(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return client.searchScroll(request);
    }

    @Override
    public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        client.searchScroll(request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(final String scrollId) {
        return client.prepareSearchScroll(scrollId);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(final MultiSearchRequest request) {
        return client.multiSearch(request);
    }

    @Override
    public void multiSearch(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {
        client.multiSearch(request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return client.prepareMultiSearch();
    }

    @Override
    public ActionFuture<SearchResponse> moreLikeThis(final MoreLikeThisRequest request) {
        return client.moreLikeThis(request);
    }

    @Override
    public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        client.moreLikeThis(request, listener);
    }

    @Override
    public MoreLikeThisRequestBuilder prepareMoreLikeThis(final String index, final String type, final String id) {
        return client.prepareMoreLikeThis(index, type, id);
    }

    @Override
    public ActionFuture<TermVectorResponse> termVector(final TermVectorRequest request) {
        return client.termVector(request);
    }

    @Override
    public void termVector(final TermVectorRequest request, final ActionListener<TermVectorResponse> listener) {
        client.termVector(request, listener);
    }

    @Override
    public TermVectorRequestBuilder prepareTermVector() {
        return client.prepareTermVector();
    }

    @Override
    public TermVectorRequestBuilder prepareTermVector(final String index, final String type, final String id) {
        return client.prepareTermVector(index, type, id);
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        return client.multiTermVectors(request);
    }

    @Override
    public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        client.multiTermVectors(request, listener);
    }

    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return client.prepareMultiTermVectors();
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(final PercolateRequest request) {
        return client.percolate(request);
    }

    @Override
    public void percolate(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        client.percolate(request, listener);
    }

    @Override
    public PercolateRequestBuilder preparePercolate() {
        return client.preparePercolate();
    }

    @Override
    public ActionFuture<MultiPercolateResponse> multiPercolate(final MultiPercolateRequest request) {
        return client.multiPercolate(request);
    }

    @Override
    public void multiPercolate(final MultiPercolateRequest request, final ActionListener<MultiPercolateResponse> listener) {
        client.multiPercolate(request, listener);
    }

    @Override
    public MultiPercolateRequestBuilder prepareMultiPercolate() {
        return client.prepareMultiPercolate();
    }

    @Override
    public ExplainRequestBuilder prepareExplain(final String index, final String type, final String id) {
        return client.prepareExplain(index, type, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(final ExplainRequest request) {
        return client.explain(request);
    }

    @Override
    public void explain(final ExplainRequest request, final ActionListener<ExplainResponse> listener) {
        client.explain(request, listener);
    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return client.prepareClearScroll();
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(final ClearScrollRequest request) {
        return client.clearScroll(request);
    }

    @Override
    public void clearScroll(final ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        client.clearScroll(request, listener);
    }

    @Override
    public Settings settings() {
        return client.settings();
    }

    public interface OnConnectListener {
        void onConnect();
    }
}
