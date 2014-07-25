package org.codelibs.elasticsearch.web.service;

import java.io.IOException;

import org.codelibs.elasticsearch.web.config.RiverConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.seasar.framework.container.SingletonS2Container;
import org.seasar.framework.container.factory.SingletonS2ContainerFactory;

public class S2ContainerService extends
        AbstractLifecycleComponent<S2ContainerService> {
    private static final ESLogger logger = Loggers
            .getLogger(S2ContainerService.class);

    private Client client;

    private ScriptService scriptService;

    @Inject
    public S2ContainerService(final Settings settings, final Client client,
            final ClusterService clusterService,
            final ScriptService scriptService) {
        super(settings);
        this.client = client;
        this.scriptService = scriptService;

        logger.info("Creating S2Container...");

        SingletonS2ContainerFactory.init();

        clusterService.addLifecycleListener(new LifecycleListener() {

            @Override
            public void beforeStop() {
            }

            @Override
            public void beforeStart() {
            }

            @Override
            public void beforeClose() {
            }

            @Override
            public void afterStop() {
            }

            @Override
            public void afterStart() {
                client.admin().cluster().prepareHealth()
                        .setWaitForYellowStatus()
                        .execute(new ActionListener<ClusterHealthResponse>() {
                            @Override
                            public void onResponse(
                                    final ClusterHealthResponse response) {
                                if (response.getStatus() == ClusterHealthStatus.RED) {
                                    logger.warn("The cluster is not available. The status is RED.");
                                } else {
                                    createRobotIndex();
                                }
                            }

                            @Override
                            public void onFailure(final Throwable e) {
                                logger.warn("The cluster is not available.", e);
                            }
                        });
            }

            @Override
            public void afterClose() {
            }
        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("Starting S2Container...");

        final RiverConfig riverConfig = SingletonS2Container
                .getComponent(RiverConfig.class);
        riverConfig.setClient(client);
        riverConfig.setScriptService(scriptService);
    }

    private void createRobotIndex() {
        final String robotIndexName = SingletonS2Container
                .getComponent("robotIndexName");
        final IndicesExistsResponse indicesExistsResponse = client.admin()
                .indices().prepareExists(robotIndexName).execute().actionGet();
        if (!indicesExistsResponse.isExists()) {
            final CreateIndexResponse createIndexResponse = client.admin()
                    .indices().prepareCreate(robotIndexName).execute()
                    .actionGet();
            if (createIndexResponse.isAcknowledged()) {
                try {
                    createMapping(robotIndexName, "queue", createQueueMapping());
                    createMapping(robotIndexName, "filter",
                            createFilterMapping());
                    createMapping(robotIndexName, "data", createDataMapping());
                } catch (final IOException e) {
                    logger.error("Failed to create a mapping.", e);
                }
            } else {
                logger.warn("Failed to create " + robotIndexName);
            }
        }
    }

    private void createMapping(final String robotIndexName, final String type,
            final XContentBuilder builder) {
        final PutMappingResponse queueMappingResponse = client.admin()
                .indices().preparePutMapping(robotIndexName).setType(type)
                .setSource(builder).execute().actionGet();
        if (!queueMappingResponse.isAcknowledged()) {
            logger.warn("Failed to create " + type + " mapping.");
        }
    }

    private XContentBuilder createQueueMapping() throws IOException {
        return XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("queue")//
                .startObject("properties")//

                // createTime
                .startObject("createTime")//
                .field("type", "date")//
                .field("format", "dateOptionalTime")//
                .endObject()//
                // depth
                .startObject("depth")//
                .field("type", "long")//
                .endObject()//
                // lastModified
                .startObject("lastModified")//
                .field("type", "date")//
                .field("format", "dateOptionalTime")//
                .endObject()//
                // method
                .startObject("method")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // parentUrl
                .startObject("parentUrl")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // sessionId
                .startObject("sessionId")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // url
                .startObject("url")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
    }

    private XContentBuilder createFilterMapping() throws IOException {
        return XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("filter")//
                .startObject("properties")//

                // filterType
                .startObject("filterType")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // sessionId
                .startObject("sessionId")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // url
                .startObject("url")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
    }

    private XContentBuilder createDataMapping() throws IOException {
        return XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("data")//
                .startObject("properties")//

                // contentLength
                .startObject("contentLength")//
                .field("type", "long")//
                .endObject()//
                // createTime
                .startObject("createTime")//
                .field("type", "date")//
                .field("format", "dateOptionalTime")//
                .endObject()//
                // executionTime
                .startObject("executionTime")//
                .field("type", "long")//
                .endObject()//
                // httpStatusCode
                .startObject("httpStatusCode")//
                .field("type", "long")//
                .endObject()//
                // lastModified
                .startObject("lastModified")//
                .field("type", "date")//
                .field("format", "dateOptionalTime")//
                .endObject()//
                // method
                .startObject("method")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // mimeType
                .startObject("mimeType")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // parentUrl
                .startObject("parentUrl")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // ruleId
                .startObject("ruleId")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // sessionId
                .startObject("sessionId")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // status
                .startObject("status")//
                .field("type", "long")//
                .endObject()//
                // url
                .startObject("url")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//

                // accessResultData
                .startObject("accessResultData")//
                // properties
                .startObject("properties")//

                // transformerName
                .startObject("transformerName")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//

                .endObject()//
                .endObject()//

                .endObject()//
                .endObject()//
                .endObject();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping S2Container...");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("Closing S2Container...");

        SingletonS2ContainerFactory.destroy();
    }

}
