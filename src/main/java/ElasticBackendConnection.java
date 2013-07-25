import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
/*
 *
 * @author arnoststedry, @date 7/24/13 5:19 PM
 */
public class ElasticBackendConnection {
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticBackendConnection.class);
    private Client client = null;
    private Node node;
    public Client client() {
        synchronized(ElasticBackendConnection.class) {
            startIfNeed();
            return client;
        }
    }

    public void startIfNeed() {
        synchronized (ElasticBackendConnection.class) {
            if (null != client) {
                return;
            }
                prepareIndex(Document.EC_LANG_INDEX, Document.EC_LANG_ANALYSER);
        }
    }

    private void prepareIndex(final String indexName, final String analyzerName) {
        try {
            final ImmutableSettings.Builder settings = prepareSettings();
            node = prepareNode(settings);
            node.start();

            client = node.client();

            final IndicesAdminClient indices = client.admin().indices();

            createIndex(indexName, analyzerName, indices);
            Thread.sleep(1000);
            putMapping(indices, indexName, analyzerName);
            Thread.sleep(1000);

        } catch (IndexAlreadyExistsException ae) {
            logger.warn("Index seems to already exist", ae);

        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    private ImmutableSettings.Builder prepareSettings() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        settings.put("client.transport.sniff", true);

            settings.put("path.data", "/tmp/elastic/data");


            settings.put("path.logs", "/tmp/elastic/logs");

        settings.build();
        return settings;
    }

    private Node prepareNode(ImmutableSettings.Builder settings) {
        NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
        return nb.node();
    }

    private void createIndex(String indexName, String analyzerName, IndicesAdminClient indices) throws IOException {
        //Tricky part. must define own analyser based on origin czech and adding asciifolding
        String myAnalyserName="my_"+analyzerName;
        String setting = jsonBuilder()
                .startObject()
                    .startObject("analysis")
                         .startObject("analyzer")
                            .startObject(myAnalyserName)
                                .field("type", "custom")
                                .field("tokenizer", "standard")
                                .array("filter", "standard", "lowercase", "my_stemmer_" + analyzerName, "asciifolding")
                            .endObject()
                         .endObject()
                         .startObject("filter")
                            .startObject("my_stemmer_" + analyzerName)
                                .field("type", "stemmer")
                                .field("name", analyzerName)
                            .endObject()
                         .endObject()
                     .endObject()
                .endObject()
                .string();
        indices.prepareCreate(indexName).setSettings(ImmutableSettings.settingsBuilder().loadFromSource(
                setting
        )).execute().actionGet();

    }

    private void putMapping(IndicesAdminClient indices, String indexName, String analyzerName) throws Exception {
        String myAnalyserName="my_"+analyzerName;
        indices.preparePutMapping(indexName).setType(Document.EC_DOCUMENT_TYPE).setSource(mapping(myAnalyserName)).execute().actionGet();
    }

    private XContentBuilder mapping(String analyzerName) throws Exception {
        return jsonBuilder()
                .startObject()
                    .startObject(Document.EC_DOCUMENT_TYPE)
                        .startObject("properties")
                            .startObject(Document.EC_TITLE_FIELD)
                                .field("type", "string")
                                .field("analyzer", analyzerName)
                                .field("filter", "asciifolding")
                            .endObject()
                            .startObject(Document.EC_BODY_FIELD)
                                 .field("type", "string")
                                 .field("analyzer", analyzerName)
                                 .field("filter", "asciifolding")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }

    public void index(Document document) throws IOException {

        final XContentBuilder sourceBuilder = jsonBuilder()
                .startObject()
                .field(Document.EC_BODY_FIELD, document.getBody())
                .field(Document.EC_TITLE_FIELD, document.getTitle())
                .endObject();

        final IndexRequestBuilder requestBuilder = indexRequestBuilder(document, sourceBuilder);

        final Client client = client();
        final IndexRequest request = requestBuilder.request();
        final ActionFuture<IndexResponse> responseActionFuture = client.index(request);
        final IndexResponse response = responseActionFuture.actionGet();
        refresh();
    }

    private IndexRequestBuilder indexRequestBuilder(Document document, XContentBuilder sourceBuilder) {
        final IndexRequestBuilder requestBuilder = new IndexRequestBuilder(client());
        requestBuilder.setSource(sourceBuilder);
        requestBuilder.setIndex(document.EC_LANG_INDEX);
        requestBuilder.setType(document.EC_DOCUMENT_TYPE);
        return requestBuilder;
    }

    private void refresh() {
            client().admin().indices().refresh(new RefreshRequest(Document.EC_LANG_INDEX)).actionGet();

    }



    public SearchHits search(String... someText) {
        final QueryBuilder queryBuilder = commonSearchQueryBuilder(someText);

        return client().prepareSearch(Document.EC_LANG_INDEX).setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(queryBuilder)
                .setFrom(0)
                .setSize(100)
                .setExplain(false)
                .execute().actionGet().hits();
    }

    private QueryBuilder commonSearchQueryBuilder(String... someText) {
        final StringBuilder sb = new StringBuilder();
        for (final String s : someText) {
            sb.append(s).append(' ');
        }

        return queryString(sb.toString())
                .field(Document.EC_BODY_FIELD, 1)
                .field(Document.EC_TITLE_FIELD, 5);
    }

    public void stop() {
        synchronized (ElasticBackendConnection.class) {
            if (null != client) {
                node.close();
                node = null;
                client = null;
            }
        }
    }



    public void deleteData() {
        synchronized (ElasticBackendConnection.class) {
            if (null != client) {
                try {
                        client.admin().indices().delete(new DeleteIndexRequest(Document.EC_LANG_INDEX)).actionGet();

                }   catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
