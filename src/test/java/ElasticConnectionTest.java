import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
/*
 *
 * @author arnoststedry, @date 7/24/13 5:19 PM
 */
public class ElasticConnectionTest {

  private ElasticBackendConnection backendConnection = new ElasticBackendConnection();

    public @Before
    void setUp() throws Exception {
        backendConnection.startIfNeed();
    }

    public @Test
    void backendHasClient() {
        assertNotNull(backendConnection.client());
    }

    public @Test void backendDoesNotCrashWhenStartedMultipleTimes() {
        final Client client = backendConnection.client();
        backendConnection.startIfNeed();
        backendConnection.startIfNeed();
        backendConnection.startIfNeed();
        backendConnection.startIfNeed();
        assertSame(client, backendConnection.client());
    }

    private void indexSampleDocs() throws IOException {
        final Document document1 = new Document();
        document1.setBody("some text");

        final Document document2 = new Document();
        document2.setBody("other body");

        final Document document3 = new Document();
        document3.setBody("other body");


        backendConnection.index(document1);
        backendConnection.index(document2);
        backendConnection.index(document3);
    }

    private void indexPrilisZlutouckyKun() throws IOException {
        final Document document4 = new Document();
        document4.setBody("Příliš žluťoučký kůň úpěl ďábelské ódy ");


        backendConnection.index(document4);
    }
    public @Test void czechTextWithDiacriticsIsFound() throws IOException {
        indexSampleDocs();
        indexPrilisZlutouckyKun();
        final SearchHits searchHits = backendConnection.search("ďábelský");
        assertEquals(1, searchHits.hits().length);
    }

    public @Test void czechTextWithDiacriticsIsFoundIfSearchedWithoutDiacritics() throws IOException {
        indexSampleDocs();
        indexPrilisZlutouckyKun();

        final SearchHits searchHits = backendConnection.search("dabelsky");
        assertEquals(1, searchHits.hits().length);
    }

    public @After
    void tearDown() throws Exception {
        backendConnection.deleteData();
        backendConnection.stop();
    }
}
