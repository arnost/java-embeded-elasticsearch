/**
 * Created with IntelliJ IDEA.
 * User: arnoststedry
 * Date: 7/25/13
 * Time: 9:35 AM
 */

//Simple document to index
public class Document {
    public static final String EC_DOCUMENT_TYPE = "my_document";
    public static final String EC_LANG_INDEX ="index_cs" ;
    public static final String EC_TITLE_FIELD = "title";
    public static final String EC_BODY_FIELD = "body";
    public static final String EC_LANG_ANALYSER = "czech";
    private String body;
    private String title;

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
