package in.nimbo.isDoing.searchEngine.web_server.controller;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class WebSearch implements WebController {
    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            if (request.getParameter("q") == null) {
                    response.getWriter().print("Welcome");
            } else {
                response.getWriter().println("Query: " + request.getParameter("q"));
                SearchRequest searchRequest = new SearchRequest(Engine.getConfigs().get("crawler.persister.db.elastic.index"));
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                QueryBuilder textQuery = QueryBuilders.matchQuery("text", request.getParameter("q"))
                        .fuzziness(Fuzziness.AUTO)
                        .maxExpansions(2);
                QueryBuilder titleQuery = QueryBuilders.matchQuery("title", request.getParameter("q"))
                        .fuzziness(Fuzziness.AUTO)
                        .maxExpansions(2)
                        .boost(3);
                QueryBuilder linkQuery = QueryBuilders.matchQuery("url", request.getParameter("q"))
                        .fuzziness(Fuzziness.AUTO)
                        .maxExpansions(2)
                        .boost(5);
                searchSourceBuilder.query(QueryBuilders.boolQuery().should(textQuery).should(titleQuery).should(linkQuery)).timeout(TimeValue.timeValueSeconds(2));
//                searchSourceBuilder.query(QueryBuilders.matchAllQuery()).timeout(TimeValue.timeValueSeconds(2));
                searchRequest.source(searchSourceBuilder);
                SearchResponse searchResponse = ElasticClient.getClient().search(searchRequest);
                long hits = searchResponse.getHits().totalHits;
                TimeValue took = searchResponse.getTook();
                PrintWriter out = response.getWriter();
                out.println("Query took { " + took + " } miliseconds");
                out.println("Query find { " + hits + " } results");
                out.println("<hr/>");
                for (SearchHit hit : searchResponse.getHits()) {
                    out.println("docId: " + hit.docId() + "  - Score:  " + hit.getScore());
                    out.println("<h3 style='padding:0;margin:0;'>" + hit.getSourceAsMap().get("title") + "</h3>");
                    out.println("<span style='color:green;font-weight:bold;'>" + hit.getSourceAsMap().get("url") + "</span>");
                    out.println("<div>" + hit.getSourceAsMap().get("text") + "</div>");
                    out.println("<hr/>");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            e.printStackTrace(response.getWriter());
        }
    }
}
