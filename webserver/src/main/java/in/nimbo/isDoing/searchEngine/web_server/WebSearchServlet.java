package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebSearchServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Map<String, Object> map = new HashMap<>();
        try {
            if (req.getParameter("q") == null || req.getParameter("q").isEmpty()) {
                map.putIfAbsent("errors", new ArrayList<>());
                ((List) map.get("errors")).add("Please specify a query");
            } else {
                map.put("query", req.getParameter("q"));
                SearchRequest searchRequest = new SearchRequest(Engine.getConfigs().get("elastic.search.index"));
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                QueryBuilder textQuery = QueryBuilders.matchQuery("text", req.getParameter("q"))
                        .fuzziness(Fuzziness.AUTO)
                        .maxExpansions(2);
                QueryBuilder titleQuery = QueryBuilders.matchQuery("title", req.getParameter("q"))
                        .fuzziness(Fuzziness.AUTO)
                        .maxExpansions(2)
                        .boost(2);
                QueryBuilder linkQuery = QueryBuilders.matchQuery("url", req.getParameter("q"))
                        .fuzziness(Fuzziness.AUTO)
                        .maxExpansions(2)
                        .boost(3);
                searchSourceBuilder.query(QueryBuilders.boolQuery().should(textQuery).should(titleQuery).should(linkQuery));
                searchSourceBuilder.size(20);
                searchRequest.source(searchSourceBuilder);
                SearchResponse searchResponse = ElasticClient.getClient().search(searchRequest);
                long hits = searchResponse.getHits().totalHits;
                TimeValue took = searchResponse.getTook();
                map.put("took", took.getSeconds());
                map.put("hits", hits);
                List<Map<String, Object>> resultList = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits()) {
                    Map<String, Object> result = new HashMap<>();
                    result.put("docId", hit.getId());
                    result.put("score", hit.getScore());
                    result.put("text", ((String) hit.getSourceAsMap().get("text")).substring(0, 250));
                    result.put("url", hit.getSourceAsMap().get("url"));
                    result.put("title", hit.getSourceAsMap().get("title"));
                    resultList.add(result);
                }
                map.put("result", resultList);
            }
        } catch (IOException e) {
            map.putIfAbsent("errors", new ArrayList<>());
            ((List) map.get("errors")).add(e.getMessage());
        }
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(map);
        resp.setContentType("application/json");
        resp.setContentLength(json.getBytes().length);
        resp.getWriter().print(json);
    }
}
