package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

public class ShowKeyWords extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Map<String, Object> map = new HashMap<>();
        try {
            if (req.getParameter("q") == null || req.getParameter("q").isEmpty()) {
                map.putIfAbsent("errors", new ArrayList<>());
                ((List) map.get("errors")).add("Please specify a query");
            } else {
                String rowKey = req.getParameter("q");

                map.put("query", req.getParameter("q"));

                String hbaseTableName = "hostKeyWords";
                String hbaseColumnFamily = "K";


                TableName tn = TableName.valueOf(hbaseTableName);
                try {
                    Table table = HBaseClient.getConnection().getTable(tn);
                    Get get = new Get(Bytes.toBytes(rowKey));
                    get.addFamily(Bytes.toBytes(hbaseColumnFamily));
                    Result result = table.get(get);
                    List<Map> resultList = new ArrayList<>();
                    for (Iterator<Cell> it = result.listCells().iterator(); it.hasNext(); ) {
                        Cell cell = it.next();
                        String word = Bytes.toString(CellUtil.cloneQualifier(cell));
                        int count = Bytes.toInt(CellUtil.cloneValue(cell));
                        Map<String, Object> data = new HashMap<>();
                        data.put("key", word);
                        data.put("count", count);
                        resultList.add(data);
                    }

                    if (resultList.isEmpty()) {
                        map.putIfAbsent("errors", new ArrayList<>());
                        ((List) map.get("errors")).add("No Result!");
                    } else
                        map.put("result", resultList);
                    table.close();

                } catch (IOException e) {
                    map.putIfAbsent("errors", new ArrayList<>());
                    ((List) map.get("errors")).add(e.getMessage());
                }
            }
        } catch (Exception e) {
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
