package xujiyou.work;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;

/**
 * ElasticsearchHandle class
 *
 * @author jiyouxu
 * @date 2019/12/19
 */
public class ElasticsearchHandle {

    private RestHighLevelClient client;

    ElasticsearchHandle() {
         this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("fueltank-4", 9200, "http")));
    }

    public void handleFollower(String followerStr) {
        JSONObject jsonObject = JSON.parseObject(followerStr);
        IndexRequest indexRequest = new IndexRequest("follower").source(jsonObject.toJavaObject(Map.class));
        try {
            IndexResponse indexResponse = this.client.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println(indexResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handleUser(String userStr) {
        JSONObject jsonObject = JSON.parseObject(userStr);
        IndexRequest indexRequest = new IndexRequest("user_detail").source(jsonObject.toJavaObject(Map.class));
        try {
            IndexResponse indexResponse = this.client.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println(indexResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
