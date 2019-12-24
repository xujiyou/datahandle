package xujiyou.work;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.neo4j.driver.*;

/**
 * Neo4jHandle class
 *
 * @author jiyouxu
 * @date 2019/12/19
 */
public class Neo4jHandle {

    private Session session;

    public Neo4jHandle() {
//        Driver driver = GraphDatabase.driver( "bolt://10.28.109.69:7687", AuthTokens.basic( "neo4j", "bbd-neo4j" ) );
//        this.session = driver.session();
    }

    public void handleFollower(String followerStr) {
//        JSONObject jsonObject = JSON.parseObject(followerStr);
//        this.session.run("CREATE (follower:Follower " + jsonObject.toJSONString() +")");
    }

    public void handleUser(String userStr) {
//        JSONObject jsonObject = JSON.parseObject(userStr);
//        this.session.run("CREATE (follower:Follower " + jsonObject.toJSONString() +")");
    }
}
