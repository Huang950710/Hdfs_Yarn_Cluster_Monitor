package yarn_monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ClusterInfo ： 集群信息
 */
public class YarnClusterInfo {

    public static void YARN_CLUSTER_INFO_JSON(String IP1, String IP2) {

        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        String datetime = time.format(date);
        YarnMonitorEntrance.fields1.put("timestamp",datetime);
        YarnMonitorEntrance.fields2.put("timestamp",datetime);


        String url1="http://"+ IP1 +":8088/ws/v1/cluster/info";
        String url2="http://"+ IP2 +":8088/ws/v1/cluster/info";


        // 创建http解析方法对象
        HttpsClient HC = new HttpsClient();
        // 获取链接对应的块信息
        String message1 = null;
        String message2 = null;
        message1 = HC.HttpsClient(url1);
        message2 = HC.HttpsClient(url2);

        //将一行 jSON 数据转换为 JSON 对象
        JSONObject jsonObject1 = JSON.parseObject(message1);
        JSONObject jsonObject2 = JSON.parseObject(message2);

        //因为 clusterInfo 在JSON数据中是集合{}，所有用 JSON 对象的 getJSONObject 来获取内容
        JSONObject clusterInfo1 = jsonObject1.getJSONObject("clusterInfo");
        JSONObject clusterInfo2 = jsonObject2.getJSONObject("clusterInfo");


        /**
         * IP地址
         */
        YarnMonitorEntrance.fields1.put("IP",IP1);
        YarnMonitorEntrance.fields2.put("IP",IP2);
        /**
         * ResourceManager 状态有效值是: NOTINITED、 INITED、 STARTED（）、 STOPPED
         */
        int a;
        if (clusterInfo1.get("state").equals("STARTED")){
            a = 1;
        }else{
            a = 0;
        }
        int b;
        if (clusterInfo2.get("state").equals("STARTED")){
            b = 1;
        }else{
            b = 0;
        }
        YarnMonitorEntrance.fields1.put("state",a);
        YarnMonitorEntrance.fields2.put("state",b);
        /**
         * ResourceManagerHA 状态有效值是: INITIALIZING、 Aactive、 STANDBY、 STOPPED
         */
        YarnMonitorEntrance.fields1.put("haState",clusterInfo1.get("haState"));
        YarnMonitorEntrance.fields2.put("haState",clusterInfo2.get("haState"));
        /**
         * 集群开始的时间(以 ms 为单位，从纪元开始)
         * 时间戳转换：time.format(Long.parseLong(clusterInfo1.get("startedOn").toString()))
         */
        YarnMonitorEntrance.fields1.put("startedOn",time.format(Long.parseLong(clusterInfo1.get("startedOn").toString())));
        YarnMonitorEntrance.fields2.put("startedOn",time.format(Long.parseLong(clusterInfo2.get("startedOn").toString())));

    }
}
