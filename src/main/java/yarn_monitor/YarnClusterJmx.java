package yarn_monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class YarnClusterJmx {

    public static void YARN_CLUSTER_JMX_JSON(String IP1, String IP2){

        //"http://13.213.130.248:8088/jmx"
        String url1="http://"+ IP1 +":8088/jmx";
        String url2="http://"+ IP2 +":8088/jmx";

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

        JSONArray beans = jsonObject1.getJSONArray("beans");
        for (Object bean : beans) {
            //将数组中的每个集合转换为一个 字符串，在转换为 JSON 对象 ，
            JSONObject jsonObject = JSON.parseObject(bean.toString());
            if (jsonObject.get("name").equals("Hadoop:service=ResourceManager,name=QueueMetrics,q0=root")){
                JSONObject jsonObject3 = JSON.parseObject(jsonObject.toString());
                Set<Map.Entry<String, Object>> entries = jsonObject3.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    /**
                     * 运行时间小于60分钟的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_0")) YarnMonitorEntrance.fields1.put(entry.getKey(),entry.getValue());
                    /**
                     * 运行时间在60到300分钟之间的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_60")) YarnMonitorEntrance.fields1.put(entry.getKey(),entry.getValue());
                    /**
                     * 运行时间在300到1440分钟之间的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_300")) YarnMonitorEntrance.fields1.put(entry.getKey(),entry.getValue());
                    /**
                     * 当前运行的应用程序的运行时间超过1440分钟
                     */
                    if (entry.getKey().equals("running_1440")) YarnMonitorEntrance.fields1.put(entry.getKey(),entry.getValue());
                }
            }
        }

        JSONArray beans1 = jsonObject2.getJSONArray("beans");
        for (Object bean : beans1) {
            //将数组中的每个集合转换为一个 字符串，在转换为 JSON 对象 ，
            JSONObject jsonObject = JSON.parseObject(bean.toString());
            if (jsonObject.get("name").equals("Hadoop:service=ResourceManager,name=QueueMetrics,q0=root")){
                JSONObject jsonObject3 = JSON.parseObject(jsonObject.toString());
                Set<Map.Entry<String, Object>> entries = jsonObject3.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    /**
                     * 运行时间小于60分钟的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_0")) YarnMonitorEntrance.fields2.put(entry.getKey(),entry.getValue());
                    /**
                     * 运行时间在60到300分钟之间的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_60")) YarnMonitorEntrance.fields2.put(entry.getKey(),entry.getValue());
                    /**
                     * 运行时间在300到1440分钟之间的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_300")) YarnMonitorEntrance.fields2.put(entry.getKey(),entry.getValue());
                    /**
                     * 当前运行的应用程序的运行时间超过1440分钟
                     */
                    if (entry.getKey().equals("running_1440")) YarnMonitorEntrance.fields2.put(entry.getKey(),entry.getValue());
                }
            }
        }

    }
}
