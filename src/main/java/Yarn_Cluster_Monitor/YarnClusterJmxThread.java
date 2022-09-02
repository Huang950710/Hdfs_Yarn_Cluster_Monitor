package Yarn_Cluster_Monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class YarnClusterJmxThread extends Thread{
    private final Map<String, String> tags = new HashMap<>();
    private final Map<String, Object> fields = new HashMap<>();
    private String COMPANY;
    private String IP;

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName());
        while(true) {
            synchronized (YarnClusterJmxThread.class) {
                this.JMX();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public YarnClusterJmxThread(String company, String IP_1, String IP_2) {

        this.COMPANY = company;
        IP = YarnClusterMasterJudgement.Yarn_Cluster_Master_Judgement(IP_1, IP_2);
    }

    public void JMX() {

        /**
         * JMX解析地址
         */
        String url = "http://" + this.IP + ":8088/jmx";

        /**
         * 创建http解析方法对象
         */
        HttpsClient HC = new HttpsClient();

        /**
         * 获取链接对应的块信息
         */
        String message1 = HC.HttpsClient(url);

        /**
         * 将一行jSON数据转换为JSON对象
         */
        JSONObject jsonObject1 = JSON.parseObject(message1);

        /**
         * 将JSON对象转换为集合
         */
        JSONArray beans = jsonObject1.getJSONArray("beans");

        /**
         * tag值标记的数据是那个业务线的数据
         */
        tags.put("company", COMPANY);

        for (Object bean : beans) {
            //将数组中的每个集合转换为一个 字符串，在转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(bean.toString());
            if (jsonObject.get("name").equals("Hadoop:service=ResourceManager,name=QueueMetrics,q0=root")) {
                JSONObject jsonObject3 = JSON.parseObject(jsonObject.toString());
                Set<Map.Entry<String, Object>> entries = jsonObject3.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    /**
                     * 运行时间小于60分钟的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_0")) fields.put(entry.getKey(), entry.getValue());
                    /**
                     * 运行时间在60到300分钟之间的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_60")) fields.put(entry.getKey(), entry.getValue());
                    /**
                     * 运行时间在300到1440分钟之间的应用程序的当前数量
                     */
                    if (entry.getKey().equals("running_300")) fields.put(entry.getKey(), entry.getValue());
                    /**
                     * 当前运行的应用程序的运行时间超过1440分钟
                     */
                    if (entry.getKey().equals("running_1440")) fields.put(entry.getKey(), entry.getValue());
                }
            } else if (jsonObject.get("name").equals("Hadoop:service=ResourceManager,name=JvmMetrics")) {
                JSONObject jsonObject3 = JSON.parseObject(jsonObject.toString());
                Set<Map.Entry<String, Object>> entries = jsonObject3.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    /**
                     * JVM 运行时可以使用的最大内存大小 (MB)
                     */
                    if (entry.getKey().equals("MemMaxM")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * JVM 配置的 HeapMemory 的大小 (MB)
                     */
                    if (entry.getKey().equals("MemHeapMaxM")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前使用的堆内存（以 MB 为单位）
                     */
                    if (entry.getKey().equals("MemHeapUsedM")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 总 GC 计数
                     */
                    if (entry.getKey().equals("GcCount")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 以毫秒为单位的总 GC 时间
                     */
                    if (entry.getKey().equals("GcTimeMillis")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前使用的非堆内存（以 MB 为单位）
                     */
                    if (entry.getKey().equals("MemNonHeapUsedM")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前提交的非堆内存（以 MB 为单位）
                     */
                    if (entry.getKey().equals("MemNonHeapCommittedM"))
                        fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 线程运行数量
                     */
                    if (entry.getKey().equals("ThreadsRunnable")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 线程阻塞数量
                     */
                    if (entry.getKey().equals("ThreadsBlocked")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 线程等待数量
                     */
                    if (entry.getKey().equals("ThreadsWaiting")) fields.put("JMX." + entry.getKey(), entry.getValue());
                }
            } else if (jsonObject.get("name").equals("Hadoop:service=ResourceManager,name=ClusterMetrics")) {
                JSONObject jsonObject3 = JSON.parseObject(jsonObject.toString());
                Set<Map.Entry<String, Object>> entries = jsonObject3.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    /**
                     * 当前活动 NodeManager 的数量
                     */
                    if (entry.getKey().equals("NumActiveNMs")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 正在退役的 NodeManager 的当前数量
                     */
                    if (entry.getKey().equals("numDecommissioningNMs"))
                        fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 退役 NodeManager 的当前数量
                     */
                    if (entry.getKey().equals("NumDecommissionedNMs"))
                        fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前被优雅关闭的 NodeManager 数量。注意，这并不包括被强制关闭的 NodeManager。
                     */
                    if (entry.getKey().equals("NumShutdownNMs")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前因为没有发送心跳而丢失的 NodeManager 数量。
                     */
                    if (entry.getKey().equals("NumLostNMs")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前不健康 NodeManager 的数量
                     */
                    if (entry.getKey().equals("NumUnhealthyNMs")) fields.put("JMX." + entry.getKey(), entry.getValue());
                    /**
                     * 当前重新启动的 NodeManager 数量
                     */
                    if (entry.getKey().equals("NumRebootedNMs")) fields.put("JMX." + entry.getKey(), entry.getValue());
                }
            }
        }
        Infludb_client.setup().insert(tags, fields, "HDFS_CLUSTER_YARN_JMX");
        Infludb_client.setup().close();
        tags.clear();
        fields.clear();
    }
}
