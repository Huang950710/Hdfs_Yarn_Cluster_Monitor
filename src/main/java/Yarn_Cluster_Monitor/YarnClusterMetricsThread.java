package Yarn_Cluster_Monitor;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class YarnClusterMetricsThread extends Thread {

    /**
     * tags ：influxdb数据库 tag 数据
     * fields，fields2 存放 influxdb 的 fields 数据
     */
    private final Map<String, String> tags = new HashMap<>();
    private final Map<String, Object> fields = new HashMap<>();
    private String COMPANY;
    private String IP;

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName());
        while(true) {
            synchronized (YarnClusterMetricsThread.class) {
                this.YARN_CLUSTER_METRICS_JSON();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public YarnClusterMetricsThread(String COMPANY, String IP1, String IP2) {
        this.COMPANY = COMPANY;
        /**
         * 主节点判断
         */
        IP = YarnClusterMasterJudgement.Yarn_Cluster_Master_Judgement(IP1, IP2);
    }

    public void YARN_CLUSTER_METRICS_JSON() {

        String url1 = "http://" + this.IP + ":8088/ws/v1/cluster/metrics";

        // 创建http解析方法对象
        HttpsClient HC = new HttpsClient();
        // 获取链接对应的块信息
        String message1 = null;
        message1 = HC.HttpsClient(url1);

        //将一行 jSON 数据转换为 JSON 对象
        JSONObject jsonObject1 = JSON.parseObject(message1);

        //因为 clusterInfo 在JSON数据中是集合{}，所有用 JSON 对象的 getJSONObject 来获取内容
        JSONObject clusterInfo1 = jsonObject1.getJSONObject("clusterMetrics");

        /**
         * tag值标记的数据是那个业务线的数据
         */
        tags.put("company", this.COMPANY);

        /**
         * app提交数量
         */
        fields.put("appsSubmitted", clusterInfo1.get("appsSubmitted"));
        /**
         * app完成数量
         */
        fields.put("appsCompleted", clusterInfo1.get("appsCompleted"));
        /**
         * app等待数量
         */
        fields.put("appsPending", clusterInfo1.get("appsPending"));
        /**
         * app的运行数量
         */
        fields.put("appsRunning", clusterInfo1.get("appsRunning"));
        /**
         * app失败数量
         */
        fields.put("appsFailed", clusterInfo1.get("appsFailed"));
        /**
         * app被kill的数量
         */
        fields.put("appsKilled", clusterInfo1.get("appsKilled"));
        /**
         * 以 MB 为单位的总内存量
         */
        fields.put("totalMB", clusterInfo1.get("totalMB"));
        /**
         * 保留的内存量（以 MB 为单位）
         */
        fields.put("reservedMB", clusterInfo1.get("reservedMB"));
        /**
         * 可用内存MB
         */
        fields.put("availableMB", clusterInfo1.get("availableMB"));
        /**
         * 分配的容器数量
         */
        fields.put("containersAllocated", clusterInfo1.get("containersAllocated"));
        /**
         * 挂起的容器数量
         */
        fields.put("containersPending", clusterInfo1.get("containersPending"));
        /**
         * 节点总数
         */
        fields.put("totalNodes", clusterInfo1.get("totalNodes"));
        /**
         * 活动节点数量
         */
        fields.put("activeNodes", clusterInfo1.get("activeNodes"));
        /**
         * 丢失节点数量
         */
        fields.put("lostNodes", clusterInfo1.get("lostNodes"));
        /**
         * 不正常节点数量
         */
        fields.put("unhealthyNodes", clusterInfo1.get("unhealthyNodes"));
        /**
         * 已退役节点数量
         */
        fields.put("decommissionedNodes", clusterInfo1.get("decommissionedNodes"));
        /**
         * 虚拟核心总数
         */
        fields.put("totalVirtualCores", clusterInfo1.get("totalVirtualCores"));
        /**
         * 分配的虚拟核心数
         */
        fields.put("allocatedVirtualCores", clusterInfo1.get("allocatedVirtualCores"));
        /**
         * 可用虚拟核心数
         */
        fields.put("availableVirtualCores", clusterInfo1.get("availableVirtualCores"));
        /**
         * 以 MB 为单位分配的内存量
         */
        fields.put("allocatedMB", clusterInfo1.get("allocatedMB"));
        /**
         * 重启节点
         */
        fields.put("rebootedNodes", clusterInfo1.get("rebootedNodes"));
        /**
         * 关闭节点
         */
        fields.put("shutdownNodes", clusterInfo1.get("shutdownNodes"));

        Infludb_client.setup().insert(tags, fields, "HDFS_CLUSTER_YARN_METRICS");
        Infludb_client.setup().close();
        tags.clear();
        fields.clear();
    }
}
