package yarn_monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

public class YarnClusterMetrics {

    public static void YARN_CLUSTER_METRICS_JSON(String IP1, String IP2){

        String url1="http://"+ IP1 +":8088/ws/v1/cluster/metrics";
        String url2="http://"+ IP2 +":8088/ws/v1/cluster/metrics";

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
        JSONObject clusterInfo1 = jsonObject1.getJSONObject("clusterMetrics");
        JSONObject clusterInfo2 = jsonObject2.getJSONObject("clusterMetrics");


        /**
         * app提交数量
         */
        YarnMonitorEntrance.fields1.put("appsSubmitted",clusterInfo1.get("appsSubmitted"));
        YarnMonitorEntrance.fields2.put("appsSubmitted",clusterInfo2.get("appsSubmitted"));
        /**
         * app完成数量
         */
        YarnMonitorEntrance.fields1.put("appsCompleted",clusterInfo1.get("appsCompleted"));
        YarnMonitorEntrance.fields2.put("appsCompleted",clusterInfo2.get("appsCompleted"));
        /**
         * app等待数量
         */
        YarnMonitorEntrance.fields1.put("appsPending",clusterInfo1.get("appsPending"));
        YarnMonitorEntrance.fields2.put("appsPending",clusterInfo2.get("appsPending"));
        /**
         * app的运行数量
         */
        YarnMonitorEntrance.fields1.put("appsRunning",clusterInfo1.get("appsRunning"));
        YarnMonitorEntrance.fields2.put("appsRunning",clusterInfo2.get("appsRunning"));
        /**
         * app失败数量
         */
        YarnMonitorEntrance.fields1.put("appsFailed",clusterInfo1.get("appsFailed"));
        YarnMonitorEntrance.fields2.put("appsFailed",clusterInfo2.get("appsFailed"));
        /**
         * app被kill的数量
         */
        YarnMonitorEntrance.fields1.put("appsKilled",clusterInfo1.get("appsKilled"));
        YarnMonitorEntrance.fields2.put("appsKilled",clusterInfo2.get("appsKilled"));
        /**
         * 以 MB 为单位的总内存量
         */
        YarnMonitorEntrance.fields1.put("totalMB",clusterInfo1.get("totalMB"));
        YarnMonitorEntrance.fields2.put("totalMB",clusterInfo2.get("totalMB"));
        /**
         * 保留的内存量（以 MB 为单位）
         */
        YarnMonitorEntrance.fields1.put("reservedMB",clusterInfo1.get("reservedMB"));
        YarnMonitorEntrance.fields2.put("reservedMB",clusterInfo2.get("reservedMB"));
        /**
         * 可用内存MB
         */
        YarnMonitorEntrance.fields1.put("availableMB",clusterInfo1.get("availableMB"));
        YarnMonitorEntrance.fields2.put("availableMB",clusterInfo2.get("availableMB"));
        /**
         * 分配的容器数量
         */
        YarnMonitorEntrance.fields1.put("containersAllocated",clusterInfo1.get("containersAllocated"));
        YarnMonitorEntrance.fields2.put("containersAllocated",clusterInfo2.get("containersAllocated"));
        /**
         * 挂起的容器数量
         */
        YarnMonitorEntrance.fields1.put("containersPending",clusterInfo1.get("containersPending"));
        YarnMonitorEntrance.fields2.put("containersPending",clusterInfo2.get("containersPending"));
        /**
         *
         */
        YarnMonitorEntrance.fields1.put("containersPending",clusterInfo1.get("containersPending"));
        YarnMonitorEntrance.fields2.put("containersPending",clusterInfo2.get("containersPending"));
        /**
         * 节点总数
         */
        YarnMonitorEntrance.fields1.put("totalNodes",clusterInfo1.get("totalNodes"));
        YarnMonitorEntrance.fields2.put("totalNodes",clusterInfo2.get("totalNodes"));
        /**
         * 活动节点数量
         */
        YarnMonitorEntrance.fields1.put("activeNodes",clusterInfo1.get("activeNodes"));
        YarnMonitorEntrance.fields2.put("activeNodes",clusterInfo2.get("activeNodes"));
        /**
         * 丢失节点数量
         */
        YarnMonitorEntrance.fields1.put("lostNodes",clusterInfo1.get("lostNodes"));
        YarnMonitorEntrance.fields2.put("lostNodes",clusterInfo2.get("lostNodes"));
        /**
         * 不正常节点数量
         */
        YarnMonitorEntrance.fields1.put("unhealthyNodes",clusterInfo1.get("unhealthyNodes"));
        YarnMonitorEntrance.fields2.put("unhealthyNodes",clusterInfo2.get("unhealthyNodes"));
        /**
         * 已退役节点数量
         */
        YarnMonitorEntrance.fields1.put("decommissionedNodes",clusterInfo1.get("decommissionedNodes"));
        YarnMonitorEntrance.fields2.put("decommissionedNodes",clusterInfo2.get("decommissionedNodes"));
        /**
         * 虚拟核心总数
         */
        YarnMonitorEntrance.fields1.put("totalVirtualCores",clusterInfo1.get("totalVirtualCores"));
        YarnMonitorEntrance.fields2.put("totalVirtualCores",clusterInfo2.get("totalVirtualCores"));
        /**
         * 分配的虚拟核心数
         */
        YarnMonitorEntrance.fields1.put("allocatedVirtualCores",clusterInfo1.get("allocatedVirtualCores"));
        YarnMonitorEntrance.fields2.put("allocatedVirtualCores",clusterInfo2.get("allocatedVirtualCores"));
        /**
         * 可用虚拟核心数
         */
        YarnMonitorEntrance.fields1.put("availableVirtualCores",clusterInfo1.get("availableVirtualCores"));
        YarnMonitorEntrance.fields2.put("availableVirtualCores",clusterInfo2.get("availableVirtualCores"));
        /**
         * 以 MB 为单位分配的内存量
         */
        YarnMonitorEntrance.fields1.put("allocatedMB",clusterInfo1.get("allocatedMB"));
        YarnMonitorEntrance.fields2.put("allocatedMB",clusterInfo2.get("allocatedMB"));
        /**
         * 重启节点
         */
        YarnMonitorEntrance.fields1.put("rebootedNodes",clusterInfo1.get("rebootedNodes"));
        YarnMonitorEntrance.fields2.put("rebootedNodes",clusterInfo2.get("rebootedNodes"));
        /**
         * 关闭节点
         */
        YarnMonitorEntrance.fields1.put("shutdownNodes",clusterInfo1.get("shutdownNodes"));
        YarnMonitorEntrance.fields2.put("shutdownNodes",clusterInfo2.get("shutdownNodes"));
    }
}
