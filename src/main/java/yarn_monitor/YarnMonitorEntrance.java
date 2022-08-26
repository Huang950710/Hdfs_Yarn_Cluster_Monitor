package yarn_monitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class YarnMonitorEntrance {

    /**
     * tags ：influxdb数据库 tag 数据
     * fields1，fields2 存放 influxdb 的 fields 数据
     */
    public static Map<String, String> tags = new HashMap<>();
    public static Map<String, Object> fields1 = new HashMap<>();
    public static Map<String, Object> fields2 = new HashMap<>();

    /**
     * 主函数入口
     * @param args
     */
    public static void main(String[] args) {

        /**
         * 参数判断
         * 1.参数个数不等于3的，直接退出程序
         * 2.发送模块不存在的，直接退出程序
         */
        if (args.length != 3) {
            System.out.println("参数个数错误");
            System.exit(1);
        }

        /**
         * 获取监控数据方法，并写入数据库
         */
        parameter(args);
    }


    /**
     * args[0] ：业务线名称
     * args[1] ：IP地址
     * args[2] ：IP地址
     */
    public static void parameter(String[] args){
        /**
         * 死循环，实时获取 Yarn 监控指标
         */
        while(true){

            /**
             * Influxdb 数据赋值
             */
            Influxdb influxdb = new Influxdb("http://10.60.79.103:8086",
                    "admin",
                    "admin",
                    "monitor");

            /**
             * tag值标记的数据是那个业务线的数据
             */
            tags.put("company",args[0]);

            /**
             * ClusterInfo ： 集群信息
             */
            YarnClusterInfo.YARN_CLUSTER_INFO_JSON(args[1],args[2]);
            YarnClusterMetrics.YARN_CLUSTER_METRICS_JSON(args[1],args[2]);
            YarnClusterJmx.YARN_CLUSTER_JMX_JSON(args[1],args[2]);

            System.out.println(fields1);
            System.out.println(fields2);

            for (int i = 0; i < 80; i++) {
                System.out.print("*");
            }
            System.out.println(" ");

            //创建inlfuxdb客户端对象
            Infludb_client infludb_client = new Infludb_client(influxdb.getUsername(), influxdb.getPassword(), influxdb.getOpenurl(), influxdb.getDatabase());
            //调用setup方法，初始化influxdb链接
            Infludb_client setup = infludb_client.setup();

            /**
             * 参数说明：
             * tags：tags数据
             * fields：fields数据
             * hdfs_yarn_monitor：数据插入的数据表
             */
            setup.insert(tags, fields1, "HDFS_YARN_CLUSTER_MONITOR");
            setup.insert(tags, fields2, "HDFS_YARN_CLUSTER_MONITOR");
            setup.close();

/*            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

        }

    }

}
