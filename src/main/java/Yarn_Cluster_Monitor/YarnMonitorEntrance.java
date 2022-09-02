package Yarn_Cluster_Monitor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

/**
 * 此模块是函数主入口
 */

public class YarnMonitorEntrance {

    /**
     * 主函数入口
     *
     * @param args
     */
    public static void main(String[] args) {

        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("代码启动时间：" + simpleDateFormat.format(date));

        /**
         * 参数判断
         * 1.参数个数不等于3的，直接退出程序
         * 2.发送模块不存在的，直接退出程序
         */
        if (args.length != 3) {
            System.out.println("参数个数错误");
            System.exit(1);
        }

        Infludb_client.setup().createdatabase();

        /**
         * 创建线程池
         */
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        /**
         * 获取监控数据方法，并写入数据库
         */
        for (int i = 0; i < 100; i++) {
            executorService.execute(new YarnClusterJmxThread(args[0], args[1], args[2]));
            executorService.execute(new YarnClusterMetricsThread(args[0], args[1], args[2]));
            executorService.execute(new YarnClusterInfoThread(args[0], args[1], args[2]));
        }
        executorService.shutdown();

    }
}

