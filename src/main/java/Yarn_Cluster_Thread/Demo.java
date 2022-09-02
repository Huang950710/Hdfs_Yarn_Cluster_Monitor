package Yarn_Cluster_Thread;

public class Demo {
    public static void main(String[] args) {
        //创建线程1
        TestThread testThread1 = new TestThread("1");
        //创建线程2
        TestThread testThread2 = new TestThread("2");
        //创建线程3
        TestThread testThread3 = new TestThread("3");

        testThread2.start();
        testThread3.start();
        testThread1.start();


    }
}
