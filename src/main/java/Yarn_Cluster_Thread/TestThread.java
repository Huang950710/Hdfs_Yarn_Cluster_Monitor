package Yarn_Cluster_Thread;

public class TestThread extends Thread{

    //重写构造方法，传个线程名字给到父类
    public TestThread(String name) {
        super(name);
    }

    //设置个全局访问变量
    static int ticketnum = 100;
    public void run() {
        for (int i = 0; i < 30; i++) {
            if (ticketnum >= 1) {
                System.out.println("在" + Thread.currentThread().getName() + "窗口买到北京到本溪的第" + ticketnum + "张票");
                ticketnum--;
            }
        }
    }
}
