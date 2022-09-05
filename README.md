# Hdfs_Yarn_Cluster_Monitor

此代码是多线程

## 指标

state：
  - ResourceManager 状态有效值是: NOTINITED、 INITED、 STARTED（）、 STOPPED
  
haState：
  - ResourceManagerHA 状态有效值是: INITIALIZING、 Aactive、 STANDBY、 STOPPED
  
startedOn：
  - 集群开始的时间(以 ms 为单位，从纪元开始)
 
state：
  - ResourceManager 状态有效值是: NOTINITED、 INITED、 STARTED（）、 STOPPED
  
haState：
  - ResourceManagerHA 状态有效值是: INITIALIZING、 Aactive、 STANDBY、 STOPPED
  
startedOn：
  - 集群开始的时间(以 ms 为单位，从纪元开始)
 
running_0：
  - 运行时间小于60分钟的应用程序的当前数量

running_60：
  - 运行时间在60到300分钟之间的应用程序的当前数量
  
running_300：
  - 运行时间在300到1440分钟之间的应用程序的当前数量
  
running_1440：
  - 当前运行的应用程序的运行时间超过1440分钟
  
MemMaxM：
  - JVM 运行时可以使用的最大内存大小 (MB)
  
MemHeapMaxM：
  - JVM 配置的 HeapMemory 的大小 (MB)
  
MemHeapUsedM：
  - 当前使用的堆内存（以 MB 为单位）
 
GcCount：
  - 总GC计数
  
GcTimeMillis：
  - 以毫秒为单位的总GC时间
  
MemNonHeapUsedM：
  - 当前使用的非堆内存（以 MB 为单位）
  
MemNonHeapCommittedM：
  - 当前提交的非堆内存（以 MB 为单位）

ThreadsRunnable：
  - 线程运行数量

ThreadsBlocked：
  - 线程阻塞数量

ThreadsWaiting：
  - 线程等待数量

NumActiveNMs：
  - 当前活动 NodeManager 的数量

numDecommissioningNMs：
  - 正在退役的 NodeManager 的当前数量

NumDecommissionedNMs：
  - 退役 NodeManager 的当前数量

NumShutdownNMs：
  - 当前被优雅关闭的 NodeManager 数量。注意，这并不包括被强制关闭的 NodeManager。

NumLostNMs：
  - 当前因为没有发送心跳而丢失的 NodeManager 数量。

NumUnhealthyNMs：  
  - 当前不健康 NodeManager 的数量

NumRebootedNMs：  
  - 当前重新启动的 NodeManager 数量
