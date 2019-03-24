---
layout:     post
title:      Spark的运行模式及原理
subtitle:   Spark的运行模式及原理（二）
date:       2019-03-22
author:     CXR
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - Spark
    - 大数据
---

## Local cluster模式
local cluster模式也就是伪分布式模式，其原理与<a href="https://smartcxr.github.io/2019/03/22/Spark's-operating-mode-and-principle/">Spark的运行模式及原理（一）</a>中的standalong模式相似，这里就不做重复介绍。

## Mesos模式
在Mesos模式中资源的调度可以分为**粗粒度**和**细粒度调度**两种。
#### Mesos模式粗粒度逻辑结构图
![Mesos模式粗粒度逻辑结构图](/pic/spark_Coarse_mesos.png "Mesos模式粗粒度逻辑结构图")
在粗粒度调度模式中，MesosCoarseGrainedSchedulerBackend继承CoarseGrainedSchedulerBackend，实现了来自Mesos的Scheduler接口，并将其注册到mesos资源调度框架中，用于接收Mesos的资源分配，得到资源之后启动，源码如下：
```
override def registered(
     driver: org.apache.mesos.SchedulerDriver,
     frameworkId: FrameworkID,
     masterInfo: MasterInfo) {

   this.appId = frameworkId.getValue
   this.mesosExternalShuffleClient.foreach(_.init(appId))
   this.schedulerDriver = driver
   markRegistered()
   launcherBackend.setAppId(appId)
   launcherBackend.setState(SparkAppHandle.State.RUNNING)
 }
```
#### Mesos模式细粒度逻辑结构图
![Mesos模式细粒度逻辑结构图](/pic/spark_mesos.png "Mesos模式细粒度逻辑结构图")
mesos细粒度资源调度在spark2.0之后被弃用了，这里就不在详述。弃用原因：在Spark中默认运行的就是细粒度模式这种模式支持资源的抢占，spark和其他frameworks以非常细粒度的运行在同一个集群中，每个application可以根据任务运行的情况在运行过程中动态的获得更多或更少的资源（mesos动态资源分配），但是这会在每个task启动的时候增加一些额外的开销。这个模式不适合于一些低延时场景例如交互式查询或者web服务请求等。spark中运行的每个task的运行都需要去申请资源，也就是说启动每个task都增加了额外的开销。在一些task数量很多，可是任务量比较轻的应用中，该开销会被放大。

## Yarn cluster模式

#### Yarn cluster模式逻辑结构图
![Yarn cluster模式逻辑结构图](/pic/spark_yarn_cluster.png "Yarn cluster模式逻辑结构图")
Client类通过Yarn Client Api提交请求到Hadoop集群上的启动一个Spark Application，Spark Application会首先注册一个Master，之后启动程序，SparkContext在用户程序中初始化时，使用CoarseGrainedSchedulerBackend配合YarnClusterScheduler启动。之后的任务调度与其他的模式相类似。

YarnClient源码：
```
/**
 * <p>
 * Obtain a {@link YarnClientApplication} for a new application,
 * which in turn contains the {@link ApplicationSubmissionContext} and
 * {@link org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse}
 * objects.
 * </p>
 *
 * @return {@link YarnClientApplication} built for a new application
 * @throws YarnException
 * @throws IOException
 */
public abstract YarnClientApplication createApplication()
    throws YarnException, IOException;
```

YarnClusterScheduler源码：
```
/**
 * This is a simple extension to ClusterScheduler - to ensure that appropriate initialization of
 * ApplicationMaster, etc is done
 */
private[spark] class YarnClusterScheduler(sc: SparkContext) extends YarnScheduler(sc) {

  logInfo("Created YarnClusterScheduler")

  override def postStartHook() {
    ApplicationMaster.sparkContextInitialized(sc)
    super.postStartHook()
    logInfo("YarnClusterScheduler.postStartHook done")
  }

}
```

## Yarn Client模式
#### Yarn Client模式逻辑结构图
![Yarn Client模式逻辑结构图](/pic/spark_yarn_client.png "Yarn Client模式逻辑结构图")

yarn client 与Yarn cluster模式的区别在于，Yarn cluster模式应用，包括SparkContext都是作为Yarn框架所需要的Application Master，在由Yarn ResourceManager为其分配随机的节点运行，而在Yarn client中SparkContext运行在本地。

在yarn client模式中，Sparkcontext在初始化过程中启动YarnClientSchedulerBackend（继承至YarnSchedulerBackend，而YarnSchedulerBackend继承了CoarseGrainedSchedulerBackend）。
