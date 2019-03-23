---
layout:     post
title:      Spark的运行模式及原理
subtitle:   Spark的运行模式及原理（二）
date:       2019-03-22
author:     CXR
header-img: img/post-bg-ios9-web.jpg
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
