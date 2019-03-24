---
layout:     post
title:      Spark的运行模式及原理
subtitle:   Spark的运行模式及原理（一）
date:       2019-03-22
author:     CXR
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - Spark
    - 大数据
---

## Spark的运行模式
spark的运行模式有以下几种：

①本地模式 Local[N],使用N个线程

②伪分布式模式 Local cluster[worker,core,memory]，可以配置工作节点数，cpu数量和内存大小

③Standalong模式 需要部署到Spark的节点上，URL为spark master的主机地址和端口

④Mesos模式 需要部署到Spark和mesos的节点上，URL为mesos的主机地址和端口

⑤Yarn standalong/Yarn cluster模式 yarn模式主要都在yarn集群中运行

⑥yarn client模式 在本地运行

## Spark的基本工作流程
以上的运行模式总体基于一个相似的工作流程，都是将任务分成**任务调度和任务执行**两个部分，Spark应用程序的运行离不开**SparkContext和Executer**两个部分，Executer负责**执行任务**，运行Executer的节点称之为Worder节点，SparkContext由用户程序启动，通过资源**调度模块和Executer**通信。基本框架图如下图所示：

![Spark基本工作流程图](https://smartcxr.github.io/pic/spark_yunxingliuchentu.png "Spark基本工作流程图")

SparkContext是程序的总入口，在SparkContext的初始化过程中，Spark会分别初始化**DAGScheduler作业调度和TaskScheduler任务调度**。详情请看https://blog.csdn.net/Smart_cxr/article/details/81153578

为了抽象出一个公共接口供DAGScheduler作业调度使用，所有的这些运行模式都基于**TaskScheduler和SchedulerBackend**这两个接口：

### TaskScheduler

```
/**
低级任务调度程序接口，目前仅由
 [[org.apache.spark.scheduler.TaskSchedulerImpl]。 该接口允许插入不同的任务调度程序。 每个TaskScheduler都为单个SparkContext调度任务。 这些调度程序从每个阶段的DAGScheduler获取提交给它们的任务集，并负责将任务发送到集群，运行它们，如果发生故障则重试，以及减轻落后者。 他们将事件返回给DAGScheduler。
**/
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for slave registrations, etc.
  def postStartHook() { }

  // Disconnect from the cluster.
  def stop(): Unit

  // 提交待运行的任务
  def submitTasks(taskSet: TaskSet): Unit

  // Kill all the tasks in a stage and fail the stage and all the jobs that depend on the stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  /**
   * Kills a task attempt.
   * Throw UnsupportedOperationException if the backend doesn't support kill a task.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // Kill all the running task attempts in a stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit

  // 设置DAGSecheduler用来回调相关函数
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // 获取要在群集中使用的默认并行级别，作为调整作业大小的提示。
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean

  /**
   * 得到一个applicaion ID
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * 处理丢失的executor
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * 处理已删除的工作节点
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * 获取与作业关联的应用程序的ID。
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
```

TaskScheduler的实现主要用于与DAGSecheduler交互，负责具体调度和运行，其核心接口是**submitTask**和**cancelTasks**

### SchedulerBackend

```
/**
用于调度系统的后端接口，允许在TaskSchedulerImpl下插入不同的系统。 我们假设一个类似Mesos的模型，其中应用程序在机器可用时获得资源，并可以在它们上启动任务。
**/
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  /**
   * Requests that an executor kills a running task.
   *
   * @param taskId Id of the task.
   * @param executorId Id of the executor the task is running on.
   * @param interruptThread Whether the executor should interrupt the task thread.
   * @param reason The reason for the task kill.
   */
  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException

  def isReady(): Boolean = true

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

  /**
   * Get the max number of tasks that can be concurrent launched currently.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @return The max number of tasks that can be concurrent launched currently.
   */
  def maxNumConcurrentTasks(): Int

}
```
SchedulerBackend的实现是与底层资源调度系统进行交互，配合TaskScheduler实现具体任务执行所需要的资源，核心接口是**reviveOffers**

### TaskSchedulerImpl
源码太长不全部复制
```
/**
通过SchedulerBackend执行多种类型群集的计划任务。
它也可以通过使用LocalSchedulerBackend`并将isLocal设置为true来使用本地设置。 它处理常见逻辑，例如确定跨作业的调度顺序，唤醒以启动推测任务等。
客户端应首先调用initialize（）和start（），然后通过submitTasks方法提交任务集。
THREADING：[[SchedulerBackend]]和任务提交客户端可以从多个线程调用此类，因此需要锁定公共API方法来维护其状态。 另外，一些[[SchedulerBackend]]在他们想要在这里发送事件时自己同步，然后获取锁定我们，所以我们需要确保在我们持有时我们不会尝试锁定后端 锁定自己。
**/
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging {
    ...
    /*
    由集群管理器调用以提供从属资源。 我们通过按优先顺序询问我们的活动任务集来执行任务。 我们以循环方式为每个节点填充任务，以便在整个集群中平衡任务。
    */
    def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized

    def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer)
    ...
  }
```
TaskSchedulerImpl是TaskScheduler的接口实现，提供了大多数本地和分布式运行调度模式的任务调度接口
此外它还实现了**resourceOffers**和**statusUpdate**这两个接口供Backend调用，用于提供资源调度和更新任务状态。在提交任务和更新阶段TaskSchedulerImpl都会调用**Backend**的**resourceOffers**函数，用于发起一次任务资源调度请求。

### Executor
实际的任务执行都是由Executor类来执行，Executor对每一个任务创建一个TaskRunner类交给线程池运行。
源码太长不一一列出
```
/*Spark执行器，由线程池支持运行任务。 这可以与Mesos，YARN和独立调度程序一起使用。 内部RPC接口用于与驱动程序通信，但Mesos细粒度模式除外。*/
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler)
  extends Logging {

    ...

    // 开启worker线程池
      private val threadPool = {
        val threadFactory = new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Executor task launch worker-%d")
          .setThreadFactory(new ThreadFactory {
            override def newThread(r: Runnable): Thread =
              // 使用不间断线程来运行任务，以便我们可以允许运行代码
              // 被`Thread.interrupt（）`打断。 一些问题，如KAFKA-1894，HADOOP-10622，
              // 如果某些方法被中断，它将永远挂起。
              new UninterruptibleThread(r, "unused") // 线程名称将由ThreadFactoryBuilder设置
          })
          .build()
        Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
      }

  // 维护正在运行的任务列表。
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  //启动任务
  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
      val tr = new TaskRunner(context, taskDescription)
      runningTasks.put(taskDescription.taskId, tr)
      threadPool.execute(tr)
    }

  ...
  }

```

运行的结果通过**ExecutorBackend**返回
```
/**
 * Executor用于将更新发送到集群调度程序的可插入接口。
 */
private[spark] trait ExecutorBackend {
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit
}
```

## Local模式
### 部署及运行
![local模式程序](https://smartcxr.github.io/pic/spark_local_run_program.png "local模式程序")
### 内部实现原理
![local内部实现原理](https://smartcxr.github.io/pic/Local_mode_logical_structure_diagram.png "local内部实现原理")

## Standalong模式
### 内部实现原理
![Standalong内部实现原理](/pic/standalong_logical_structe_pic.png "Standalong内部实现原理")

Standalong模式使用StandaloneSchedulerBackend（spark2.4源码），（这里需要解释一下在旧的版本中是SparkDeploySchedulerBackend），配合TaskSchedulerImpl工作，而StandaloneSchedulerBackend继承了CoarseGrainedSchedulerBackend，在新的Spark版本中，大概是在1.6版本以后Spark内部就已经不在使用Akka作为底层的消息通信，而改用了netty的RPC实现内部的消息通信。

#### spark1.3的CoarseGrainedSchedulerBackend
![spark1.3_CoarseGrainedSchedulerBackend](/pic/spark1.3_CoarseGrainedSchedulerBackend.png "spark1.3_CoarseGrainedSchedulerBackend")

#### spark2.4的CoarseGrainedSchedulerBackend
![spark2.4_CoarseGrainedSchedulerBackend](/pic/spark2.4_CoarseGrainedSchedulerBackend.png "spark2.4_CoarseGrainedSchedulerBackend")

因此CoarseGrainedSchedulerBackend是一个基于Netty的RPC实现的粗粒度资源调度类，在整个spark作业期间，CoarseGrainedSchedulerBackend会监听并且持有注册给他的Executor资源，并且在接受Executor注册、状态更新、响应Scheduler请求等各种时刻，根据现有的Executor资源发起任务调度。因为在Spar中底层真正工作的是Executor，因此它是可以通过各种途径启动的，对应的，在Standalong模式中，StandaloneSchedulerBackend通过向Client类向Spark Master发送请求，在独立部署的Spark集群中启动CoarseGrainedSchedulerBackend，根据所需要的CPU数量，一个或多个CoarseGrainedExecutorBackend在worker节点上启动并注册给CoarseGrainedSchedulerBackend的Driver。
![spark2.4_client_start](/pic/spark2.4_client_start.png "spark2.4_client_start")
