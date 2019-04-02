---
layout:     post
title:      Spark调度管理原理
subtitle:   Spark调度管理原理
date:       2019-03-24
author:     CXR
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - Spark
    - 大数据
---

## spark调度管理概述
在spark的作业中，各个任务之间可能存在着因果的依赖关系，也就是有些任务必须先执行后才能执行其他相关的任务，所以在本质上这种关系适合用DAG有向无环图来表示。在Spark的调度作业中最重要的就是DAGScheduler，也就是基于DAG的调度。DAGScheduler和TaskScheduler的功能划分：TaskScheduler负责每个具体任务的**实际物理调度**，而DAGScheduler负责将作业**拆分成不同阶段具有依赖关系的的多批任务**。

## 基本概念
+ Task 最小的处理单元
+ TaskSet 一组任务集合，包含多个Task任务
+ Stage 一个任务集合对应的调度阶段
+ Job 由一个RDD action生成的一个或多个调度阶段所组成的一次计算作业。
+ Application Spark应用程序，由一个或多个作业组成。

逻辑关系图如下：

![作业逻辑关系图](/pic/spark_job_scheduler.png "作业逻辑关系图")

在Spark应用程序中，用户代码基本是基于RDD的一系列操作，并不是所有的RDD都会出发Job的任务提交操作，只有通过action操作才会有作业提交的操作，例如collect、reduceByKey、groupByKey这些action的操作才会触发job的作业提交操作。DAGScheduler的最重要的任务就是计算作业和任务的依赖关系，制定逻辑，DAGScheduler中作业的发起入口主要有两个一个是submitJob，另一个是runJob，这两个的区别是前者返回的是一个Jobwriter可以用在异步调用中，用来判断作业完成或者取消作业，而后者则在内部调用submitJob，阻塞等待直到作业完成或者作业失败。

DAGScheduler的相关源码如下：
```
/**
   * 将操作作业提交给调度程序。
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * 在给定的RDD上运行操作作业，并在结果到达时将所有结果传递给resultHandler函数。
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @note Throws `Exception` when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }
```
DAGScheduler在SparkContext中被初始化，一个SparkContext对应创建一个DAGScheduler，DAGScheduler的事件循环了是基于RPC的消息传递机制来构建的。DAGScheduler在初始化的过程中会创建一个DAGSchedulerEventProcessLoop来处理各种DAGSchedulerEvent，这些事件包括作业的提交，任务状态的变化、监控等。

相关源码如下：

```
  //初始化DAGSchedulerEventProcessLoop
  private[spark] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  //DAGSchedulerEventProcessLoop类
  private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * DAG调度程序的主事件循环。
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, workerLost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}
```

因为eventProcessLoop是DAGScheduler的私有成员，所以不论是程序还是TaskScheduler，与DAGScheduler交互都不会直接向eventProcessLoop发消息，而是会通过公共的函数接口来发送消息，例如上面源码中的submitJob，这样一来就将事件转为异步处理了
```
eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))spark_Basic_schedule_of_job_scheduling.png
```

## 作业调度流程

基本流程图
![作业调度基本流程图](/pic/spark_Basic_schedule_of_job_scheduling.png "作业调度基本流程图")

![作业调度基本流程图](/pic/spark_Basic_schedule_of_job_scheduling2.png "作业调度基本流程图")

### stage的划分
当一个RDD操作触发计算，向DAGScheduler提交作业时，DAGScheduler会从RDD依赖链的最后一个RDD开始遍历，如果遇到了**宽依赖**则就构建了一个新的stage，以此来对stage来进行划分。

源码如下：
```
/**
   * 根据宽依赖返回父RDD
   *
   * This function will not return more distant ancestors.  For example, if C has a shuffle
   * dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
  private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ArrayStack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }
```

### stage的提交

在stage划分的时候，会划分为一个或多个stage，最后一个stage成为finalStage，stage的提交是从finalStage开始的，从后往前找到第一个stage进行提交。如下图所示

![stage提交示意图](/pic/spark_stage_submit_order.png "stage提交示意图")

源码如下：
```
/** 提交stage，从第一个没有父stage的开始递归提交 */
private def submitStage(stage: Stage) {
  val jobId = activeJobForStage(stage)
  if (jobId.isDefined) {
    logDebug("submitStage(" + stage + ")")
    if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
      val missing = getMissingParentStages(stage).sortBy(_.id)
      logDebug("missing: " + missing)
      if (missing.isEmpty) {
        logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
        submitMissingTasks(stage, jobId.get)
      } else {
        for (parent <- missing) {
          submitStage(parent)
        }
        waitingStages += stage
      }
    }
  } else {
    abortStage(stage, "No active job for stage " + stage.id, None)
  }
}
```

在所有的stage都提交后DAGScheduler会重新检查对应的调度任务是否都已经完成了，如果没有会再次进行对没有提交的任务进行重新的提交。

### task任务的提交

stage的提交最终都会被转换成taskset进行提交，DAGScheduler通过TaskScheduler的submitTasks接口来进行task的提交操作

部分源码如下：
```
if (tasks.size > 0) {
     logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
       s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
     taskScheduler.submitTasks(new TaskSet(
       tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties))
   } else {
     // Because we posted SparkListenerStageSubmitted earlier, we should mark
     // the stage as completed here in case there are no tasks to run
     markStageAsFinished(stage, None)

     stage match {
       case stage: ShuffleMapStage =>
         logDebug(s"Stage ${stage} is actually done; " +
             s"(available: ${stage.isAvailable}," +
             s"available outputs: ${stage.numAvailableOutputs}," +
             s"partitions: ${stage.numPartitions})")
         markMapStageJobsAsFinished(stage)
       case stage : ResultStage =>
         logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
     }
     submitWaitingChildStages(stage)
   }
```

## 任务调度池

taskSchedulerImpl在初始化的过程中会根据用户的设定来创建一个任务调度池，根据调度模式会进一步的创建schedulableBuilder对象，通过buildPool方法完成调度池的创建工作
```
def initialize(backend: SchedulerBackend) {
  this.backend = backend
  schedulableBuilder = {
    schedulingMode match {
      case SchedulingMode.FIFO =>
        new FIFOSchedulableBuilder(rootPool)
      case SchedulingMode.FAIR =>
        new FairSchedulableBuilder(rootPool, conf)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
        s"$schedulingMode")
    }
  }
  schedulableBuilder.buildPools()
}
```
调度策略有两种FIFO和Fair两种策略，默认是使用FIFO先进先出的调度策略，Fair调度策略是根据优先级来对任务进行调度的。
## Spark应用程序内Job的调度
（1）FIFO模式：

在默认情况下，Spark的调度器以FIFO（先进先出）方式调度Job的执行。每个Job被切分为多个Stage。第一个Job优先获取所有可用的资源，接下来第二个Job再 获取剩余资源。以此类推，如果第一个Job并没有占用所有的资源，则第二个Job还可以继续 获取剩余资源，这样多个Job可以并行运行。如果第一个Job很大，占用所有资源，则第二个 Job就需要等待第一个任务执行完，释放空余资源，再申请和分配Job。如图所示：

![FIFO模式调度示意图](/pic/spark_FIFO.png "FIFO模式调度示意图")

调度算法源码如下：
```
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}
```
在算法执行中，先看优先级，TaskSet的优先级是JobID，因为先提交的JobID小，所以 就会被更优先地调度，这里相当于进行了两层排序，先看是否是同一个Job的Taskset，不同 Job之间的TaskSet先排序。
最后执行的stageId最小为0，最先应该执行的stageId最大。但是这里的调度机制是优先调度Stageid小的。在DAGScheduler中控制Stage是否被提交到队列中，如果还有父母 Stage未执行完，则该stage的Taskset不会提交到调度池中，这就保证了虽然最先做的stage 的id大，但是排序完，由于后面的还没提交到调度池中，所以会先执行。由此可见，stage的TaskSet调度逻辑主要在DAGScheduler中，而Job调度由FIFO或者FAIR算法调度。

（2）FAIR模式:
在FAIR共享模式调度下，Spark在多Job之间以轮询（round robin）方式为任务分配资源，所有的任务拥有大致相当的优先级来共享集群的资源。这就意味着当一个长任务正在执行时，短任务仍可以分配到资源，提交并执行，并且获得不错的响应时间。这样就不用像以前一样需要等待长任务执行完才可以。这种调度模式很适合多用户的场景。用户可以通过配置spark.scheduler.mode方式来让应用以FAIR模式调度。FAIR调度器同样支持将Job分组加入调度池中调度，用户可以同时针对不同优先级对每个调度池配置不同的调度权重。这种方式允许更重要的Job配置在高优先级池中优先调度。如图所示：

![FAIR调度模型](/pic/spark_fair.png "FAIR调度模型")

调度算法源码如下：
```
private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    var compare = 0
    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name
    }
  }
```
## TaskSetManager的调度
结合job调度和stage调度方式，可以知道，每个stage对应一个TaskSetManager通过Stage回溯到第一个没有父stage的stage提交到调度池Pool中，在调度池中会根据jobid进行排序，id小的优先调度，如果父stage没有执行完成则不会提交到调度池中。
