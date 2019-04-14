package sn.analytics;

import com.google.common.base.Stopwatch;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process incoming taks
 */

@Component
public class TaskProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TaskProcessor.class);


    @Value("${connectUrl}")
    private String connectUrl;

    @Value("${topic}")
    private String topic;

    @Value("${maxSlots}")
    private Integer maxSlots;

    @Value("${maxWorkers}")
    private Integer maxWorkers;

    @Value("${subscriptionName}")
    private String subscriptionName;

    @Value("${consumerName}")
    private String consumerName;

    @Value("${taskTimeoutInMins}")
    private Integer taskTimeoutInMins;


    //just as ref
    @Value("${instanceId}")
    private Integer instanceId;


    private AtomicInteger occupiedSlots= new AtomicInteger();


    private ExecutorService workerTaskPool;
    private ExecutorService watcherTaskPool;




    PulsarClient client;
    Consumer<byte[]> consumer;

    @PostConstruct
    public void init(){
        logger.info("connect with url {}",connectUrl);
        try {
           client= PulsarClient.builder()
                   .serviceUrl(connectUrl)
                   .build();

            consumer= client.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Shared)
                    .consumerName(consumerName+"-"+instanceId)

                    //max timeout for any task
                    .ackTimeout(300, TimeUnit.MINUTES)
                    .receiverQueueSize(maxSlots)
                    .subscriptionName(subscriptionName)
                    .subscribe();


            logger.info("max slots {} with workers {}", maxSlots,maxWorkers);
            workerTaskPool = Executors.newFixedThreadPool(maxWorkers);
            watcherTaskPool = Executors.newFixedThreadPool(2*maxSlots);

        } catch (Exception e) {

            e.printStackTrace();
            //TODO: retry for connection
            logger.warn("error in connecting to pulsar ",e);
            System.exit(0);
        }
    }


    public Integer getMaxSlots(){
        return maxSlots;
    }
    public Integer getOccupiedSlots(){
        return occupiedSlots.get();
    }

    @Scheduled(cron ="0 */1 * * * *")
    public void taskQuencher(){


        logger.info("quench for remaining tasks, occupied slots {}, avail {}", occupiedSlots.get(), (maxSlots-occupiedSlots.get()));
        if (occupiedSlots.get() > maxSlots){
            logger.info("returning empty , no quench all slots occupied ");
            return;
        }

        do {
            try {

                Message msg = consumer.receive();

                if (msg != null) {
                    TaskConfig taskConfig = new TaskConfig(msg.getSequenceId(), msg);
                    Future<TaskStatus> taskStatusFuture = workerTaskPool.submit(new TaskRunner(taskConfig));

                    occupiedSlots.getAndIncrement();
                    logger.info("process task {} on instance {}",taskConfig.getMessage().getSequenceId(),instanceId);
                    watcherTaskPool.submit(new TaskWatcher(taskConfig,taskStatusFuture));

                }
            } catch (Exception e) {
                logger.error("error in getting message from pulsar queue {}",e);
            }
        }while (occupiedSlots.get() < maxSlots);
    }


    private class TaskRunner implements Callable<TaskStatus>{

        final TaskConfig taskConfig;

        public TaskRunner(TaskConfig taskConfig) {
            this.taskConfig = taskConfig;
        }

        @Override
        public TaskStatus call() throws Exception {



            TaskStatus taskStatus = new TaskStatus();

            Stopwatch sw = Stopwatch.createStarted();

            try{

                //Wrap and execute the task
                TaskWorker worker = new TaskWorker();
                //lets block
                worker.run();
            }catch (Exception e){
                logger.warn("task {} {} failed",e);
            }

            sw.stop();
            taskStatus.setTimeTaken((int) sw.elapsed(TimeUnit.SECONDS));
            return taskStatus;
        }
    }

    private class TaskWatcher implements Runnable{

        final TaskConfig taskConfig;
        final Future<TaskStatus> taskStatusFuture;

        public TaskWatcher(TaskConfig taskConfig, Future<TaskStatus> taskStatusFuture) {
            this.taskConfig = taskConfig;
            this.taskStatusFuture = taskStatusFuture;
        }

        @Override
        public void run() {

            //essentially block and await with a timeout
            TaskStatus taskStatus=null;
            try {
                taskStatus = taskStatusFuture.get(taskTimeoutInMins,TimeUnit.MINUTES);
                logger.info("task {} completed {} in {}",taskConfig.message.getSequenceId(),taskStatus.state,taskStatus.timeTaken);
                consumer.acknowledge(taskConfig.message);
                logger.info(" task {}  complete on instance {}",taskConfig.getMessage().getSequenceId(),instanceId);

            } catch (InterruptedException e) {
                logger.warn("Error in task exec {} ", e);

            } catch (ExecutionException e) {
                logger.warn("Error in task exec {} ", e);

            } catch (TimeoutException e) {
                logger.warn("task {} timed out ", taskConfig.taskId);
                taskStatusFuture.cancel(true);
            } catch (PulsarClientException e) {
                logger.warn("Pulsar message ack exception {} ", e);
            }

            occupiedSlots.getAndDecrement();
        }
    }

}
