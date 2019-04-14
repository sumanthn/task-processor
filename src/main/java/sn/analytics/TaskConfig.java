package sn.analytics;

import org.apache.pulsar.client.api.Message;

/*
    Task related configuration
 */
public class TaskConfig {
    final long taskId;
    final Message message;
    //any other config required to execute the task


    public TaskConfig(long taskId, Message message) {
        this.taskId = taskId;
        this.message = message;
    }

    public long getTaskId() {
        return taskId;
    }

    public Message getMessage() {
        return message;
    }
}
