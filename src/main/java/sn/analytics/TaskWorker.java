package sn.analytics;

/**
 * Wrap up any task in this
 */
public class TaskWorker implements Runnable {
    @Override
    public void run() {


        try {
            Thread.sleep(90*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
