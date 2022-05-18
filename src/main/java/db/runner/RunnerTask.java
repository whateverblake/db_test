package db.runner;

public class RunnerTask implements Runnable{

    public  boolean  isRun = true ;

    public void stop() {
        isRun = false;
    }

    @Override
    public void run() {

    }


}
