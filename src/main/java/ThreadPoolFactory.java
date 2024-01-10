import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version: java version 1.8
 * @version: scala version 2.12
 * @Author: hyl
 * @description:
 * @date: 2024-01-06 19:46
 */
public class ThreadPoolFactory implements ThreadFactory {

    private AtomicInteger threadIdx = new AtomicInteger(0);

    private String threadNamePrefix;

    public ThreadPoolFactory(String Prefix) {
        threadNamePrefix = Prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(threadNamePrefix + "-xxljob-" + threadIdx.getAndIncrement());
        return thread;
    }
}
