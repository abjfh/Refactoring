import org.junit.Test

import java.util.concurrent.{Executors, TimeUnit}

/**
 * @version: java version 1.8
 * @version: scala version 2.12
 * @Author: hyl
 * @description:
 * @date: 2024-01-10 16:04
 */
class test02 {
  @Test
  def fun12:Unit = {

    val executorService = Executors.newScheduledThreadPool(5, new ThreadPoolFactory("scheduledThread"))

    executorService.schedule(new Runnable() {
      override def run(): Unit = {
        print("scheduleThreadPool")
        System.out.println(Thread.currentThread.getName + ", delay 1s")
      }
    }, 1, TimeUnit.SECONDS)


  }
}
