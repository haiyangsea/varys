package varys.examples.local

import java.util.concurrent.Executors

import varys.Utils
import varys.framework.master.Master
import varys.framework.slave.Slave

/**
 * Created by Allen on 2015/4/19.
 */
object LocalCluster extends App {
    val executors = Executors.newCachedThreadPool()

    val masterUrl = s"varys://${Utils.localHostName()}:1606"

    executors.execute(new Runnable {
      override def run(): Unit = Master.main(Array())
    })

    executors.execute(new Runnable {
      override def run(): Unit = Slave.main(Array(masterUrl))
    })

    def addShutdownHook(f: () => Unit): Unit = {
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() = f()
      })
    }
}
