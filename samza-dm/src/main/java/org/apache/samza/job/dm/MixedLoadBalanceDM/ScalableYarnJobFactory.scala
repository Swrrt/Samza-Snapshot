package org.apache.samza.job.dm.MixedLoadBalanceDM

import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.samza.job.StreamJobFactory
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.samza.config.Config
import org.apache.samza.job.yarn.FileSystemImplConfig
import org.apache.samza.util.hadoop.HttpFileSystem
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

class ScalableYarnJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config) = {
    // TODO fix this. needed to support http package locations.
    val hConfig = new YarnConfiguration
    hConfig.set("fs.http.impl", classOf[HttpFileSystem].getName)
    hConfig.set("fs.https.impl", classOf[HttpFileSystem].getName)
    hConfig.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    // pass along the RM config if has any
    if (config.containsKey(YarnConfiguration.RM_ADDRESS)) {
      hConfig.set(YarnConfiguration.RM_ADDRESS, config.get(YarnConfiguration.RM_ADDRESS, "0.0.0.0:8032"))
    }

    // Use the Samza job config "fs.<scheme>.impl" and "fs.<scheme>.impl.*" for YarnConfiguration
    val fsImplConfig = new FileSystemImplConfig(config)
    fsImplConfig.getSchemes.asScala.foreach(
      scheme => {
        fsImplConfig.getSchemeConfig(scheme).asScala.foreach {
          case(confKey, confValue) => hConfig.set(confKey, confValue)
        }
      }
    )

    new ScalableYarnJob(config, hConfig)
  }
}
