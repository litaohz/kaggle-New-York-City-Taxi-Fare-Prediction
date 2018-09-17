package com.netease.music.algo.event.model
import java.io.InputStream


import scala.io.Source
object Test {
  def main(args: Array[String]): Unit = {
    val stream1: InputStream = this.getClass.getResourceAsStream("/abtest_config")
    val configText = Source.fromInputStream(stream1).getLines().toArray
    println(configText.size)
  }
}
