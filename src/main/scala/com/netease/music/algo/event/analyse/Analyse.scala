package com.netease.music.algo.event.analyse

import scala.reflect.runtime.{universe => ru}
object Analyse {
  def dynamicInvocation( className: String) = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule("com.netease.music.algo.event.analyse.".concat(className))
    val im = m.reflectModule(module)
    val method = im.symbol.info.decl(ru.TermName("main")).asMethod

    val objMirror = m.reflect(im.instance)
    objMirror.reflectMethod(method)(Array[String]())


  }

  def main(args: Array[String]): Unit = {
    dynamicInvocation(args(0))



  }
}
