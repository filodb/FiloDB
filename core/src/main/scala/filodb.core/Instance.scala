package filodb.core

import scala.reflect.ClassTag
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging

/** Used to bootstrap configurable FQCNs from config to instances. */
trait Instance {

  def createClass[T: ClassTag](fqcn: String): Try[Class[_ <: T]] =
    Try[Class[_ <: T]]({
      val c = Class.forName(fqcn, false, getClass.getClassLoader).asInstanceOf[Class[_ <: T]]
      val t = implicitly[ClassTag[T]].runtimeClass
      if (c.isAssignableFrom(c) || c.getName == fqcn) c
      else throw new ClassCastException(s"$c must be assignable from or be $c")
    })

  /** Attempts to create instance of configured type, for object with constructor args. */
  def createInstance[T: ClassTag](clazz: Class[_], args: Seq[(Class[_], AnyRef)]): Try[T] =
    Try {
      val types = args.map(_._1).toArray
      val values = args.map(_._2).toArray
      val constructor = clazz.getDeclaredConstructor(types: _*)
      constructor.setAccessible(true)
      val obj = constructor.newInstance(values: _*)
      obj.asInstanceOf[T]
    }

  /** Attempts to create instance of configured type, for no-arg constructor. */
  def createInstance[T: ClassTag](clazz: Class[_]): Try[T] =
    Try {
      val constructor = clazz.getDeclaredConstructor()
      constructor.setAccessible(true)
      val obj = constructor.newInstance()
      obj.asInstanceOf[T]
    }
}

trait ConfigurableInstance extends Instance with StrictLogging
