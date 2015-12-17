package filodb.spark.client

import org.apache.commons.lang.StringUtils

import scala.util.parsing.combinator.{JavaTokenParsers, RegexParsers}
import scala.language.implicitConversions

sealed trait Command

case class Load(tableName: String,
                url: String,
                format: String,
                options: Map[String, String]) extends Command

case class Create(tableName: String,
                  columns: Map[String, String],
                  partitionCols: Seq[String],
                  primaryCols: Seq[String],
                  segmentCols: Seq[String],
                  sortCols: Seq[String]) extends Command


object SimpleParser extends RegexParsers with JavaTokenParsers {

  def parseLoad(input: String): Load =
    parseAll(load, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => s.get.asInstanceOf[Load]
      case e: Error =>
        val msg = "Cannot parse [" + input + "] because " + e.msg
        throw new IllegalArgumentException(msg)
      case f: Failure =>
        val msg = "Cannot parse [" + input + "] because " + f.msg
        throw new IllegalArgumentException(msg)
    }

  def parseCreate(input: String): Create =
    parseAll(create, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => s.get.asInstanceOf[Create]
      case e: Error =>
        val msg = "Cannot parse [" + input + "] because " + e.msg
        throw new IllegalArgumentException(msg)
      case f: Failure =>
        val msg = "Cannot parse [" + input + "] because " + f.msg
        throw new IllegalArgumentException(msg)
    }

  def parseSelect(input: String): Boolean =
    parseAll(select, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => true
      case e: Error =>
        val msg = "Cannot parse [" + input + "] because " + e.msg
        throw new IllegalArgumentException(msg)
      case f: Failure =>
        val msg = "Cannot parse [" + input + "] because " + f.msg
        throw new IllegalArgumentException(msg)
    }

  lazy val select: Parser[Boolean] =
    SELECT ~ ".*".r ^^ {
      case url ~ dl => true
    }


  lazy val load: Parser[Load] =
    (LOAD ~> quotedStr) ~
      (INTO ~> ident) ~
      (WITH ~> FORMAT ~> quotedStr) ~
      (WITH ~> OPTIONS ~> options).? ^^ {
      case url ~ name ~ format ~ options =>
        Load(name, url, format, options.getOrElse(Map()))
    }

  lazy val create: Parser[Create] =
    CREATE ~
      (TABLE | VIEW) ~
      (IF ~> NOT ~> EXISTS).? ~
      ident ~
      columns ~
      (PRIMARY ~> KEY ~> columnNames) ~
      (PARTITION ~> BY ~> columnNames) ~
      (SEGMENT ~> BY ~> columnNames)~
      (SORT ~> BY ~> columnNames) ^^ {
      case c ~ tv ~ e ~ tableName ~ cols ~ primaryCols ~ partitionCols ~ segmentCols ~ sortCols =>
        Create(tableName, cols, partitionCols, primaryCols, segmentCols, sortCols)
    }

  def options: Parser[Map[String, String]] =
    "(" ~> repsep(option, ",") <~ ")" ^^ {
      Map() ++ _
    }

  def option: Parser[(String, String)] =
    quotedStr ~ quotedStr ^^ {
      case optionKey ~ optionVal => (optionKey, optionVal)
    }


  def columns: Parser[Map[String, String]] =
    "(" ~> repsep(column, ",") <~ ")" ^^ {
      Map() ++ _
    }

  def column: Parser[(String, String)] =
    ident ~ ident ^^ {
      case columnName ~ dataType => (columnName, dataType)
    }

  def columnNames: Parser[Seq[String]] =
    "(" ~> repsep(columnName, ",") <~ ")" ^^ {
      Seq() ++ _
    }

  def columnName: Parser[(String)] =
    ident ^^ {
      case columnName: String => columnName
    }


  protected lazy val quotedStr: Parser[String] =
    ("'" + """([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r ^^ {
      str => str.substring(1, str.length - 1)
    }

  protected val DELIMITED = Keyword("DELIMITED")
  protected val WITH = Keyword("WITH")
  protected val NULL = Keyword("NULL")
  protected val EMPTY = Keyword("EMPTY")
  protected val NO = Keyword("NO")
  protected val DEFAULTS = Keyword("DEFAULTS")
  protected val BY = Keyword("BY")
  protected val LOAD = Keyword("LOAD")
  protected val CREATE = Keyword("CREATE")
  protected val TABLE = Keyword("TABLE")
  protected val IF = Keyword("IF")
  protected val NOT = Keyword("NOT")
  protected val EXISTS = Keyword("EXISTS")
  protected val VIEW = Keyword("VIEW")
  protected val PROJECTION = Keyword("PROJECTION")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val INTO = Keyword("INTO")
  protected val PARTITION = Keyword("PARTITION")
  protected val PRIMARY = Keyword("PRIMARY")
  protected val SEGMENT = Keyword("SEGMENT")
  protected val SELECT = Keyword("SELECT")
  protected val KEY = Keyword("KEY")
  protected val OPTIONS = Keyword("OPTIONS")
  protected val FORMAT = Keyword("FORMAT")
  protected val SORT = Keyword("SORT")

  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }

}
