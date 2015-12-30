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
                  segmentCols: Seq[String]) extends Command

case class Describe(tableName: String,
                    isTable: Boolean,
                    projectionName: Option[Int]) extends Command


object SimpleParser extends RegexParsers with JavaTokenParsers {

  private def handleError(e: Error,input: String) = {
    val msg = "Cannot parse [" + input + "] because " + e.msg
    throw new IllegalArgumentException(msg)
  }

  private def handleFailure(f: Failure,input: String) ={
    val msg = "Cannot parse [" + input + "] because " + f.msg
    throw new IllegalArgumentException(msg)
  }

  def parseLoad(input: String): Load =
    parseAll(load, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => s.get.asInstanceOf[Load]
      case e: Error => handleError(e, input)
      case f: Failure => handleFailure(f, input)
    }

  def parseShow(input: String): Boolean =
    parseAll(show, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => true
      case e: Error => handleError(e, input)
      case f: Failure => handleFailure(f, input)
    }

  def parseDecribe(input: String): Describe =
    parseAll(describe, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => s.get.asInstanceOf[Describe]
      case e: Error => handleError(e, input)
      case f: Failure => handleFailure(f, input)
    }

  def parseCreate(input: String): Create =
    parseAll(create, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => s.get.asInstanceOf[Create]
      case e: Error => handleError(e, input)
      case f: Failure => handleFailure(f, input)
    }

  def parseSelect(input: String): Boolean =
    parseAll(select, StringUtils.removeEnd(input, ";")) match {
      case s: Success[_] => true
      case e: Error => handleError(e, input)
      case f: Failure => handleFailure(f, input)
    }

  lazy val select: Parser[Boolean] =
    SELECT ~ ".*".r ^^ {
      case url ~ dl => true
    }

  lazy val show: Parser[Boolean] =
    SHOW ~ TABLES ^^ {
      case url ~ dl => true
    }

  lazy val describe: Parser[Describe] =
    DESCRIBE ~ (TABLE | PROJECTION) ~ ident ~ "[0-9]*".r.? ^^ {
      case url ~ tp ~ table ~ proj =>
        if (tp.equals("TABLE")) {
            Describe(table,true,None)
        }
        else{
          Describe(table,false,Some(proj.getOrElse("0").toInt))
        }
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
      (SEGMENT ~> BY ~> columnNames) ^^ {
      case c ~ tv ~ e ~ tableName ~ cols ~ primaryCols ~ partitionCols ~ segmentCols =>
        Create(tableName, cols, partitionCols, primaryCols, segmentCols)
    }

  def options: Parser[Map[String, String]] = {
    def option: Parser[(String, String)] =
      quotedStr ~ ":" ~ quotedStr ^^ {
        case optionKey ~ sep ~ optionVal => (optionKey, optionVal)
      }
    "(" ~> repsep(option, ",") <~ ")" ^^ {
      Map() ++ _
    }
  }

  def columns: Parser[Map[String, String]] = {
    def column: Parser[(String, String)] =
      ident ~ ident ^^ {
        case columnName ~ dataType => (columnName, dataType)
      }
    "(" ~> repsep(column, ",") <~ ")" ^^ {
      Map() ++ _
    }
  }

  def columnNames: Parser[Seq[String]] = {
    def columnName: Parser[(String)] =
      ident ^^ {
        case columnName: String => columnName
      }
    "(" ~> repsep(columnName, ",") <~ ")" ^^ {
      Seq() ++ _
    }
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
  protected val SHOW = Keyword("SHOW")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val TABLES = Keyword("TABLES")

  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }
}
