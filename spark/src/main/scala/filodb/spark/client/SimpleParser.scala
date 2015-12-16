package filodb.spark.client

import org.apache.commons.lang.StringUtils

import scala.util.parsing.combinator.{JavaTokenParsers, RegexParsers}
import scala.language.implicitConversions
sealed trait Command

case class Load(tableName: String,
                delimiter: Char,
                url: String,
                nullValue: String,
                emptyValue: String,
                useDefaults: Boolean) extends Command

case class Create(tableName: String,
                  columns: Map[String, String]) extends Command


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


  lazy val load: Parser[Load] =
    (LOAD ~> quotedStr) ~
      (DELIMITED ~> BY ~> quotedStr).? ~
      (WITH ~> NULL ~> quotedStr).? ~
      (WITH ~> EMPTY ~> quotedStr).? ~
      (NO ~> DEFAULTS).? ~
      (INTO ~> ident) ^^ {
      case url ~ dl ~ nullVal ~ emptyVal ~ noDef ~ name =>
        Load(name, dl.getOrElse(",").toCharArray()(0), url,
          nullValue = nullVal.orNull,
          emptyValue = emptyVal.orNull,
          noDef.fold(true) {
            case s: String => false
            case _ => true
          })
    }

  lazy val create: Parser[Create] =
    CREATE ~
      (TABLE | VIEW) ~
      (IF ~> NOT ~> EXISTS).? ~
      ident ~ columns ^^ {
      case c ~ tv ~ e ~ tableName ~ cols =>
        Create(tableName, cols)
    }

  def columns: Parser[Map[String, String]] =
    "(" ~> repsep(column, ",") <~ ")" ^^ {
      Map() ++ _
    }

  def column: Parser[(String, String)] =
    ident ~ ident ^^ {
      case columnName ~ dataType => (columnName, dataType)
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

  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }

}
