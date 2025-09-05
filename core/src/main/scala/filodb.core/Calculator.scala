package filodb.core

class Calculator {

  def add(a: Int, b: Int): Int = a + b

  def subtract(a: Int, b: Int): Int = a - b

  def multiply(a: Int, b: Int): Int = a * b

  def divide(a: Int, b: Int): Option[Int] =
    if (b == 0) None else Some(a / b)

  def max(a: Int, b: Int): Int = if (a > b) a else b

  def min(a: Int, b: Int): Int = if (a < b) a else b

  def square(n: Int): Int = n * n

  def isEven(n: Int): Boolean = n % 2 == 0

  def factorial(n: Int): Int =
    if (n <= 1) 1 else n * factorial(n - 1)
}
