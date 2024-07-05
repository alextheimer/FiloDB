package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ColumnFilterMapSpec extends AnyFunSpec with Matchers {

  private def equals(column: String, value: String): ColumnFilter = {
    ColumnFilter(column, Filter.Equals(value))
  }

  private def regex(column: String, value: String): ColumnFilter = {
    ColumnFilter(column, Filter.EqualsRegex(value))
  }

  it("should store/retrieve ColumnFilterMap elements correctly") {
    {
      // empty map
      val cfMap = new ColumnFilterMap[String](Nil)
      cfMap.get(Map("foo" -> "bar")) shouldEqual None
      cfMap.get(Map("foo" -> "bar", "baz" -> "bat")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // single equals filter
      val entries = Seq(
        (Seq(equals("a", "a")), "foo")
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "a")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "a", "b" -> "b")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "b")) shouldEqual None
      cfMap.get(Map("a" -> "b", "b" -> "a")) shouldEqual None
    }

    {
      // single regex filter
      val entries = Seq(
        (Seq(regex("a", "a.*")), "foo")
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "DNE")) shouldEqual None
      cfMap.get(Map("a" -> "a123")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "a123", "b" -> "hello")).get shouldEqual "foo"
    }

    {
      // multiple filters
      val entries = Seq(
        (Seq(
          regex("a", "aaa.*"),
          equals("b", "b"),
          regex("c", "ccc.*"),
          equals("d", "d")), "foo")
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "DNE", "b" -> "b", "c" -> "ccc123", "d" -> "d")) shouldEqual None
      cfMap.get(Map("a" -> "aaa123", "b" -> "b", "c" -> "ccc123", "d" -> "d")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "aaa123", "b" -> "b", "c" -> "ccc123", "d" -> "d", "e" -> "e")).get shouldEqual "foo"
    }

    {
      // multiple sets with single filter
      val entries = Seq(
        (Seq(regex("a", "a.*")), "a"),
        (Seq(regex("b", "b.*")), "b"),
        (Seq(equals("c", "c")), "c"),
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "DNE", "b" -> "DNE", "c" -> "DNE")) shouldEqual None
      cfMap.get(Map("a" -> "a123", "b" -> "DNE", "c" -> "DNE")).get shouldEqual "a"
      cfMap.get(Map("a" -> "DNE", "b" -> "b123", "c" -> "DNE")).get shouldEqual "b"
      cfMap.get(Map("a" -> "DNE", "b" -> "DNE", "c" -> "c")).get shouldEqual "c"
      cfMap.get(Map("a" -> "a123", "b" -> "b123", "c" -> "c")).isDefined shouldEqual true
    }

    {
      // multiple sets; different lengths at same paths
      val entries = Seq(
        (Seq(
          equals("a", "a")), "1"),
        (Seq(
          equals("a", "a"),
          equals("b", "b")), "2"),
        (Seq(
          equals("a", "a"),
          equals("b", "b"),
          equals("c", "c")), "3"),
        (Seq(
          equals("b", "b"),
          equals("c", "c")), "4"),
        (Seq(
          regex("a", "aaa.*")), "1r"),
        (Seq(
          regex("a", "aaa.*"),
          regex("b", "bbb.*")), "2r"),
        (Seq(
          regex("a", "aaa.*"),
          regex("b", "bbb.*"),
          regex("c", "ccc.*")), "3r"),
        (Seq(
          regex("b", "bbb.*"),
          regex("c", "ccc.*")), "4r"),
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "DNE", "b" -> "DNE", "c" -> "DNE")) shouldEqual None
      cfMap.get(Map("a" -> "a", "b" -> "DNE", "c" -> "DNE")).get shouldEqual "1"
      cfMap.get(Map("a" -> "DNE", "b" -> "DNE", "c" -> "DNE", "d" -> "d")) shouldEqual None
      cfMap.get(Map("a" -> "a", "b" -> "DNE", "c" -> "DNE", "d" -> "d")).get shouldEqual "1"
      cfMap.get(Map("a" -> "a", "b" -> "b", "c" -> "DNE")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "a", "b" -> "b", "c" -> "c")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "DNE", "b" -> "b", "c" -> "c")).get shouldEqual "4"
      cfMap.get(Map("a" -> "aaa123", "b" -> "DNE", "c" -> "DNE")).get shouldEqual "1r"
      cfMap.get(Map("a" -> "aaa123", "b" -> "bbb123", "c" -> "DNE")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "aaa123", "b" -> "bbb123", "c" -> "ccc123")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "DNE", "b" -> "bbb123", "c" -> "ccc123")).get shouldEqual "4r"
    }
  }
}
