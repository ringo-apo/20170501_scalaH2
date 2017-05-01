package example

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

object H2 {
    def main(args: Array[String]): Unit = {

      //Create Schema
      createSchemaStructure()

      //Create Table
      createTableStructure()

      //insert
      println("文字列を入力してください")
      val name = io.StdIn.readLine()
      insertDbStructure(name)

      //select
      selectDbStructure()
    }

  //Create Schema
  private def createSchemaStructure(): Unit = {
    Class.forName("org.h2.Driver")
    val conn: Connection = DriverManager.getConnection( "jdbc:h2:./h2db/TEST_SCHEMA", "sa", "" )

    try {
      val sql = "create schema if not exists TEST_SCHEMA;"
      val stmt = conn.createStatement()

      try {
        stmt.execute(sql)
        println("createSchemaStructure ok")
      }
      finally {
        stmt.close
      }
    }
    finally {
      conn.close()
    }
  }

  //Create Table
  private def createTableStructure(): Unit = {
    Class.forName("org.h2.Driver")
    val conn: Connection = DriverManager.getConnection( "jdbc:h2:./h2db/TEST_SCHEMA", "sa", "" )

    try {
      val sql = "create table if not exists TEST_TABLE (ID int auto_increment primary key,NAME varchar(50));"
      val stmt = conn.createStatement()

      try {
        stmt.execute(sql)
        println("createTableStructure ok")
      }
      finally {
        stmt.close
      }
    }
    finally {
      conn.close()
    }
  }


  //Insert
  private def insertDbStructure(data1:String ): Unit = {
    Class.forName("org.h2.Driver")
    val conn: Connection = DriverManager.getConnection( "jdbc:h2:./h2db/TEST_SCHEMA", "sa", "" )

    try {
      val sql = "insert into TEST_TABLE (NAME) values ('" + data1 + "');"
      val stmt = conn.createStatement()

      try {
        stmt.executeUpdate(sql)
        println( "insert ok")
      }
      finally {
        stmt.close
      }
    }
    finally {
      conn.close()
    }
  }


  //Select
  private def selectDbStructure(): Unit = {
    Class.forName("org.h2.Driver")
    val conn: Connection = DriverManager.getConnection( "jdbc:h2:./h2db/TEST_SCHEMA", "sa", "" )

    try {
      val sql = "select * from TEST_TABLE;"
      val stmt = conn.createStatement()

      try {
        val rs: ResultSet = stmt.executeQuery(sql)
        while (rs.next()) {
          println(s"""${rs.getInt("id")}, ${rs.getString("name")}""")
        }
        rs.close()
      }
      finally {
        stmt.close
      }
      println("select ok")
    }
    finally {
      conn.close()
    }
  }
}
