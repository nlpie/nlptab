package dao

import java.util.Date
import javax.inject.{Singleton, Inject}

import models.System
import play.api.db.slick.{HasDatabaseConfigProvider, DatabaseConfigProvider}
import slick.driver.JdbcProfile

import scala.concurrent.Future

/**
  *
  */
@Singleton()
class SystemsDao @Inject() (protected val dbConfigProvider: DatabaseConfigProvider)
  extends HasDatabaseConfigProvider[JdbcProfile] {

  import driver.api._

  class Systems(tag: Tag) extends Table[System](tag, "SYSTEM") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")
    def description = column[String]("DESCRIPTION")
    def created = column[Option[Date]]("CREATED")
    def version = column[String]("VERSION")
    def workspaceId = column[Long]("WORKSPACE_ID")

    def * = (id.?, name, description, created, version, workspaceId) <> (System.tupled, System.unapply _)
  }

  private val systems = TableQuery[Systems]

  def findById(id: Long): Future[Option[System]] =
    db.run(systems.filter(_.id === id).result.headOption)

  def findByWorkspaceId(id: Long): Future[Seq[System]] =
    db.run(systems.filter(_.workspaceId === id).result).map(rows => rows.map { system => system })
}
