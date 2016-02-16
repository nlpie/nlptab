package dao

import javax.inject.{Singleton, Inject}

import models.Workspace
import play.api.db.slick.{HasDatabaseConfigProvider, DatabaseConfigProvider}
import slick.driver.JdbcProfile

/**
  *
  */
@Singleton
class WorkspacesDao @Inject() (protected val dbConfigProvider: DatabaseConfigProvider)
  extends HasDatabaseConfigProvider[JdbcProfile] {

  import driver.api._

  class Workspaces(tag: Tag) extends Table[Workspace](tag, "WORKSPACE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")
    def description = column[String]("DESCRIPTION")

    def * = (id.?, name, description) <> (Workspace.tupled, Workspace.unapply _)
  }
}
