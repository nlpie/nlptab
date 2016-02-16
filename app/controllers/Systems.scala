package controllers

import javax.inject.{Inject, Singleton}

import dao.SystemsDao
import play.api.mvc._

/**
  *
  */
@Singleton
class Systems @Inject() (systemsDao: SystemsDao) extends Controller {
  def withId(id: Long) = Action.async { implicit request =>
    val optionalSystemFuture = systemsDao.findById(id)
    optionalSystemFuture.map { case(optionalSystem) =>
      optionalSystem match {
        case Some(system) => Ok(system)
        case None => NotFound
      }
    }
  }


  def withWorkspaceId(id: Long) = Action.async {
    val optionalSystemsFuture = systemsDao
  }
}
