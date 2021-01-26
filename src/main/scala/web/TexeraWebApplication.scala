package web

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import Engine.Common.AmberTag.WorkflowTag
import akka.actor.{ActorRef, ActorSystem}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.servlet.ErrorPageErrorHandler
import texera.common.TexeraUtils
import web.resource.{SystemMetadataResource, WorkflowWebsocketResource}

object TexeraWebApplication {

  var actorSystem: ActorSystem = null


  def main(args: Array[String]): Unit = {
    // start actor master
    actorSystem = WebUtils.startActorMaster()

    // start web server
    val server = if (args.length > 1) args(0)
    else "server"
    val serverConfig = if (args.length > 2) args(1)
    else TexeraUtils.texeraHomePath.resolve("conf").resolve("web-config.yml").toString
    new TexeraWebApplication().run(server, serverConfig)
  }
}

class TexeraWebApplication extends io.dropwizard.Application[TexeraWebConfiguration] {

  override def initialize(bootstrap: Bootstrap[TexeraWebConfiguration]): Unit = {
    // serve static frontend GUI files
    bootstrap.addBundle(new FileAssetsBundle("./new-gui/dist/", "/", "index.html"))
    // add websocket bundle
    bootstrap.addBundle(new WebsocketBundle(classOf[WorkflowWebsocketResource]))
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(configuration: TexeraWebConfiguration, environment: Environment): Unit = {
    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    // redirect all 404 to index page, according to Angular routing requirements
    val eph = new ErrorPageErrorHandler
    eph.addErrorPage(404, "/")
    environment.getApplicationContext.setErrorHandler(eph)

    environment.jersey().register(classOf[SystemMetadataResource])
  }

}
