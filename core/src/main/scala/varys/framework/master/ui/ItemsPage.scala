package varys.framework.master.ui

import javax.servlet.http.HttpServletRequest

import akka.pattern.ask
import varys.framework.{RequestMasterState, MasterState}
import varys.ui.UIUtils
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.Node

/**
 * Created by hWX221863 on 2014/10/8.
 */
abstract class ItemsPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  implicit val timeout = parent.timeout

  def makeContent: Seq[Node]
  val actives: Seq[String]

  def getState: MasterState = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    Await.result(stateFuture, 30 seconds)
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val state = getState
    val content = makeContent

    val page = <div class="row">
      <div class="col-md-12">
        <ul class="unstyled">
          <li><strong>URL:</strong> {state.uri}</li>
          <li><strong>Slaves:</strong> {state.slaves.size}</li>
          <li><strong>Coflows:</strong>
            {state.activeCoflows.size} Running,
            {state.completedCoflows.size} Completed </li>
        </ul>
      </div>
    </div>
      <div class="row">
        <div class="col-md-12">
          <nav class="navbar navbar-inverse" role="navigation">
            <div class="container-fluid">
              <div class="collapse navbar-collapse">
                <ul class="nav navbar-nav" role="tablist" id="nav-tab">
                  <li class={s"${actives(0)}"}><a href="/slaves">Slaves</a></li>
                  <li class={s"${actives(1)}"}><a href="/clients">Active Clients</a></li>
                  <li class={s"${actives(2)}"}><a href="/running-coflows">Running Coflows</a></li>
                  <li class={s"${actives(3)}"}><a href="/completed-coflows">Completed Coflows</a></li>
                </ul>
              </div>
            </div>
          </nav>
        </div>
      </div>
      <div class="row">
        <div class="col-md-12">
          {content}
        </div>
      </div>;
    UIUtils.basicVarysPage(page, "Varys Master at " + state.uri)
  }
}
