package varys.framework.master.ui

import javax.servlet.http.HttpServletRequest
import scala.xml.Node
import varys.framework.{FrameworkWebUI, RequestMasterState, MasterState}
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask
import varys.framework.master.{CoflowInfo, ClientInfo}
import varys.ui.UIUtils

/**
 * Created by hWX221863 on 2014/9/23.
 */
class ClientPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  implicit val timeout = parent.timeout

  def render(request: HttpServletRequest): Seq[Node] = {
    val clientId = request.getParameter("clientId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)

    val clientInfo: ClientInfo = state.activeClients
                                      .find(client => client.id == clientId)
                                      .getOrElse(null)
    val coflowHeaders = Seq("ID", "Name", "User", "Submit Date", "State", "Coflow Type",
      "Max Flows", "To Register Flow Count", "To Complete Flow Count")
    val coflowTable =  UIUtils.listingTable(coflowHeaders, coflowRow, clientInfo.coflows.toSeq)

    val content =  <div class="row">
      <div class="col-md-12">
        <ul class="unstyled">
          <li><strong>ID:</strong> {clientInfo.id}</li>
          <li><strong>Name:</strong> {clientInfo.name}</li>
          <li><strong>User:</strong> {clientInfo.user}</li>
          <li><strong>Submit Date:</strong> {clientInfo.submitDate}</li>
          <li><strong>Address:</strong> {clientInfo.host + ":" + clientInfo.commPort}</li>
          <li><strong>Start Time:</strong> {FrameworkWebUI.formatDate(clientInfo.startTime)}</li>
          <li><strong>Finish Time:</strong> {FrameworkWebUI.formatDate(clientInfo.endTime)}</li>
        </ul>
      </div>
    </div>
      <div class="row">
        <div class="col-md-12">
          <h3>Coflow In Client</h3>
          {coflowTable}
        </div>
      </div>;

    UIUtils.basicVarysPage(content, "Client: " + clientInfo.name)
  }


  def coflowRow(coflow: CoflowInfo): Seq[Node] = {
    <tr>
      <td> {coflow.id}</td>
      <td> {coflow.desc.name}</td>
      <td> {coflow.desc.user}</td>
      <td> {FrameworkWebUI.formatDate(coflow.submitDate)}</td>
      <td> {coflow.curState}</td>
      <td>{coflow.desc.coflowType.toString}</td>
      <td>{coflow.desc.maxFlows}</td>
      <td>{coflow.numFlowsToRegister}</td>
      <td>{coflow.numFlowsToComplete}</td>
    </tr>
  }
}
