package varys.framework.master.ui

import akka.pattern.ask
import scala.concurrent.duration._
import varys.framework.{RequestMasterState, MasterState, FrameworkWebUI}
import varys.framework.master.ClientInfo
import varys.ui.UIUtils

import scala.concurrent.Await
import scala.xml.Node

/**
 * Created by hWX221863 on 2014/10/8.
 */
class ActiveClientsPage(parent: MasterWebUI) extends ItemsPage(parent) {
  val actives: Seq[String] = Seq("", "active", "", "")

  override def makeContent: Seq[Node] = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)

    val clientHeaders = Seq("ID", "Name", "User", "Address", "Submit Date", "Duration")
    UIUtils.listingTable(clientHeaders, clientRow, state.activeClients.toSeq)
  }

  def clientRow(client: ClientInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"client?clientId=" + client.id} target="_blank">{client.id}</a>
      </td>
      <td>{client.name}</td>
      <td>{client.user}</td>
      <td>{client.host + ":" + client.commPort}</td>
      <td>{FrameworkWebUI.formatDate(client.submitDate)}</td>
      <td>{FrameworkWebUI.formatDuration(client.duration)}</td>
    </tr>
  }
}
