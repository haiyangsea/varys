package varys.framework.master.ui

import varys.framework.FrameworkWebUI
import varys.framework.master.CoflowInfo
import varys.ui.UIUtils

import scala.xml.Node

/**
 * Created by hWX221863 on 2014/10/8.
 */
class CompleteCoflowsPage(parent: MasterWebUI) extends ItemsPage(parent) {
  val actives: Seq[String] = Seq("", "", "", "active")

  override def makeContent: Seq[Node] = {
    val state = getState
    val coflowHeaders = Seq("ID", "Name", "Submitted Time", "User", "State", "Duration")
    val completedCoflows = state.completedCoflows.sortBy(_.endTime).reverse
    UIUtils.listingTable(coflowHeaders, coflowRow, completedCoflows)
  }

  def coflowRow(coflow: CoflowInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"/coflow?coflowId=" + coflow.id} target="_blank">{coflow.id}</a>
      </td>
      <td>{coflow.desc.name}</td>
      <td>{FrameworkWebUI.formatDate(coflow.submitDate)}</td>
      <td>{coflow.desc.user}</td>
      <td>{coflow.curState.toString}</td>
      <td>{FrameworkWebUI.formatDuration(coflow.duration)}</td>
    </tr>
  }
}
