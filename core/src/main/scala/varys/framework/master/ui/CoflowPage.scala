/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package varys.framework.master.ui

import akka.pattern.ask

import javax.servlet.http.HttpServletRequest

import net.liftweb.json.JsonAST.JValue

import scala.xml.Node
import scala.concurrent.duration._

import varys.framework.{MasterState, RequestMasterState}
import varys.framework.JsonProtocol
import varys.ui.UIUtils
import scala.concurrent.Await
import varys.framework.master.FlowInfo
import varys.Utils

private[varys] class CoflowPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  implicit val timeout = parent.timeout

  /** Details for a particular Coflow */
  def renderJson(request: HttpServletRequest): JValue = {
    val coflowId = request.getParameter("coflowId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)
    val coflow = state.activeCoflows.find(_.id == coflowId).getOrElse({
      state.completedCoflows.find(_.id == coflowId).getOrElse(null)
    })
    JsonProtocol.writeCoflowInfo(coflow)
  }

  /** Details for a particular Coflow */
  def render(request: HttpServletRequest): Seq[Node] = {
    val coflowId = request.getParameter("coflowId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)
    val coflow = state.activeCoflows.find(_.id == coflowId).getOrElse({
      state.completedCoflows.find(_.id == coflowId).getOrElse(null)
    })
    val flowsHeader = Seq("Flow ID", "Max Receivers", "Flow Size", "Client Host", "State")
    val flowsTable = UIUtils.listingTable(flowsHeader, renderFlowRow, coflow.flows.toSeq)

    val content =
        <div class="row">
          <div class="col-md-12">
            <ul class="unstyled">
              <li><strong>ID:</strong> {coflow.id}</li>
              <li><strong>Name:</strong> {coflow.desc.name}</li>
              <li><strong>User:</strong> {coflow.desc.user}</li>
              <li><strong>Submit Date:</strong> {coflow.submitDate}</li>
              <li><strong>State:</strong> {coflow.curState}</li>
              <li><strong>Coflow Type:</strong>{coflow.desc.coflowType.toString}</li>
              <li><strong>Max Flows: </strong>{coflow.desc.maxFlows}</li>
              <li><strong>Number Of Flows To Regitster: </strong>{coflow.numFlowsToRegister}</li>
              <li><strong>Number Of Flows To Complete: </strong>{coflow.numFlowsToComplete}</li>
            </ul>
          </div>
        </div>
        <div class="row">
          <div class="col-md-12">
            <h3>Flows In Coflow</h3>
            {flowsTable}
          </div>
        </div>;

    UIUtils.basicVarysPage(content, "Coflow: " + coflow.desc.name)
  }

  def renderFlowRow(flow: FlowInfo) = {
    <tr>
      <td>{flow.desc.id}</td>
      <td>{flow.desc.maxReceivers}</td>
      <td>{Utils.bytesToString(flow.getFlowSize())}</td>
      <td>{if(flow.destClient == null) "Empty" else flow.destClient.host}</td>
      <td>{flow.state.toString}</td>
    </tr>
  }
}
