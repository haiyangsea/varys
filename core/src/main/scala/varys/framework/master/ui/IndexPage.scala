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
import scala.concurrent.duration._

import javax.servlet.http.HttpServletRequest

import net.liftweb.json.JsonAST.JValue

import scala.xml.Node

import varys.framework.FrameworkWebUI
import varys.framework.{MasterState, RequestMasterState}
import varys.framework.JsonProtocol
import varys.framework.master.{CoflowInfo, SlaveInfo, ClientInfo}
import varys.ui.UIUtils
import varys.Utils
import scala.concurrent.Await

private[varys] class IndexPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  implicit val timeout = parent.timeout

  def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)
    JsonProtocol.writeMasterState(state)
  }

  /** Index view listing coflows and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)

    val slaveHeaders = Seq("Id", "Address", "State")
    val slaves = state.slaves.sortBy(_.id)
    val slaveTable = UIUtils.listingTable(slaveHeaders, slaveRow, slaves)

    val clientHeaders = Seq("ID", "Name", "User", "Address", "Submit Date", "Duration")
    val clientTable = UIUtils.listingTable(clientHeaders, clientRow, state.activeClients.toSeq)

    val coflowHeaders = Seq("ID", "Name", "Submitted Time", "User", "State", "Duration")
    val activeCoflows = state.activeCoflows.sortBy(_.startTime).reverse
    val activeCoflowsTable = UIUtils.listingTable(coflowHeaders, coflowRow, activeCoflows)
    val completedCoflows = state.completedCoflows.sortBy(_.endTime).reverse
    val completedCoflowsTable = UIUtils.listingTable(coflowHeaders, coflowRow, completedCoflows)

    val content =
        <div class="row">
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
                    <li class="active"><a href="#slaves" role="tab" data-toggle="tab">Slaves</a></li>
                    <li><a href="#clients" role="tab" data-toggle="tab">Active Clients</a></li>
                    <li><a href="#running-coflows" role="tab" data-toggle="tab">Running Coflows</a></li>
                    <li><a href="#completed-coflows" role="tab" data-toggle="tab">Completed Coflows</a></li>
                  </ul>
                </div>
              </div>
            </nav>
          </div>
        </div>

        <div class="tab-content">
          <div class="tab-pane active" id="slaves">
            <div class="row">
              <div class="col-md-12">
                {slaveTable}
              </div>
            </div>
          </div>
          <div class="tab-pane" id="clients">
            <div class="row">
              <div class="col-md-12">
                {clientTable}
              </div>
            </div>
          </div>
          <div class="tab-pane" id="running-coflows">
            <div class="row">
              <div class="col-md-12">
                {activeCoflowsTable}
              </div>
            </div>
          </div>
          <div class="tab-pane" id="completed-coflows">
            <div class="row">
              <div class="col-md-12">
                {completedCoflowsTable}
              </div>
            </div>
          </div>
        </div>;
    UIUtils.basicVarysPage(content, "Varys Master at " + state.uri)
  }

  def slaveRow(slave: SlaveInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={slave.webUiAddress} target="_blank">{slave.id}</a>
      </td>
      <td>{slave.host}:{slave.port}</td>
      <td>{slave.state}</td>
    </tr>
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

  def coflowRow(coflow: CoflowInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"coflow?coflowId=" + coflow.id} target="_blank">{coflow.id}</a>
      </td>
      <td>{coflow.desc.name}</td>
      <td>{FrameworkWebUI.formatDate(coflow.submitDate)}</td>
      <td>{coflow.desc.user}</td>
      <td>{coflow.curState.toString}</td>
      <td>{FrameworkWebUI.formatDuration(coflow.duration)}</td>
    </tr>
  }
}
