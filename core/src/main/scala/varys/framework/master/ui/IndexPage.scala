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

import varys.framework.{MasterState, RequestMasterState}
import varys.framework.JsonProtocol
import varys.framework.master.SlaveInfo
import varys.ui.UIUtils
import scala.concurrent.Await

private[varys] class IndexPage(parent: MasterWebUI) extends ItemsPage(parent) {

  def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)
    JsonProtocol.writeMasterState(state)
  }

  val actives: Seq[String] = Seq("active", "", "", "")
  override def makeContent: Seq[Node] = {
    val state = getState
    val slaveHeaders = Seq("Id", "Address", "State")
    val slaves = state.slaves.sortBy(_.id)
    UIUtils.listingTable(slaveHeaders, slaveRow, slaves)
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


}
