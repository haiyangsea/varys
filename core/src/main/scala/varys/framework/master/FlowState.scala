package varys.framework.master

/**
 * Created by hWX221863 on 2014/9/28.
 */
private[varys] object FlowState  extends Enumeration {
  type FlowState = Value

  val FRESH,ACTIVE,FINISH = Value
}
