package varys.framework.network;

/**
 * Created by hWX221863 on 2015/4/15.
 */
public class NoThrottle extends Throttle
{
  @Override
  public void throttle()
  {
    // do nothing
  }

  @Override
  public void updateRate(double newRate)
  {
    // do nothing
  }

  @Override
  public long getTotalSleepTime() {
    return 0;
  }
}
