package edu.snu.reef.dolphin.core.metric;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Map;
import java.util.TreeMap;

/**
 * Metric tracker for maximum memory usage and average memory usage
 */
public final class MetricTrackerMemory implements MetricTracker {

  /**
   * time interval between two measurements of memory usage (millisecond)
   */
  @NamedParameter(doc = "Time interval between two measurements of memory usage (millisecond)",
      short_name = "memoryMeasureInterval", default_value = "100")
  public class MeasureInterval implements Name<Long> {
  }

  /**
   * key for the Max memory measure (maximum memory usage)
   */
  public final static String KEY_METRIC_MEMORY_MAX = "METRIC_MEMORY_MAX";

  /**
   * Key for the Average memory measure (average memory usage)
   */
  public final static String KEY_METRIC_MEMORY_AVERAGE = "METRIC_MEMORY_AVERAGE";

  /**
   * value returned when memory usage was never measured
   */
  public final static double VALUE_METRIC_MEMORY_UNKNOWN = -1.0;

  /**
   * time interval between metric tracking
   */
  private final long measureInterval;

  /**
   * maximum memory usage
   */
  private long maxMemory = 0;

  /**
   * sum of measured memory usage
   */
  private long sumMemory = 0;

  /**
   * number of times memory usage is measured
   */
  private int measureTimes = 0;

  /**
   * Whether the thread measuring memory usage should stop or not
   */
  private boolean shouldStop = true;

  /**
   * Whether the thread measuring should close or not
   */
  private boolean shouldTerminate = false;
  private final MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();

  /**
   * This class is instantiated by TANG
   *
   * Constructor for the memory usage tracker, which accepts the time interval between metric tracking as a parameter
   * @param measureInterval time interval between metric tracking
   */
  @Inject
  public MetricTrackerMemory(@Parameter(MeasureInterval.class) Long measureInterval) {
    this.measureInterval = measureInterval;
    new Thread(new Daemon()).start();
  }

  public void start() {
    synchronized(this) {
      shouldStop = false;
      maxMemory = 0;
      sumMemory = 0;
      measureTimes = 0;
      this.notify();
    }
  }

  public Map<String, Double> stop() {
    final Map<String, Double> result = new TreeMap<>();
    synchronized(this){
      shouldStop = true;
      if (measureTimes == 0) {
        result.put(KEY_METRIC_MEMORY_MAX, VALUE_METRIC_MEMORY_UNKNOWN);
        result.put(KEY_METRIC_MEMORY_AVERAGE, VALUE_METRIC_MEMORY_UNKNOWN);
      } else {
        result.put(KEY_METRIC_MEMORY_MAX, (double) maxMemory);
        result.put(KEY_METRIC_MEMORY_AVERAGE, ((double) sumMemory) / measureTimes);
      }
    }
    return result;
  }

  public void close() {
    synchronized(this) {
      shouldTerminate = true;
      this.notify();
    }
  }

  private class Daemon implements Runnable {

    @Override
    public void run() {

      while (true) {
        synchronized (MetricTrackerMemory.this) {
          if (shouldTerminate) {
            break;
          } else if (shouldStop) {
            try {

              //wait until stop or close method is called
              MetricTrackerMemory.this.wait();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            continue;
          } else {
            final long currentMemory = mbean.getHeapMemoryUsage().getUsed() + mbean.getNonHeapMemoryUsage().getUsed();
            maxMemory = Math.max(maxMemory, currentMemory);
            sumMemory += currentMemory;
            measureTimes += 1;
          }
        }
        try {
          Thread.sleep(measureInterval);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
