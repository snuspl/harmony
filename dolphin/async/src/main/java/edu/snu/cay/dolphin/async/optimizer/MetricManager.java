/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A temporary storage for holding worker and server metrics related to optimization.
 * Users can optionally run a dashboard server, which visualizes the received metrics
 * (See {@link edu.snu.cay.common.param.Parameters.DashboardPort}).
 */
@DriverSide
public final class MetricManager {
  private static final Logger LOG = Logger.getLogger(MetricManager.class.getName());

  /**
   * Size of the queue for saving unsent metrics to the Dashboard server.
   */
  private static final int METRIC_QUEUE_SIZE = 1024;

  /**
   * Worker-side metrics, each in the form of (workerId, {@link EvaluatorParameters}) mapping.
   */
  private final Map<String, List<EvaluatorParameters>> workerEvalParams;

  /**
   * Server-side metrics, each in the form of (serverId, {@link EvaluatorParameters}) mapping.
   */
  private final Map<String, List<EvaluatorParameters>> serverEvalParams;

  /**
   * A flag to enable/disable metric collection. It is disabled by default.
   */
  private boolean metricCollectionEnabled;

  /**
   * A map that contains each evaluator's mapping to the number of blocks it contains.
   * The map is loaded only when metric collection is enabled.
   */
  private volatile Map<String, Integer> numBlockByEvalIdForWorker;
  private volatile Map<String, Integer> numBlockByEvalIdForServer;

  private final DashboardSetupStatus dashboardSetupStatus;

  /**
   * Thread for sending metrics to dashboard server.
   */
  private final ExecutorService metricsSenderExecutor = Executors.newSingleThreadScheduledExecutor();

  /**
   * Metrics request queue which saves unsent requests.
   */
  private final ArrayBlockingQueue<String> metricsRequestQueue = new ArrayBlockingQueue<String>(METRIC_QUEUE_SIZE);

  /**
   * Constructor of MetricManager.
   * @param hostAddress Host address of the dashboard server. The address is set lazily at the constructor
   *                    if the client has configured a feasible port number.
   * @param port        Port number of dolphin dashboard server.
   */
  @Inject
  private MetricManager(@Parameter(Parameters.DashboardHostAddress.class) final String hostAddress,
                        @Parameter(Parameters.DashboardPort.class) final int port) {
    this.workerEvalParams = Collections.synchronizedMap(new HashMap<>());
    this.serverEvalParams = Collections.synchronizedMap(new HashMap<>());
    this.metricCollectionEnabled = false;
    this.numBlockByEvalIdForWorker = null;
    this.numBlockByEvalIdForServer = null;

    this.dashboardSetupStatus = initDashboard(hostAddress, port);
  }

  private static final class DashboardSetupStatus {
    /**
     * {@code true} if the Dashboard server is in use.
     */
    private final boolean dashboardEnabled;

    /**
     * URL of Dolphin dashboard server. Empty if not using dashboard.
     */
    private final String dashboardURL;

    /**
     * Reusable HTTP client managed with PoolingHttpClientConnectionManager.
     */
    private final CloseableHttpAsyncClient reusableHttpClient;

    private DashboardSetupStatus(final boolean dashboardEnabled,
                                 final String dashboardURL,
                                 final CloseableHttpAsyncClient reusableHttpClient) {
      this.dashboardEnabled = dashboardEnabled;
      this.dashboardURL = dashboardURL;
      this.reusableHttpClient = reusableHttpClient;
    }

    private static DashboardSetupStatus getFailed() {
      return new DashboardSetupStatus(false, null, null);
    }

    private static DashboardSetupStatus getSuccessful(final String dashboardURL,
                                                      final CloseableHttpAsyncClient reusableHttpClient) {
      return new DashboardSetupStatus(true, dashboardURL, reusableHttpClient);
    }
  }

  private DashboardSetupStatus initDashboard(final String hostAddress, final int port) {
    final boolean invalidHostAddr = hostAddress.equals(AsyncDolphinLauncher.INVALID_HOST_ADDRESS);

    if (invalidHostAddr) {
      LOG.log(Level.INFO, "Dashboard is not in use");
      return DashboardSetupStatus.getFailed();

    } else {
      final String dashboardURL = "http://" + hostAddress + ":" + port + "/";
      try {
        // make a pool of http requests with request limitation of INT_MAX.
        final PoolingNHttpClientConnectionManager connectionManager
            = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
        connectionManager.setMaxTotal(Integer.MAX_VALUE);
        final CloseableHttpAsyncClient reusableHttpClient =
            HttpAsyncClients.custom().setConnectionManager(connectionManager).build();
        reusableHttpClient.start();

        // run another thread to send metrics.
        runMetricsSenderThread();

        return DashboardSetupStatus.getSuccessful(dashboardURL, reusableHttpClient);
      } catch (IOReactorException e) {
        LOG.log(Level.WARNING, "Dashboard: Fail on initializing IOReactor.", e);
        return DashboardSetupStatus.getFailed();
      }
    }
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    if (metricCollectionEnabled) {
      final int numDataBlocksOnWorker = metrics.getNumDataBlocks();

      if (numBlockByEvalIdForWorker != null && numBlockByEvalIdForWorker.containsKey(workerId)) {
        final int numDataBlocksOnDriver = numBlockByEvalIdForWorker.get(workerId);

        if (numDataBlocksOnWorker == numDataBlocksOnDriver) {
          final DataInfo dataInfo = new DataInfoImpl(numDataBlocksOnWorker);
          final EvaluatorParameters evaluatorParameters = new WorkerEvaluatorParameters(workerId, dataInfo, metrics);
          synchronized (workerEvalParams) {
            if (!workerEvalParams.containsKey(workerId)) {
              workerEvalParams.put(workerId, new ArrayList<>());
            }
            workerEvalParams.get(workerId).add(evaluatorParameters);
          }
        } else {
          LOG.log(Level.FINE, "{0} contains {1} blocks, driver says {2} blocks. Dropping metric.",
              new Object[]{workerId, numDataBlocksOnWorker, numDataBlocksOnDriver});
        }
      } else {
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", workerId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", workerId);
    }

    // Regardless of metrics' validity, we send metrics to the dashboard for monitoring purpose.
    if (dashboardSetupStatus.dashboardEnabled) {
      try {
        metricsRequestQueue.put(String.format("id=%s&metrics=%s&time=%d",
            workerId, metrics, System.currentTimeMillis()));
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Dashboard: Interrupted while taking metrics to send from the queue.", e);
      }
    }
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain server.
   * This method does not override existing metrics with the same {@code serverId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeServerMetrics(final String serverId, final ServerMetrics metrics) {
    if (metricCollectionEnabled) {
      final int numModelBlocksOnServer = metrics.getNumModelBlocks();

      if (numBlockByEvalIdForServer != null && numBlockByEvalIdForServer.containsKey(serverId)) {
        final int numModelBlocksOnDriver = numBlockByEvalIdForServer.get(serverId);

        if (numModelBlocksOnServer == numModelBlocksOnDriver) {
          final DataInfo dataInfo = new DataInfoImpl(numModelBlocksOnServer);
          final EvaluatorParameters evaluatorParameters = new ServerEvaluatorParameters(serverId, dataInfo, metrics);
          synchronized (serverEvalParams) {
            if (!serverEvalParams.containsKey(serverId)) {
              serverEvalParams.put(serverId, new ArrayList<>());
            }
            serverEvalParams.get(serverId).add(evaluatorParameters);
          }
        } else {
          LOG.log(Level.FINE, "{0} contains {1} blocks, driver says {2} blocks. Dropping metric.",
              new Object[]{serverId, numModelBlocksOnServer, numModelBlocksOnDriver});
        }
      } else {
        LOG.log(Level.FINE, "No information about {0}. Dropping metric.", serverId);
      }
    } else {
      LOG.log(Level.FINE, "Metric collection disabled. Dropping metric from {0}", serverId);
    }

    // Regardless of metrics' validity, we send metrics to the dashboard for monitoring purpose.
    if (dashboardSetupStatus.dashboardEnabled) {
      try {
        metricsRequestQueue.put(String.format("id=%s&metrics=%s&time=%d",
            serverId, metrics, System.currentTimeMillis()));
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Dashboard: Interrupted while taking metrics to send from the queue.", e);
      }
    }
  }

  public Map<String, List<EvaluatorParameters>> getWorkerMetrics() {
    synchronized (workerEvalParams) {
      final Map<String, List<EvaluatorParameters>> currWorkerMetrics = new HashMap<>();

      for (final Map.Entry<String, List<EvaluatorParameters>> entry : workerEvalParams.entrySet()) {
        currWorkerMetrics.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
      return currWorkerMetrics;
    }
  }

  public Map<String, List<EvaluatorParameters>> getServerMetrics() {
    synchronized (serverEvalParams) {
      final Map<String, List<EvaluatorParameters>> currServerMetrics = new HashMap<>();

      for (final Map.Entry<String, List<EvaluatorParameters>> entry : serverEvalParams.entrySet()) {
        currServerMetrics.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
      return currServerMetrics;
    }
  }

  /**
   * Stops metric collection and clear metrics collected until this point.
   */
  public void stopMetricCollection() {
    LOG.log(Level.INFO, "Metric collection stopped!");
    metricCollectionEnabled = false;
    clearServerMetrics();
    clearWorkerMetrics();
  }

  /**
   * Starts metric collection and loads information required for metric validation.
   */
  public void startMetricCollection() {
    LOG.log(Level.INFO, "Metric collection started!");
    metricCollectionEnabled = true;
  }

  /**
   * Loads information required for metric validation.
   * Any information to be used for metric validation may be added here
   * and used to filter out invalid incoming metric in
   * {@link #storeWorkerMetrics(String, WorkerMetrics)} or {@link #storeServerMetrics(String, ServerMetrics)}
   */
  public void loadMetricValidationInfo(final Map<String, Integer> numBlockForWorker,
                                       final Map<String, Integer> numBlockForServer) {
    this.numBlockByEvalIdForWorker = numBlockForWorker;
    this.numBlockByEvalIdForServer = numBlockForServer;
  }

  /**
   * Empty out the current set of worker metrics.
   */
  private void clearWorkerMetrics() {
    synchronized (workerEvalParams) {
      workerEvalParams.clear();
    }
  }

  /**
   * Empty out the current set of server metrics.
   */
  private void clearServerMetrics() {
    synchronized (serverEvalParams) {
      serverEvalParams.clear();
    }
  }

  /**
   * Runs a thread watching the metrics queue to send the metrics from the metricsRequestQueue via http request.
   */
  private void runMetricsSenderThread() {
    metricsSenderExecutor.execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            final String request = metricsRequestQueue.take();
            sendMetricsToDashboard(request);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Dashboard: Interrupted while sending metrics to the dashboard server.", e);
          }
        }
      }
    });
  }

  private class DashboardResponseCallback implements FutureCallback<HttpResponse> {
    @Override
    public void completed(final HttpResponse result) {
      final int code = result.getStatusLine().getStatusCode();
      if (code != HttpStatus.SC_OK) {
        LOG.log(Level.WARNING, "Dashboard: Post request failed. Code-{0}", code);
      }
    }

    @Override
    public void failed(final Exception ex) {
      //TODO #772: deal with request failure.
      LOG.log(Level.WARNING, "Dashboard: Post request failed.", ex);
    }

    @Override
    public void cancelled() {
      //TODO #772: deal with request failure.
      LOG.log(Level.WARNING, "Dashboard: Post request cancelled.");
    }
  }

  /**
   * Send metrics to Dashboard server.
   * @param request The POST request content which is to be sent to the dashboard server.
   */
  private void sendMetricsToDashboard(final String request) {
    try {
      final HttpPost httpPost = new HttpPost(dashboardSetupStatus.dashboardURL);
      httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
      httpPost.setEntity(new StringEntity(request));
      dashboardSetupStatus.reusableHttpClient.execute(httpPost, new DashboardResponseCallback());
    } catch (IOException e) {
      //TODO #772: deal with request failure.
      LOG.log(Level.WARNING, "Dashboard: post request failed.", e);
    }
  }
}