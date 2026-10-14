package adf.core.agent;

import rescuecore2.connection.Connection;
import rescuecore2.log.Logger;

/** Closes transport workers after a shutdown message or a lost connection. */
final class AgentConnectionLifecycle {
  private static final long CONNECTION_STATUS_POLL_INTERVAL_MILLIS = 100;

  // Maximum wait for agent processing to stop during shutdown or disconnect
  // cleanup. This does not limit think() during a running simulation.
  private static final long AGENT_SHUTDOWN_WAIT_TIMEOUT_MILLIS = 2_000;

  private final Connection connection;
  private final Runnable stopAgent;
  private volatile boolean shutdownRequested;

  AgentConnectionLifecycle(Connection connection, Runnable stopAgent) {
    this.connection = connection;
    this.stopAgent = stopAgent;
  }

  void start() {
    Thread monitor = new Thread(this::awaitShutdown, "ADF connection lifecycle");
    monitor.start();
  }

  void requestShutdown() {
    shutdownRequested = true;
  }

  boolean isShutdownRequested() {
    return shutdownRequested;
  }

  private void awaitShutdown() {
    boolean interrupted = false;
    try {
      while (!shutdownRequested && connection.isAlive()) {
        Thread.sleep(CONNECTION_STATUS_POLL_INTERVAL_MILLIS);
      }
    } catch (InterruptedException e) {
      // Complete cleanup before restoring the interrupt status.
      interrupted = true;
    } finally {
      shutdownRequested = true;
      Thread cleanup = new Thread(stopAgent, "ADF agent shutdown");
      cleanup.setDaemon(true);
      cleanup.start();
      // Shutdown joins transport workers; never call it in a broadcast callback.
      try {
        connection.shutdown();
      } finally {
        try {
          cleanup.join(AGENT_SHUTDOWN_WAIT_TIMEOUT_MILLIS);
          if (cleanup.isAlive()) {
            Logger.warn("Agent processing did not stop within "
                + AGENT_SHUTDOWN_WAIT_TIMEOUT_MILLIS + " ms during shutdown");
          }
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }
}
