package adf.core.agent;

import rescuecore2.connection.Connection;

/** Closes transport workers after a shutdown message or a lost connection. */
final class AgentConnectionLifecycle {
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

  private void awaitShutdown() {
    try {
      while (!shutdownRequested && connection.isAlive()) {
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // Do this outside the connection's broadcast callback: shutdown joins
      // transport workers and must not race a callback waiting for that join.
      try {
        stopAgent.run();
      } finally {
        connection.shutdown();
      }
    }
  }
}
