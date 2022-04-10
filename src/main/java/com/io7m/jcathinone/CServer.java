/*
 * Copyright © 2018 Mark Raynsford <code@io7m.com> https://www.io7m.com
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
 * IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package com.io7m.jcathinone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A server.
 */

public final class CServer
{
  private static final Logger LOG = LoggerFactory.getLogger(CServer.class);

  private final CServerConfiguration configuration;
  private final ExecutorService executor;
  private final AtomicBoolean done;

  private CServer(
    final CServerConfiguration in_configuration)
  {
    this.configuration =
      Objects.requireNonNull(in_configuration, "configuration");
    this.done =
      new AtomicBoolean(false);

    this.executor = Executors.newFixedThreadPool(
      in_configuration.queueDirectories().size(),
      runnable -> {
        final Thread thread = new Thread(runnable);
        thread.setName("com.io7m.jcathinone.server." + thread.getId());
        return thread;
      });

    for (final CServerQueueDirectory directory : in_configuration.queueDirectories()) {
      this.executor.execute(new QueueTask(this, directory));
    }
  }

  /**
   * Create and start a new server.
   *
   * @param configuration The configuration
   *
   * @return A new server
   */

  public static CServer create(
    final CServerConfiguration configuration)
  {
    return new CServer(configuration);
  }

  /**
   * Stop the server.
   */

  public void stop()
  {
    if (this.done.compareAndSet(false, true)) {
      this.executor.shutdown();
    }
  }

  private static final class QueueTask implements Runnable
  {
    private final CServerQueueDirectory directory;
    private final CServer server;
    private boolean failed;

    QueueTask(
      final CServer in_server,
      final CServerQueueDirectory in_directory)
    {
      this.server = Objects.requireNonNull(in_server, "in_server");
      this.directory = Objects.requireNonNull(in_directory, "directory");
      this.failed = false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run()
    {
      try {
        LOG.debug("task started for queue directory {}", this.directory.directory());

        try (CServerBrokerConnection connection = CServerBrokerConnection.create(this.directory)) {
          try (CServerDirectory dir = new CServerDirectory(
            this.directory, path -> this.processLogging(connection, path))) {

            final Path path = this.directory.directory();
            Files.list(path).forEach(file -> this.processLogging(connection, file));

            while (true) {
              LOG.trace("tick");

              if (this.server.done.get()) {
                LOG.debug("server is done");
                return;
              }

              if (!connection.isOpen()) {
                LOG.debug("connection is closed");
                this.resubmitTask();
                return;
              }

              if (this.failed) {
                LOG.debug("task failed");
                this.resubmitTask();
                return;
              }

              if (!dir.poll()) {
                LOG.debug("directory poll is finished");
                return;
              }
            }
          }
        }
      } catch (final Exception e) {
        LOG.error("error: ", e);
        this.resubmitTask();
      } finally {
        LOG.debug("task ended for queue directory {}", this.directory.directory());
      }
    }

    private void processLogging(
      final CServerBrokerConnection connection,
      final Path file)
    {
      final Path real =
        this.directory.directory()
          .resolve(file)
          .toAbsolutePath();

      try {
        process(connection, real);
      } catch (final Exception e) {
        LOG.error("error processing {}: ", real, e);
        this.failed = true;
      }
    }

    private static void process(
      final CServerBrokerConnection connection,
      final Path file)
      throws IOException
    {
      LOG.debug("processing: {}", file);

      try (InputStream stream = Files.newInputStream(file)) {
        connection.send(CMessages.ofStream(stream));
      }

      Files.deleteIfExists(file);
    }

    private void resubmitTask()
    {
      this.server.executor.execute(() -> {
        LOG.debug("waiting for 5 seconds before restarting task");

        try {
          Thread.sleep(5_000L);
        } catch (final InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        new QueueTask(this.server, this.directory).run();
      });
    }
  }
}
