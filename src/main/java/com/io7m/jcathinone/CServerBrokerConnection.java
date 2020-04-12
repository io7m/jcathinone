/*
 * Copyright © 2018 Mark Raynsford <code@io7m.com> http://io7m.com
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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A connection to a message broker.
 */

public final class CServerBrokerConnection implements Closeable
{
  private static final Logger LOG = LoggerFactory.getLogger(CServerBrokerConnection.class);

  private final CServerQueueDirectory configuration;
  private final ServerLocator locator;
  private final ClientSessionFactory clients;
  private final ClientSession session;
  private final ClientProducer producer;
  private final AtomicBoolean closed;

  private CServerBrokerConnection(
    final CServerQueueDirectory in_configuration,
    final ServerLocator in_locator,
    final ClientSessionFactory in_clients,
    final ClientSession in_session,
    final ClientProducer in_producer)
  {
    this.configuration =
      Objects.requireNonNull(in_configuration, "configuration");
    this.locator =
      Objects.requireNonNull(in_locator, "locator");
    this.clients =
      Objects.requireNonNull(in_clients, "clients");
    this.session =
      Objects.requireNonNull(in_session, "session");
    this.producer =
      Objects.requireNonNull(in_producer, "producer");
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Create a new connection.
   *
   * @param configuration The configuration
   *
   * @return A new connection
   *
   * @throws Exception On errors
   */

  public static CServerBrokerConnection create(
    final CServerQueueDirectory configuration)
    throws Exception
  {
    final String address =
      new StringBuilder(64)
        .append("tcp://")
        .append(configuration.brokerAddress())
        .append(":")
        .append(configuration.brokerPort())
        .append("?sslEnabled=true")
        .toString();

    final ServerLocator locator =
      ActiveMQClient.createServerLocator(address);
    final ClientSessionFactory clients =
      locator.createSessionFactory();
    final ClientSession session =
      clients.createSession(
        configuration.brokerUser(),
        configuration.brokerPassword(),
        false,
        false,
        false,
        false,
        1);

    final ClientProducer producer =
      session.createProducer(configuration.queueAddress());

    final CServerBrokerConnection connection =
      new CServerBrokerConnection(configuration, locator, clients, session, producer);

    session.addFailureListener(new CServerSessionFailureListener(connection));
    return connection;
  }

  /**
   * @return {@code true} if the connection is still open
   */

  public boolean isOpen()
  {
    if (this.session.isClosed()) {
      return false;
    }
    if (this.producer.isClosed()) {
      return false;
    }
    return !this.closed.get();
  }

  @Override
  public void close()
    throws IOException
  {
    if (this.closed.compareAndSet(false, true)) {
      IOException exception = null;
      exception = this.closeProducer(exception);
      exception = this.closeSession(exception);
      exception = this.closeClientFactory(exception);
      exception = this.closeLocator(exception);
      if (exception != null) {
        throw exception;
      }
    }
  }

  private IOException closeLocator(
    final IOException exception)
  {
    IOException ioException = exception;
    try {
      LOG.trace("closing locator");
      this.locator.close();
    } catch (final Exception e) {
      if (ioException == null) {
        ioException = new IOException("Failed to close resources");
      }
      ioException.addSuppressed(e);
    } finally {
      LOG.trace("closed locator");
    }
    return ioException;
  }

  private IOException closeClientFactory(
    final IOException exception)
  {
    IOException ioException = exception;
    try {
      LOG.trace("closing client factory");
      this.clients.close();
    } catch (final Exception e) {
      if (ioException == null) {
        ioException = new IOException("Failed to close resources");
      }
      ioException.addSuppressed(e);
    } finally {
      LOG.trace("closed client factory");
    }
    return ioException;
  }

  private IOException closeSession(
    final IOException exception)
  {
    IOException ioException = exception;
    try {
      LOG.trace("closing session");
      this.session.close();
    } catch (final Exception e) {
      if (ioException == null) {
        ioException = new IOException("Failed to close resources");
      }
      ioException.addSuppressed(e);
    } finally {
      LOG.trace("closed session");
    }
    return ioException;
  }

  private IOException closeProducer(
    final IOException exception)
  {
    IOException ioException = exception;
    try {
      LOG.trace("closing producer");
      this.producer.close();
    } catch (final Exception e) {
      ioException = new IOException("Failed to close resources");
      ioException.addSuppressed(e);
    } finally {
      LOG.trace("closed producer");
    }
    return ioException;
  }

  /**
   * Send the given message.
   *
   * @param cmessage The message
   *
   * @throws IOException On errors
   */

  public void send(
    final CMessage cmessage)
    throws IOException
  {
    try {
      final ClientMessage message = this.session.createMessage(cmessage.durable());
      cmessage.expiry().ifPresent(
        duration -> {
          final long expiration = System.currentTimeMillis() + duration.toMillis();
          message.setExpiration(expiration);
        });

      message.setTimestamp(System.currentTimeMillis());
      final byte[] bytes = cmessage.message().getBytes(UTF_8);
      message.writeBodyBufferBytes(bytes);

      if (LOG.isDebugEnabled()) {
        LOG.debug("sending {} octet message (expires {}, durable {})",
                  Integer.valueOf(bytes.length),
                  cmessage.expiry(),
                  Boolean.valueOf(cmessage.durable()));
      }

      this.producer.send(this.configuration.queueAddress(), message);
      this.session.commit();
    } catch (final ActiveMQException e) {
      throw new IOException(e);
    }
  }

  private static class CServerSessionFailureListener
    implements SessionFailureListener
  {
    private final CServerBrokerConnection connection;

    CServerSessionFailureListener(
      final CServerBrokerConnection inConnection)
    {
      this.connection =
        Objects.requireNonNull(inConnection, "inConnection");
    }

    @Override
    public void beforeReconnect(
      final ActiveMQException exception)
    {
      LOG.debug("reconnect: ", exception);

      try {
        this.connection.close();
      } catch (final IOException e) {
        LOG.debug("connection close failed: ", exception);
      }
    }

    @Override
    public void connectionFailed(
      final ActiveMQException exception,
      final boolean failedOver)
    {
      LOG.debug("connection failed: ", exception);

      try {
        this.connection.close();
      } catch (final IOException e) {
        LOG.debug("connection close failed: ", exception);
      }
    }

    @Override
    public void connectionFailed(
      final ActiveMQException exception,
      final boolean failedOver,
      final String scaleDownTargetNodeID)
    {
      try {
        this.connection.close();
      } catch (final IOException e) {
        LOG.debug("connection close failed: ", exception);
      }
    }
  }
}
