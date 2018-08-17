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

    return new CServerBrokerConnection(configuration, locator, clients, session, producer);
  }

  @Override
  public void close()
    throws IOException
  {
    if (this.closed.compareAndSet(false, true)) {
      IOException exception = null;

      try {
        this.session.close();
      } catch (final ActiveMQException e) {
        exception = new IOException("Failed to close resources");
        exception.addSuppressed(e);
      }
      try {
        this.producer.close();
      } catch (final ActiveMQException e) {
        if (exception == null) {
          exception = new IOException("Failed to close resources");
        }
        exception.addSuppressed(e);
      }
      this.clients.close();
      this.locator.close();

      if (exception != null) {
        throw exception;
      }
    }
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
      message.writeBodyBufferBytes(cmessage.message().getBytes(UTF_8));
      this.producer.send(this.configuration.queueAddress(), message);
      this.session.commit();
    } catch (final ActiveMQException e) {
      throw new IOException(e);
    }
  }
}
