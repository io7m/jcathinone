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

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * Functions to parse messages.
 */

public final class CMessages
{
  private CMessages()
  {

  }

  /**
   * Parse a message from the given stream.
   *
   * @param stream The input stream
   *
   * @return A parsed message
   *
   * @throws IOException On errors
   */

  public static CMessage ofStream(
    final InputStream stream)
    throws IOException
  {
    Objects.requireNonNull(stream, "stream");

    final Properties properties = new Properties();
    properties.load(stream);
    return ofProperties(properties);
  }

  /**
   * Parse a message from the given properties.
   *
   * @param properties The properties
   *
   * @return A parsed message
   *
   * @throws IOException On errors
   */

  public static CMessage ofProperties(
    final Properties properties)
    throws IOException
  {
    Objects.requireNonNull(properties, "properties");

    try {
      final CMessage.Builder builder = CMessage.builder();

      if (properties.containsKey("expiration")) {
        builder.setExpiry(Duration.parse(properties.getProperty("expiration")));
      }

      if (properties.containsKey("durable")) {
        builder.setDurable(Boolean.parseBoolean(properties.getProperty("durable")));
      }

      builder.setMessage(properties.getProperty("message"));
      return builder.build();
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }
}
