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
import java.nio.file.FileSystem;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Functions to parse configurations.
 */

public final class CServerConfigurations
{
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  private CServerConfigurations()
  {

  }

  /**
   * Parse a configuration from the given properties.
   *
   * @param filesystem The source filesystem
   * @param properties The input properties
   *
   * @return A parsed message
   *
   * @throws IOException On errors
   */

  public static CServerConfiguration ofProperties(
    final FileSystem filesystem,
    final Properties properties)
    throws IOException
  {
    Objects.requireNonNull(filesystem, "filesystem");
    Objects.requireNonNull(properties, "properties");

    try {
      final CServerConfiguration.Builder builder = CServerConfiguration.builder();

      final String directories_value =
        properties.getProperty("com.io7m.jcathinone.directories");

      for (final String directory : WHITESPACE.split(directories_value)) {
        builder.addQueueDirectories(parseQueueDirectory(filesystem, properties, directory));
      }

      return builder.build();
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }

  private static CServerQueueDirectory parseQueueDirectory(
    final FileSystem filesystem,
    final Properties properties,
    final String directory)
  {
    final CServerQueueDirectory.Builder builder =
      CServerQueueDirectory.builder();

    builder.setDirectory(
      filesystem.getPath(properties.getProperty(
        String.format("com.io7m.jcathinone.directories.%s.path", directory))));
    builder.setQueueAddress(properties.getProperty(
      String.format("com.io7m.jcathinone.directories.%s.queue_address", directory)));
    builder.setBrokerUser(properties.getProperty(
      String.format("com.io7m.jcathinone.directories.%s.broker_user", directory)));
    builder.setBrokerPassword(properties.getProperty(
      String.format("com.io7m.jcathinone.directories.%s.broker_password", directory)));
    builder.setBrokerAddress(properties.getProperty(
      String.format("com.io7m.jcathinone.directories.%s.broker_address", directory)));
    builder.setBrokerPort(Integer.parseInt(properties.getProperty(
      String.format("com.io7m.jcathinone.directories.%s.broker_port", directory))));

    return builder.build();
  }
}
