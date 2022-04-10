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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A polled queue directory.
 */

public final class CServerDirectory implements Closeable
{
  private final CServerQueueDirectory directory;
  private final WatchService watcher;
  private final Consumer<Path> operation;

  /**
   * Create a new directory.
   *
   * @param in_directory The configuration
   * @param in_operation A receiver of "new file" events
   *
   * @throws IOException On errors
   */

  public CServerDirectory(
    final CServerQueueDirectory in_directory,
    final Consumer<Path> in_operation)
    throws IOException
  {
    this.directory = Objects.requireNonNull(in_directory, "directory");
    this.operation = Objects.requireNonNull(in_operation, "operation");

    final Path path = this.directory.directory();
    final FileSystem filesystem = path.getFileSystem();
    this.watcher = filesystem.newWatchService();
    path.register(this.watcher, StandardWatchEventKinds.ENTRY_CREATE);
  }

  /**
   * Poll the directory for new files.
   *
   * @return {@code true} if the directory can be polled again
   */

  @SuppressWarnings("unchecked")
  public boolean poll()
  {
    final WatchKey key;
    try {
      key = this.watcher.poll(1L, TimeUnit.SECONDS);
    } catch (final InterruptedException x) {
      Thread.currentThread().interrupt();
      return true;
    }

    if (key == null) {
      return true;
    }

    for (final WatchEvent<?> event : key.pollEvents()) {
      final WatchEvent.Kind<?> kind = event.kind();

      if (Objects.equals(kind, StandardWatchEventKinds.OVERFLOW)) {
        continue;
      }

      final WatchEvent<Path> ev = (WatchEvent<Path>) event;
      final Path filename = ev.context();
      this.operation.accept(filename);
    }

    return key.reset();
  }

  @Override
  public void close()
    throws IOException
  {
    this.watcher.close();
  }
}
