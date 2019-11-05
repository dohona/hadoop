package cntic;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

public class DirectoryWatcher {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    // first create the watch service instance. This service watches a
    // directory for changes.
    WatchService watchService = FileSystems.getDefault().newWatchService();
    // the directory that has to be watched needs to be registered. Any
    // object that implements the
    // Watchable interface can be registered. lets register the temp
    // directory
    Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));

    tempDir = Paths.get("g:\\tmp\\Test");

    System.out.println(tempDir);
    // we register three events. i.e. whenever a file is created, deleted or
    // modified the watcher gets informed
    WatchKey key =
        tempDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE,
            StandardWatchEventKinds.ENTRY_MODIFY);

    boolean stopPolling = false;
    // we can poll for events in an infinite loop
    for (;;) {
      try {
        // the take method waits till watch service receives a
        // notification
        key = watchService.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      // once a key is obtained, we poll for events on that key
      List<WatchEvent<?>> keys = key.pollEvents();
      for (WatchEvent<?> watchEvent : keys) {
        // get the kind of event

        WatchEvent<Path> ev = (WatchEvent<Path>) watchEvent;
        Path filename = ev.context();
        Path fullChildPath = tempDir.resolve(filename);

        System.out.println("Fullname: "
            + fullChildPath);
        if (Files.isDirectory(fullChildPath)) {
          System.out.println("It is directory!");
          continue;
        }

        Kind<?> watchEventKind = watchEvent.kind();
        // sometimes events are created faster than they are registered
        // or the implementation
        // may specify a maximum number of events and further events are
        // discarded. In these cases
        // an event of kind overflow is returned. We ignore this case
        // for nowl
        if (watchEventKind == StandardWatchEventKinds.OVERFLOW) {
          continue;
        }
        if (watchEventKind == StandardWatchEventKinds.ENTRY_CREATE) {
          // a new file has been created
          // print the name of the file. To test this, go to the temp
          // directory
          // and create a plain text file. name the file a.txt. If you
          // are on windows, watch what happens!
          System.out.println("File Created:"
              + filename.toString());
        } else if (watchEventKind == StandardWatchEventKinds.ENTRY_MODIFY) {
          // The file has been modified. Go to the file created above
          // and modify it
          System.out.println("File Modified:"
              + filename);
        } else if (watchEventKind == StandardWatchEventKinds.ENTRY_DELETE) {
          // the file has been deleted. delete the file. and exit the
          // loop.
          System.out.println("File deleted:"
              + watchEvent.context());
          if ("a.txt".equals(filename.toString())) {
            stopPolling = true;
          }
        }

      }

      // we need to reset the key so the further key events may be
      // polled
      boolean valid = key.reset();
      if (stopPolling
          || !valid) {
        break;
      }
    }
    // close the watcher service
    watchService.close();
  }
}
