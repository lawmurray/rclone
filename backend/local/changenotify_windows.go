//go:build windows

package local

import (
	"context"
	"os"
	"path/filepath"
	"time"
	_ "unsafe" // use go:linkname

	"github.com/fsnotify/fsnotify"
	_ "github.com/fsnotify/fsnotify" // access fsnotify.enableRecurse
	"github.com/rclone/rclone/fs"
)

// Hack to enable recursive watchers in fsnotify, which are available for
// Windows and Linux (although not quite what is needed for Linux here), but
// not yet Mac, hence there is no public interface for this in fsnotify just
// yet. This is currently only needed for, and enabled for, Windows builds.
//
// Setting fsnotify.enableRecurse to true enables recursive handling: paths
// that end with with \... or /... as watched recursively.

//go:linkname enableRecurse github.com/fsnotify/fsnotify.enableRecurse
var enableRecurse bool

// ChangeNotify calls the passed function with a path that has had changes.
// Close the returned channel to stop being notified.
func (f *Fs) ChangeNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {

	// Create new watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		fs.Errorf(f, "Failed to create watcher: %s", err)
		return
	}

	// All known files and directories, used to call notifyFunc() with correct
	// entry type even on remove and rename events.
	known := make(map[string]fs.EntryType)

	// Files and directories that have changed in the last poll window.
	changed := make(map[string]fs.EntryType)

	// Walk the root directory to populate 'known'
	err = filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		entryPath, _ := filepath.Rel(f.root, path)
		entryType := fs.EntryObject
		if err != nil || d == nil {
			// The file or directory has already been removed, and we do not
			// know what type it was. It can be ignored, as this means it has
			// been both created and removed between ticks, which does not
			// change the diff.
			fs.Errorf(f, "Failed to walk %s, already removed? %s", path, err)
			return nil
		}
		if d.IsDir() {
			entryType = fs.EntryDirectory
		}
		known[entryPath] = entryType
		return nil
	})

	// Start goroutine to handle filesystem events
	go func() {
		// A polling implementation is used. While somewhat unnecessary, as
		// notifyFunc() could be called immediately on each filesystem event, it
		// turns out to have some advantages in accurately keeping track of the
		// entry type (i.e. file or directory) for each file, and allows the
		// interpretation of change events as a diff since the last set of
		// change events were sent.
		var ticker *time.Ticker
		var tickerC <-chan time.Time

	loop:
		for {
			select {
			case pollInterval, ok := <-pollIntervalChan:
				// Update ticker
				if !ok {
					if ticker != nil {
						ticker.Stop()
					}
					break loop
				}
				if ticker != nil {
					ticker.Stop()
					ticker, tickerC = nil, nil
				}
				if pollInterval != 0 {
					ticker = time.NewTicker(pollInterval)
					tickerC = ticker.C
				}
			case <-tickerC:
				// notify for all changed paths since last tick
				for entryPath, entryType := range changed {
					notifyFunc(filepath.ToSlash(entryPath), entryType)
				}
				changed = make(map[string]fs.EntryType)
			case event, ok := <-watcher.Events:
				if !ok {
					break loop
				}
				if event.Has(fsnotify.Create) {
					fs.Debugf(f, "Create: %s", event.Name)
				}
				if event.Has(fsnotify.Remove) {
					fs.Debugf(f, "Remove: %s", event.Name)
				}
				if event.Has(fsnotify.Rename) {
					fs.Debugf(f, "Rename: %s", event.Name)
				}
				if event.Has(fsnotify.Write) {
					fs.Debugf(f, "Write: %s", event.Name)
				}
				if event.Has(fsnotify.Chmod) {
					fs.Debugf(f, "Chmod: %s", event.Name)
				}

				// Determine the entry type (file or directory) using 'known'. This
				// is instead of Stat(), say, which is both expensive (a system
				// call) and does not work if the entry has been removed (including
				// removed before a creation or write event is handled).
				entryPath, _ := filepath.Rel(f.root, event.Name)
				entryType := fs.EntryObject

				if event.Has(fsnotify.Create) {
					info, err := os.Lstat(event.Name)
					if err != nil {
						// Entry has already been deleted, so cannot determine whether it
						// was a file or directory. It is ignored, as it does not affect
						// the state of the filesystem between the previous tick and the
						// next tick.
					} else {
						if info.IsDir() {
							entryType = fs.EntryDirectory
						}
						known[entryPath] = entryType
						changed[entryPath] = entryType
					}
				} else {
					entryType, ok := known[entryPath]
					if !ok {
						// By the time the create event was handled for this entry, it was
						// already deleted, and it could not be determined whether it was
						// a file or directory. It is ignored, as it does not affect the
						// state of the filesystem between the previous tick and the next
						// tick.
					} else {
						changed[entryPath] = entryType
						if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
							delete(known, entryPath)
						}
					}

					// Internally, fsnotify stops watching directories that are removed
					// or renamed, so it is not necessary to make updates to the watch
					// list.
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					break loop
				}
				fs.Errorf(f, "Error: %s", err.Error())
			}
		}

		// Close watcher
		err := watcher.Close()
		if err != nil {
			fs.Errorf(f, "Failed to close watcher: %s", err)
		}
	}()
}
