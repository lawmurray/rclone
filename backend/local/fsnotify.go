// Hack to enable recursive watchers in fsnotify, which are available for
// Windows and Linux (although not quite what is needed for Linux here), but
// not yet Mac, hence there is no public interface for this in fsnotify just
// yet. This is currently only needed for, and enabled for, Windows builds.

//go:build windows

package local

import (
	_ "unsafe" // use go:linkname

	_ "github.com/fsnotify/fsnotify" // access fsnotify.enableRecurse
)

// Setting fsnotify.enableRecurse to true enables recursive handling: paths
// that end with with \... or /... as watched recursively.

//go:linkname enableRecurse github.com/fsnotify/fsnotify.enableRecurse
var enableRecurse bool
