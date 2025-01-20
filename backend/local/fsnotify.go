package local

import (
	_ "unsafe" // use go:linkname

	_ "github.com/fsnotify/fsnotify" // access fsnotify.enableRecurse
)

// Hack to enable recursive watchers in fsnotify, which are available for
// Windows and Linux (although not quite what is needed for Linux here), but
// not yet Mac, hence there is no public interface for this in fsnotify just
// yet.

//go:linkname enableRecurse github.com/fsnotify/fsnotify.enableRecurse
var enableRecurse bool
