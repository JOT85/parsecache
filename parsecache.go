// Package parsecache provides a structure which caches parsed files and directory entries on top of a filesystem.
package parsecache

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// cleanPath attempts to return a standardized path for internal use.
//
// More specifically, the returned path will start with "/" and all . and .. components should be
// resolved.
func cleanPath(path string) string {
	return filepath.Clean("/" + path)
}

// opener returns an function that opens the specified, cleaned path, in a filesystem. It is for
// internal use. Paths should be cleaned by `cleanPath` before being passed to this function.
func opener(filesystem fs.FS, path string) func() (fs.File, error) {
	if path == "/" {
		path = "."
	} else if path[0] == '/' || path[0] == os.PathSeparator {
		path = path[1:]
	} else {
		panic("path not cleaned correctly")
	}
	return func() (fs.File, error) {
		return filesystem.Open(path)
	}
}

// FsCache is a cache on top of a generic filesystem. It's not safe for concurrent use, use
// `ConcurrentFsCache` for a thread-safe version.
//
// It can cache directory listings and parsed file content.
type FsCache[T any] struct {
	// fs is the underlying filesystem.
	fs fs.FS

	// parser is the function used to parse a file.
	parser Parser[T]

	// MaxAge is the maximum allowed age of a cache entry.
	MaxAge time.Duration

	// dirs is the map of cleanedPath -> cachedDir
	dirs map[string]*CachedDir

	// files is the map of cleanedPath -> cachedFile
	files map[string]*CachedFile[T]
}

// FsCache is a concurrency safe cache on top of a generic filesystem.
//
// It can cache directory listings and parsed file content.
type ConcurrentFsCache[T any] struct {
	// fs is the underlying filesystem, it is assumed that this is safe for concurrent use.
	fs fs.FS

	// parser is the function used to parse a file.
	parser Parser[T]

	// maxAge is the maximum allowed age of a cache entry.
	//
	// Since this is used by both file and directory caching, reading must acquire at least one of
	// `dirsLock` or `filesLock` and writing to must acquire both.
	maxAge time.Duration

	// dirs is the map of cleanedPath -> cachedDir
	dirs     map[string]*ConcurrentCachedDir
	dirsLock sync.RWMutex

	// files is the map of cleanedPath -> cachedFile
	files     map[string]*ConcurrentCachedFile[T]
	filesLock sync.RWMutex
}

func (cache *ConcurrentFsCache[T]) SetMaxAge(maxAge time.Duration) {
	cache.filesLock.Lock()
	cache.dirsLock.Lock()
	cache.maxAge = maxAge
	cache.dirsLock.Unlock()
	cache.filesLock.Unlock()
}

// ConcurrentCachedDir is a concurrency-safe wrapper around a `CachedDir`.
type ConcurrentCachedDir struct {
	lock      sync.RWMutex
	cachedDir CachedDir
}

// CachedDir stores a cache entry for a directory.
type CachedDir struct {
	// lastLoadTime is the time the cache was last *successfully* loaded or revalidated.
	// It will be nil if this has never occurred.
	lastLoadTime time.Time
	// lastSize is the filesize of the cache entry
	lastSize int64
	// lastModTime is the modtime of the cache entry
	lastModTime time.Time
	// entries is the value that was last *successfully* loaded.
	entries []fs.DirEntry
}

// ConcurrentCachedFile is a concurrency-safe wrapper around a `CachedFile`.
type ConcurrentCachedFile[T any] struct {
	lock       sync.RWMutex
	cachedFile CachedFile[T]
}

// CachedFile stores a cache entry for a file.
type CachedFile[T any] struct {
	// lastLoadTime is the time the cache was last *successfully* loaded or revalidated.
	// It will be nil if this has never occurred.
	lastLoadTime time.Time
	// lastSize is the filesize of the cache entry
	lastSize int64
	// lastModTime is the modtime of the cache entry
	lastModTime time.Time
	// entries is the value that was last *successfully* loaded and parsed from the file.
	content T
}

// NewFsCache creates a new cache on top of the `fs` filesystem, using `parser` to parse the content
// of files and sets the maximum age of cache entries to `maxAge`.
func NewFsCache[T any](fs fs.FS, parser Parser[T], maxAge time.Duration) FsCache[T] {
	cache := FsCache[T]{
		fs:     fs,
		parser: parser,
		MaxAge: maxAge,
	}
	cache.Clear()
	return cache
}

// NewConcurrentFsCache returns a new cache on top of the `fs` filesystem, safe for concurrent
// access, using `parser` to parse the content of files and sets the maximum age of cache entries to
// `maxAge`.
//
// `fs` must be safe for concurrent use.
func NewConcurrentFsCache[T any](fs fs.FS, parser Parser[T], maxAge time.Duration) *ConcurrentFsCache[T] {
	cache := ConcurrentFsCache[T]{
		fs:     fs,
		parser: parser,
		maxAge: maxAge,
	}
	cache.Clear()
	return &cache
}

// GetDirEntry gets the `CachedDir` for the path if one exists.
func (cache *FsCache[T]) GetDirEntry(path string) (entry *CachedDir, ok bool) {
	entry, ok = cache.dirs[cleanPath(path)]
	return
}

// GetDir gets the entries of a directory, which may be cached.
func (cache *FsCache[T]) GetDir(dir string) ([]fs.DirEntry, error) {
	return cache.GetDirWithMaxAge(dir, cache.MaxAge)
}

// GetDirWithMaxAge gets the entries of a directory, with the specified maximum age.
func (cache *FsCache[T]) GetDirWithMaxAge(dir string, maxAge time.Duration) ([]fs.DirEntry, error) {
	path := cleanPath(dir)
	cached, ok := cache.dirs[path]
	if !ok {
		cached = &CachedDir{}
		cache.dirs[path] = cached
	}
	entries, err := cached.Get(opener(cache.fs, path), maxAge)
	if err != nil {
		delete(cache.dirs, path)
	}
	return entries, err
}

// GetFileEntry gets the `CachedFile` for the path if one exists.
func (cache *FsCache[T]) GetFileEntry(path string) (entry *CachedFile[T], ok bool) {
	entry, ok = cache.files[cleanPath(path)]
	return
}

// GetFile returns the parsed content of a file, which may be cached.
func (cache *FsCache[T]) GetFile(file string) (T, error) {
	return cache.GetFileWithMaxAge(file, cache.MaxAge)
}

// GetFileWithMaxAge returns the parsed content of a file, with the specified maximum age.
func (cache *FsCache[T]) GetFileWithMaxAge(file string, maxAge time.Duration) (T, error) {
	path := cleanPath(file)
	cached, ok := cache.files[path]
	if !ok {
		cached = &CachedFile[T]{}
		cache.files[path] = cached
	}
	content, err := cached.Get(opener(cache.fs, path), cache.parser, maxAge)
	if err != nil {
		delete(cache.files, path)
	}
	return content, err
}

// GetDirEntry gets the `ConcurrentCachedDir` for the path if one exists.
func (cache *ConcurrentFsCache[T]) GetDirEntry(path string) (entry *ConcurrentCachedDir, ok bool) {
	cache.dirsLock.RLock()
	defer cache.dirsLock.RUnlock()
	entry, ok = cache.dirs[cleanPath(path)]
	return
}

// GetDir gets the entries of a directory, which may be cached.
func (cache *ConcurrentFsCache[T]) GetDir(dir string) ([]fs.DirEntry, error) {
	return cache.getDir(dir, 0, false)
}

// GetDirWithMaxAge gets the entries of a directory, with the specified maximum age.
func (cache *ConcurrentFsCache[T]) GetDirWithMaxAge(dir string, maxAge time.Duration) ([]fs.DirEntry, error) {
	return cache.getDir(dir, maxAge, true)
}

// getDir gets the entries of a directory. The maximum age is `maxAge` if `useMaxAge` or
// `cache.maxAge` otherwise.
func (cache *ConcurrentFsCache[T]) getDir(dir string, maxAge time.Duration, useMaxAge bool) ([]fs.DirEntry, error) {
	path := cleanPath(dir)

	// Read the existing cache entry (if it exists) and the maxAge
	cache.dirsLock.RLock()
	cached, ok := cache.dirs[path]
	if !useMaxAge {
		maxAge = cache.maxAge
	}
	cache.dirsLock.RUnlock()

	// Create a new entry if one didn't exist, we'll insert this later, if the load is successful.
	if !ok {
		cached = &ConcurrentCachedDir{}
	}

	// Get the content from the entry!
	entries, err := cached.Get(opener(cache.fs, path), maxAge)

	// Insert the new entry if required
	if !ok && err == nil {
		cache.dirsLock.Lock()
		cache.dirs[path] = cached
		cache.dirsLock.Unlock()
	}

	return entries, err
}

// GetFileEntry gets the `CachedFile` for the path if one exists.
func (cache *ConcurrentFsCache[T]) GetFileEntry(path string) (entry *ConcurrentCachedFile[T], ok bool) {
	cache.filesLock.RLock()
	defer cache.filesLock.RUnlock()
	entry, ok = cache.files[cleanPath(path)]
	return
}

// GetFile returns the parsed content of a file, which may be cached.
func (cache *ConcurrentFsCache[T]) GetFile(file string) (T, error) {
	return cache.getFile(file, 0, false)
}

// GetFileWithMaxAge returns the parsed content of a file, which may be cached.
func (cache *ConcurrentFsCache[T]) GetFileWithMaxAge(file string, maxAge time.Duration) (T, error) {
	return cache.getFile(file, maxAge, true)
}

// getFile gets the entries of a directory. The maximum age is `maxAge` if `useMaxAge` or
// `cache.maxAge` otherwise.
func (cache *ConcurrentFsCache[T]) getFile(file string, maxAge time.Duration, useMaxAge bool) (T, error) {
	path := cleanPath(file)

	// Read the existing cache entry (if it exists) and the maxAge
	cache.filesLock.RLock()
	cached, ok := cache.files[path]
	if !useMaxAge {
		maxAge = cache.maxAge
	}
	cache.filesLock.RUnlock()

	// Create a new entry if one didn't exist, we'll insert this later, if the load is successful.
	if !ok {
		cached = &ConcurrentCachedFile[T]{}
	}

	// Get the content from the entry!
	content, err := cached.Get(opener(cache.fs, path), cache.parser, maxAge)

	// Insert the new entry if required
	if !ok && err == nil {
		cache.filesLock.Lock()
		cache.files[path] = cached
		cache.filesLock.Unlock()
	}

	return content, err
}

// ClearDirs from the cache.
func (cache *FsCache[T]) ClearDirs() {
	cache.dirs = make(map[string]*CachedDir, 4)
}

// ClearFile from the cache.
func (cache *FsCache[T]) ClearFiles() {
	cache.files = make(map[string]*CachedFile[T], 16)
}

// Clear the cache.
func (cache *FsCache[T]) Clear() {
	cache.ClearDirs()
	cache.ClearFiles()
}

// ClearDirs from the cache.
func (cache *ConcurrentFsCache[T]) ClearDirs() {
	cache.dirsLock.Lock()
	defer cache.dirsLock.Unlock()
	cache.dirs = make(map[string]*ConcurrentCachedDir, 4)
}

// ClearFiles from the cache.
func (cache *ConcurrentFsCache[T]) ClearFiles() {
	cache.filesLock.Lock()
	defer cache.filesLock.Unlock()
	cache.files = make(map[string]*ConcurrentCachedFile[T], 16)
}

// Clear the cache.
func (cache *ConcurrentFsCache[T]) Clear() {
	cache.ClearDirs()
	cache.ClearFiles()
}

// Cached returns the cached entries, the time it was cached, and a boolean, which is true only if
// the cache entry has been loaded.
func (f *ConcurrentCachedDir) Cached() ([]fs.DirEntry, time.Time, bool) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.cachedDir.Cached()
}

// Cached returns the cached entries, the time it was cached, and a boolean, which is true only if
// the cache entry has been loaded.
func (f *CachedDir) Cached() ([]fs.DirEntry, time.Time, bool) {
	return f.entries, f.lastLoadTime, !f.lastLoadTime.IsZero()
}

// Cached returns the cached content, the time it was cached, and a boolean, which is true only if the
// cache entry has been loaded.
func (f *ConcurrentCachedFile[T]) Cached() (T, time.Time, bool) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.cachedFile.Cached()
}

// Cached returns the cached content, the time it was cached, and a boolean, which is true only if
// the cache entry has been loaded.
func (f *CachedFile[T]) Cached() (T, time.Time, bool) {
	return f.content, f.lastLoadTime, !f.lastLoadTime.IsZero()
}

// Get the directory entries, the results may be cached upto the specified `maxAge`.
//
// `open` should open the underlying file is required, this will be once or not at all.
func (f *ConcurrentCachedDir) Get(open func() (fs.File, error), maxAge time.Duration) ([]fs.DirEntry, error) {
	// Ideally, return only with a read lock!
	entries, cachedAt, ok := f.Cached()
	if ok && time.Since(cachedAt) < maxAge {
		return entries, nil
	}

	// Otherwise we call the underlying get method with a write lock.
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.cachedDir.Get(open, maxAge)
}

// Get the directory entries, the results may be cached upto the specified `maxAge`.
//
// `open` should open the underlying file is required, this will be once or not at all.
func (f *CachedDir) Get(open func() (fs.File, error), maxAge time.Duration) ([]fs.DirEntry, error) {
	loaded := !f.lastLoadTime.IsZero()
	loadTime := time.Now()

	// Always use the cached result if it's not too old.
	if loaded && loadTime.Sub(f.lastLoadTime) < maxAge {
		return f.entries, nil
	}

	// Otherwise, get the stats to check if this cache entry is still valid.
	file, err := open()
	if err != nil {
		return f.entries, err
	}
	defer file.Close()
	stats, err := file.Stat()
	if err != nil {
		return f.entries, err
	}
	size := stats.Size()
	modTime := stats.ModTime()

	// Use the cached result if the mod time and size haven't changed
	if loaded && size == f.lastSize && modTime == f.lastModTime {
		f.lastLoadTime = loadTime
		return f.entries, nil
	}

	// Actually read the file
	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		// TODO
		panic("directory doesn't implement ReadDirFile")
	}
	entries, err := dir.ReadDir(0)
	if err != nil {
		return f.entries, err
	}
	f.lastLoadTime = loadTime
	f.entries = entries
	f.lastSize = size
	f.lastModTime = modTime
	return f.entries, nil
}

// Get the parsed file content, the results may be cached upto the specified `maxAge`.
//
// `open` should open the underlying file is required, this will be once or not at all.
func (f *ConcurrentCachedFile[T]) Get(open func() (fs.File, error), parse func(fs.File) (T, error), maxAge time.Duration) (T, error) {
	// Ideally, return only with a read lock!
	content, cachedAt, ok := f.Cached()
	if ok && time.Since(cachedAt) < maxAge {
		return content, nil
	}

	// Otherwise we call the underlying get method with a write lock.
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.cachedFile.Get(open, parse, maxAge)
}

// Get the parsed file content, the results may be cached upto the specified `maxAge`.
//
// `open` should open the underlying file is required, this will be once or not at all.
func (f *CachedFile[T]) Get(open func() (fs.File, error), parse func(fs.File) (T, error), maxAge time.Duration) (T, error) {
	loaded := !f.lastLoadTime.IsZero()
	loadTime := time.Now()

	// Always use the cached result if it's not too old.
	if loadTime.Sub(f.lastLoadTime) < maxAge {
		return f.content, nil
	}

	// Otherwise, get the stats to check if this cache entry is still valid.
	file, err := open()
	if err != nil {
		return f.content, err
	}
	defer file.Close()
	stats, err := file.Stat()
	if err != nil {
		return f.content, err
	}
	size := stats.Size()
	modTime := stats.ModTime()

	// Use the cached result if the mod time and size haven't changed
	if loaded && size == f.lastSize && modTime == f.lastModTime {
		f.lastLoadTime = loadTime
		return f.content, nil
	}

	// Actually read the file
	content, err := parse(file)
	if err != nil {
		return f.content, err
	}
	f.lastLoadTime = loadTime
	f.content = content
	f.lastSize = size
	f.lastModTime = modTime
	return f.content, nil
}

// Parser parses the file contents into the type `T`.
type Parser[T any] func(fs.File) (T, error)

// JsonParser[T] is a value of type Parser[T] which parses a file as JSON.
func JsonParser[T any](f fs.File) (T, error) {
	decoder := json.NewDecoder(f)
	var parsed T
	err := decoder.Decode(&parsed)
	return parsed, err
}
