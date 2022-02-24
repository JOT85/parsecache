package parsecache

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testFileStructure struct {
	Hello  string
	Number uint16
	Float  float32
}

type testInterface interface {
	GetFile(string) (testFileStructure, error)
	GetDir(string) ([]fs.DirEntry, error)
}

func cacheTests(t *testing.T, cache testInterface, maxAge time.Duration, dir string) {
	os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{
    "Hello": "world!",
    "Number": 12,
    "Float": -0.3
}`), 0660)
	os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{
    "Number": 600,
    "Float": 0.9
}`), 0660)
	time.Sleep(time.Second / 10)

	a, err := cache.GetFile("a.json")
	if err != nil {
		panic(err)
	}
	if a.Hello != "world!" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not parsed correctly")
	}
	a, err = cache.GetFile("a.json")
	if err != nil {
		panic(err)
	}
	if a.Hello != "world!" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not cached correctly")
	}

	b, err := cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 600 || b.Float != 0.9 {
		t.Error("b.json not parsed correctly")
	}
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 600 || b.Float != 0.9 {
		t.Error("b.json not cached correctly")
	}

	if a.Hello != "world!" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not cached correctly 3")
	}
	a, err = cache.GetFile("a.json")
	if err != nil {
		panic(err)
	}
	if a.Hello != "world!" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not cached correctly 4")
	}

	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 600 || b.Float != 0.9 {
		t.Error("b.json not cached correctly 2")
	}

	os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{
    "Hello": "change",
    "Number": 12,
    "Float": -0.3
}`), 0660)
	a, err = cache.GetFile("a.json")
	if err != nil {
		panic(err)
	}
	if a.Hello != "world!" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not cached correctly 5")
	}
	time.Sleep(maxAge)
	a, err = cache.GetFile("a.json")
	if err != nil {
		panic(err)
	}
	t.Log(a)
	if a.Hello != "change" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not invalidated correctly")
	}
	time.Sleep(maxAge + time.Second/10)
	a, err = cache.GetFile("a.json")
	if err != nil {
		panic(err)
	}
	t.Log(a)
	if a.Hello != "change" || a.Number != 12 || a.Float != -0.3 {
		t.Error("a.json not cached correctly 6")
	}
	bStats, err := os.Stat(filepath.Join(dir, "b.json"))
	if err != nil {
		panic(err)
	}
	// Change b but keep it the same size and modtime, meaning the cache won't be invalidated.
	os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{
    "Number": 700,
    "Float": 0.9
}`), 0660)
	err = os.Chtimes(filepath.Join(dir, "b.json"), time.Now(), bStats.ModTime())
	if err != nil {
		panic(err)
	}

	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 600 || b.Float != 0.9 {
		t.Error("b.json not cached correctly 3")
	}
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 600 || b.Float != 0.9 {
		t.Error("b.json not cached correctly 4")
	}

	secondModTime := time.Now()
	err = os.Chtimes(filepath.Join(dir, "b.json"), secondModTime, secondModTime)
	if err != nil {
		panic(err)
	}
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 600 || b.Float != 0.9 {
		t.Error("b.json not cached correctly 4")
	}

	time.Sleep(maxAge + time.Second/10)
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 700 || b.Float != 0.9 {
		t.Error("b.json not invalidated correctly")
	}

	// Change b but keep it the same modtime, meaning the cache will be invalidated by the size.
	os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{
    "Number": 0,
    "Float": 0.9
}`), 0660)
	err = os.Chtimes(filepath.Join(dir, "b.json"), time.Now(), secondModTime)
	if err != nil {
		panic(err)
	}
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 700 || b.Float != 0.9 {
		t.Error("b.json not cached correctly 5")
	}
	time.Sleep(maxAge + time.Second/10)
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 0 || b.Float != 0.9 {
		t.Error("b.json not invalidated correctly 2")
	}

	entries, err := cache.GetDir("/")
	if err != nil {
		panic(err)
	}
	if len(entries) != 2 {
		t.Error("/ not read correctly")
	}
	entries, err = cache.GetDir("/")
	if err != nil {
		panic(err)
	}
	if len(entries) != 2 {
		t.Error("/ not cached correctly")
	}
	err = os.Remove(filepath.Join(dir, "a.json"))
	if err != nil {
		panic(err)
	}
	entries, err = cache.GetDir("/")
	if err != nil {
		panic(err)
	}
	if len(entries) != 2 {
		t.Error("/ not cached correctly")
	}
	time.Sleep(maxAge + time.Second/10)
	entries, err = cache.GetDir("/")
	if err != nil {
		panic(err)
	}
	if len(entries) != 1 {
		t.Error("/ not invalidated correctly")
	}

	entries, err = cache.GetDir("/not-exist")
	if err == nil || !os.IsNotExist(err) {
		t.Error("GetDir doesn't return correct error")
	}
	if len(entries) != 0 {
		t.Error("non-existant dir returned entries")
	}
	entries, err = cache.GetDir("/not-exist")
	if err == nil || !os.IsNotExist(err) {
		t.Error("GetDir doesn't return correct error")
	}
	if len(entries) != 0 {
		t.Error("non-existant dir returned entries")
	}

	_, err = cache.GetFile("/c.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 1")
	}
	_, err = cache.GetFile("c.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 2")
	}
	_, err = cache.GetFile("c.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 3")
	}

	_, err = cache.GetFile("a.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 4")
	}
	_, err = cache.GetFile("a.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 4")
	}

	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 0 || b.Float != 0.9 {
		t.Error("b.json not cached correctly again")
	}
	err = os.Remove(filepath.Join(dir, "b.json"))
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 0 || b.Float != 0.9 {
		t.Error("b.json not cached correctly again 2")
	}
	b, err = cache.GetFile("b.json")
	if err != nil {
		panic(err)
	}
	if b.Hello != "" || b.Number != 0 || b.Float != 0.9 {
		t.Error("b.json not cached correctly again 3")
	}
	if err != nil {
		panic(err)
	}
	time.Sleep(maxAge + time.Second/10)
	b, err = cache.GetFile("b.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 4")
	}
	b, err = cache.GetFile("b.json")
	if err == nil || !os.IsNotExist(err) {
		t.Error("Get doesn't return correct error 5")
	}
}

func TestParseCache(t *testing.T) {
	maxAge := time.Second * 2
	dir, err := os.MkdirTemp("", "parsecache-test-normal-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	cache := NewConcurrentFsCache(os.DirFS(dir), JsonParser[testFileStructure], maxAge)
	cacheTests(t, cache, maxAge, dir)
}

func TestParseCacheConcurrent(t *testing.T) {
	maxAge := time.Second * 2
	dir, err := os.MkdirTemp("", "parsecache-test-concurrent-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	cache := NewFsCache(os.DirFS(dir), JsonParser[testFileStructure], maxAge)
	cacheTests(t, &cache, maxAge, dir)
}
