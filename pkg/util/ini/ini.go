/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ini

import (
	"bytes"
	"io"
	"reflect"
	"strings"

	"gopkg.in/ini.v1"
)

// IsIniKeyBooleanValue returns true when key is a bool value, aka. without value.
func IsIniKeyBooleanValue(k *ini.Key) bool {
	if k == nil {
		return false
	}
	return reflect.Indirect(reflect.ValueOf(k)).FieldByName("isBooleanType").Bool()
}

// CanonicalizeMyCnfFile replaces every dash-linked variable with underscore_linked.
// It will silently overwrite the existing duplicate if allowDuplicate is true.
func CanonicalizeMyCnfFile(f *ini.File, allowDuplicate bool) error {
	for _, sec := range f.Sections() {
		for _, key := range sec.Keys() {
			canonicalName := strings.ReplaceAll(key.Name(), "-", "_")

			// Keep the last "-", which has special meaning.
			if canonicalName[len(canonicalName)-1] == '-' {
				canonicalName = canonicalName[:len(canonicalName)-1] + "-"
			}

			// Delete the old key and insert a new key if name is changed.
			if canonicalName != key.Name() {
				sec.DeleteKey(key.Name())
				if IsIniKeyBooleanValue(key) {
					_, err := sec.NewBooleanKey(canonicalName)
					if err != nil {
						return err
					}
				} else {
					_, err := sec.NewKey(canonicalName, key.Value())
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func MoveDefaultSectionInto(f *ini.File, section string) error {
	if len(section) == 0 || section == ini.DefaultSection {
		return nil
	}

	src := f.Section(ini.DefaultSection)
	dst := f.Section(section)

	for _, key := range src.Keys() {
		if IsIniKeyBooleanValue(key) {
			dstKey, err := dst.NewBooleanKey(key.Name())
			if err != nil {
				return err
			}
			dstKey.Comment = key.Comment
		} else {
			dstKey, err := dst.NewKey(key.Name(), key.Value())
			if err != nil {
				return err
			}
			dstKey.Comment = key.Comment
		}
	}

	f.DeleteSection(ini.DefaultSection)

	return nil
}

func parseMyCnfFile(r io.Reader) (*ini.File, error) {
	f, err := ini.LoadSources(ini.LoadOptions{
		AllowBooleanKeys:           true,
		AllowPythonMultilineValues: true,
		SpaceBeforeInlineComment:   true,
		PreserveSurroundedQuote:    true,
		IgnoreInlineComment:        true,
	}, r)

	if err != nil {
		return nil, err
	}

	if err := CanonicalizeMyCnfFile(f, false); err != nil {
		return nil, err
	}

	return f, nil
}

func ParseMyCnfTemplateFile(r io.Reader) (*ini.File, error) {
	f, err := parseMyCnfFile(r)
	if err != nil {
		return nil, err
	}

	f.DeleteSection(ini.DefaultSection)

	return f, nil
}

func ParseMyCnfOverlayFile(r io.Reader) (*ini.File, error) {
	f, err := parseMyCnfFile(r)
	if err != nil {
		return nil, err
	}

	// All we care about is the mysqld section.
	err = MoveDefaultSectionInto(f, "mysqld")
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Patch patches the origin ini file with the given patch. Key with different values
// will be overwritten and new keys will be inserted. Patch extends the update-insert
// only behaviour with another delete semantic: patch a bool key with "-" suffix will
// try to delete the key without "-" in the original file. For example,
//
// origin:
// 	 [mysqld]
// 	 core-file
//
// patch:
// 	 [mysqld]
// 	 core-file-
//
// After the Patch, the 'core-file' in origin will be removed.
func Patch(origin, patch *ini.File) (*ini.File, error) {
	if patch == nil {
		return origin, nil
	}

	for _, section := range patch.Sections() {
		sectionName := section.Name()

		dst := origin.Section(sectionName)
		for _, key := range section.Keys() {
			if IsIniKeyBooleanValue(key) {
				// Handle the special case that key ends with '-',
				// it means delete the original key in template
				if strings.HasSuffix(key.Name(), "-") {
					keyName := key.Name()[:len(key.Name())-1]
					if len(keyName) != 0 && dst.HasKey(keyName) {
						dst.DeleteKey(keyName)
					}
					continue
				}

				if dst.HasKey(key.Name()) {
					continue
				}
				dstKey, err := dst.NewBooleanKey(key.Name())
				if err != nil {
					return nil, err
				}
				dstKey.Comment = key.Comment
			} else {
				dstKey := dst.Key(key.Name())
				dstKey.SetValue(key.Value())
				dstKey.Comment = key.Comment
			}
		}
	}

	return origin, nil
}

func DiffMyCnfFile(old, new *ini.File, section string) map[string]string {
	newSec := new.Section(section)
	oldSec := old.Section(section)

	diff := make(map[string]string)
	for _, k := range newSec.Keys() {
		// Also ignore boolean key
		if IsIniKeyBooleanValue(k) {
			continue
		}

		if !oldSec.HasKey(k.Name()) {
			diff[k.Name()] = k.Value()
		} else {
			oldK := oldSec.Key(k.Name())
			if k.Value() != oldK.Value() {
				diff[k.Name()] = k.Value()
			}
		}
	}

	return diff
}

func ToString(f *ini.File) string {
	if f == nil {
		return ""
	}
	w := &bytes.Buffer{}
	if _, err := f.WriteTo(w); err != nil {
		return ""
	}

	return w.String()
}
