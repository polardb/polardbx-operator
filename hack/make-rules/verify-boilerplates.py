#!/usr/bin/env python3

# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import datetime
import os.path
import re
import sys
from typing import Callable, Union

from lib import *

BOILERPLATE_TEMPLATE_ROOT = os.path.join(BASIC_BUILD_ENV.root_dir, 'hack/boilerplates')
SOURCE_ROOT = BASIC_BUILD_ENV.root_dir

language_associate_file_extensions = {
    'python': 'py',
    'go': 'go',
    'bash': 'sh',
}


def filter_generated_go_source_files(files: [str]) -> [str]:
    return [f for f in files if
            not f.startswith('zz_generated.') and not f.endswith('.pb.go') and not f.endswith('.generated.go')]


language_associate_file_filter = {
    'go': filter_generated_go_source_files,
}


def preprocess_common_file_content(s: str) -> str:
    # Strip white characters from header
    return s.lstrip()


SHEBANG_REGEX = re.compile('^#![^\\r\\n]+[\\r\\n]+(.*)', re.MULTILINE | re.DOTALL)


def preprocess_executable_script_file_content(s: str) -> str:
    # Remove shebang
    m = SHEBANG_REGEX.match(s)
    if m:
        return preprocess_common_file_content(m.group(1))
    else:
        return s


language_associate_preprocessors = {
    'python': preprocess_executable_script_file_content,
    'bash': preprocess_executable_script_file_content,
    'go': preprocess_common_file_content,
}

LICENSE_YEAR_REGEX_STR = '((?:\\d+|\\d+\\s*-\\s*\\d+)(?:,\\d+|\\d+\\s*-\\s*\\d+)*)'

def load_boilerplate_regex(language) -> re.Pattern:
    extension = language_associate_file_extensions[language]
    boilerplate_template_file = os.path.join(BOILERPLATE_TEMPLATE_ROOT, 'boilerplate.' + extension + '.txt')
    if not os.path.exists(boilerplate_template_file):
        raise RuntimeError('boilerplate template file not found for ' + language)

    with open(boilerplate_template_file) as f:
        template_str = f.read()

    # Replace YEAR with date regex.
    lines = template_str.splitlines()
    escaped_lines = [re.escape(w.rstrip()) for w in lines]
    escape_str = '\\s*\\n'.join(escaped_lines)
    regex_str = '^' + escape_str.replace('YEAR', LICENSE_YEAR_REGEX_STR) + '.*$'
    return re.compile(regex_str, re.MULTILINE | re.DOTALL)


def read_file_content_and_preprocess(path: str, process: Callable[[str], str]) -> str:
    with open(path) as f:
        return process(f.read())


def current_year() -> int:
    return datetime.datetime.now().year


CURRENT_YEAR = current_year()


def extract_license_year_range(year_str: str) -> list[Union[tuple[int, int], int]]:
    year_ranges = []
    for s in year_str.split(sep=','):
        if '-' in s:
            r = s.split('-')
            year_ranges.append((int(r[0].strip()), int(r[1].strip())))
        else:
            year_ranges.append((int(s.strip()),))

    sorted(year_ranges, key=lambda x: x[0])
    return year_ranges


def is_year_ranges_overlaps(year_ranges: list[Union[tuple[int, int], int]]) -> (bool, int or None):
    last = None  # None or Union[tuple[int, int], int]
    for r in year_ranges:
        if last:
            if r[0] <= last[-1]:
                return True, r[0]
        last = r
    return False, None


def walk_through_project_and_check_boilerplates(language: str, *, exclude_dirs: [str] or None = None,
                                                include_dirs: [str] or None = None,
                                                start_year: int or None = None):
    if exclude_dirs:
        exclude_dirs = [d.rstrip('/') for d in exclude_dirs]
    if include_dirs:
        include_dirs = [d.rstrip('/') for d in include_dirs]

    def is_path_in(p: str, paths: [str]) -> bool:
        if not paths:
            return False
        for d in paths:

            if p.startswith(d + '/'):
                return True
        return False

    extension = language_associate_file_extensions[language]
    preprocessor = language_associate_preprocessors[language]
    boilerplate_regex = load_boilerplate_regex(language)
    file_filter = language_associate_file_filter.get(language)

    source_files_not_match_msg = []
    for root, dirs, files in os.walk(SOURCE_ROOT):
        relative_path = os.path.relpath(root, SOURCE_ROOT)
        if is_path_in(relative_path, exclude_dirs) or \
                (include_dirs and not is_path_in(relative_path, include_dirs)):
            continue

        source_files = [f for f in files if str(f).endswith('.' + extension)]
        if file_filter:
            source_files = file_filter(source_files)

        for source_file in source_files:
            content = read_file_content_and_preprocess(os.path.join(root, source_file), preprocessor)
            match = boilerplate_regex.match(content)
            if not match:
                source_files_not_match_msg.append(os.path.join(relative_path, source_file) + ', misses license header')
            else:
                year_range = extract_license_year_range(match.group(1))
                # should not overlaps
                overlap, overlap_year = is_year_ranges_overlaps(year_range)
                if overlap:
                    source_files_not_match_msg.append(
                        os.path.join(relative_path, source_file) +
                        ', invalid year range, year overlaps ' + str(overlap_year))
                    continue

                s_year, e_year = year_range[0][0], year_range[-1][0]
                if start_year and s_year < start_year:
                    source_files_not_match_msg.append(
                        os.path.join(relative_path, source_file) +
                        ', invalid year range, start year %d smaller than required %d' %
                        (s_year, start_year))
                    continue
                if e_year > CURRENT_YEAR:
                    source_files_not_match_msg.append(
                        os.path.join(relative_path, source_file) +
                        ', invalid year range, end year %d exceeds current year %d' %
                        (e_year, CURRENT_YEAR))

    sorted(source_files_not_match_msg)

    # Report source files not match
    if len(source_files_not_match_msg) > 0:
        print('\n'.join(source_files_not_match_msg))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--exclude-dirs', dest='exclude_dirs', default='', type=str,
                        help='Exclude directories, separated by comma.')
    parser.add_argument('--include-dirs', dest='include_dirs', default='', type=str,
                        help='Include directories, separated by comma.')
    parser.add_argument('--language', dest='language', default='', type=str,
                        help='Language of source files.')
    parser.add_argument('--start-year', dest='start_year', default=CURRENT_YEAR, type=int,
                        help='Start year. If license with begin year lower than this, it will complain.')
    args = parser.parse_args()

    include_dirs = [s.strip() for s in args.include_dirs.split(',')] \
        if args.include_dirs and len(args.include_dirs) > 0 else None
    exclude_dirs = [s.strip() for s in args.exclude_dirs.split(',')] \
        if args.exclude_dirs and len(args.exclude_dirs) > 0 else None

    walk_through_project_and_check_boilerplates(
        args.language,
        exclude_dirs=exclude_dirs,
        include_dirs=include_dirs,
        start_year=args.start_year
    )


if __name__ == '__main__':
    sys.exit(main())
