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

import difflib
import os.path
import sys
from typing import List, Tuple

from lib import *

NOTICE_HEADER = '''PolarDB-X operator contains and relies on various third-party components under other open source licenses.

The following sections summaries those components and their licenses.

## Third-party

+ [hsperfdata](https://github.com/xin053/hsperfdata), [MIT license](./third-party/hsperfdata/LICENSE).

## Vendors

'''


def generate_markdown_list(vendor_licenses: List[Tuple[str, str, str]], vendor_license_root: str) -> List[str]:
    vendor_license_markdown_item = []
    for vendor_name, guessed_license, license_file_relative_path in vendor_licenses:
        if len(guessed_license) > 0:
            vendor_license_markdown_item.append(
                '+ %s, [%s license](%s/%s).' % (
                    vendor_name, guessed_license, vendor_license_root, license_file_relative_path))
        else:
            vendor_license_markdown_item.append(
                '+ %s, [license](%s/%s).' % (
                    vendor_name, vendor_license_root, license_file_relative_path))
    return vendor_license_markdown_item


def normalize_license_content(s: str):
    # remove all white spaces and concat with one blank.
    return ' '.join(s.split()).lower()


MIT_LICENSE = normalize_license_content('''Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.''')

BSD2_LICENSE = normalize_license_content('''
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
''')

BSD3_LICENSE = normalize_license_content('''Redistribution and use in source and binary forms, with or without 
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
* Neither the name of AUTHOR nor the names of its contributors 
  may be used to endorse or promote products derived from this software 
  without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE 
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
''')

BSD3_NEW_CLAUSE = normalize_license_content('''endorse or promote products derived from this software 
  without specific prior written permission''')

ISC_LICENSE = normalize_license_content('''Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.''')


def guess_license(license_file: str) -> str:
    with open(license_file, 'r') as f:
        content = f.read()

    content = normalize_license_content(content)

    if 'mit license' in content:
        return 'MIT'
    elif 'apache license' in content:
        if 'version 2.0' in content:
            return 'Apache 2.0'
        else:
            return 'Apache'
    elif 'isc license' in content:
        return 'ISC'
    elif 'Mozilla Public License'.lower() in content:
        if 'version 2.0' in content:
            return 'MPL 2.0'
        else:
            return ''
    else:
        closest_matches = difflib.get_close_matches(content, [MIT_LICENSE, BSD2_LICENSE, BSD3_LICENSE, ISC_LICENSE], n=1, cutoff=0.3)
        if len(closest_matches) == 0:
            return ''
        else:
            m = closest_matches[0]
            if m == MIT_LICENSE:
                return 'MIT'
            elif m == BSD3_LICENSE or m == BSD2_LICENSE:
                if BSD3_NEW_CLAUSE in content:
                    # The new clause
                    return '3-Clause BSD'
                else:
                    return '2-Clause BSD'
            elif m == ISC_LICENSE:
                return 'ISC'
            else:
                raise RuntimeError('never here')


def walk_through_vendor_licenses(licenses_dir: str) -> List[Tuple[str, str, str]]:
    vendor_licenses = []
    for root, dirs, files in os.walk(licenses_dir):
        if 'LICENSE' in files:
            relative_path = os.path.relpath(root, licenses_dir)
            license_file_relative_path = os.path.join(relative_path, 'LICENSE')
            vendor_licenses.append(
                (relative_path, guess_license(os.path.join(root, 'LICENSE')), license_file_relative_path))
    return vendor_licenses


def generate_notice_file(build_env: BuildEnv):
    # Generate vendor licenses.
    vendor_licenses_dir = os.path.join(build_env.root_dir, 'LICENSES/vendor')
    vendor_licenses = walk_through_vendor_licenses(vendor_licenses_dir)
    vendor_notice_list = generate_markdown_list(vendor_licenses, './LICENSES/vendor')

    # Write to notice file.
    notice_file = os.path.join(build_env.root_dir, 'NOTICE.md')
    notice_content = NOTICE_HEADER + '\n\n'.join(vendor_notice_list) + '\n'
    with open(notice_file, 'w+') as f:
        f.write(notice_content)


def main():
    generate_notice_file(BASIC_BUILD_ENV)
    return 0


if __name__ == '__main__':
    sys.exit(main())
