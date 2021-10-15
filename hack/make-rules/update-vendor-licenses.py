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

import hashlib
import shutil
import tempfile
from typing import Dict

from lib import *


def walk_through_vendor_and_copy_licenses_to(vendor_path: str, licenses_path: str):
    for root, dirs, files in os.walk(vendor_path):
        # If LICENSE file found, we copy it into licenses path
        if 'LICENSE' in files:
            relative_path = os.path.relpath(root, vendor_path)
            target_path = os.path.join(licenses_path, relative_path)
            os.makedirs(target_path, exist_ok=True)
            shutil.copy2(os.path.join(root, 'LICENSE'), os.path.join(target_path, 'LICENSE'))


def build_licenses_map(licenses_path: str) -> Dict[str, str]:
    licenses = {}
    for root, dirs, files in os.walk(licenses_path):
        if 'LICENSE' in files:
            relative_path = os.path.relpath(root, licenses_path)
            with open(os.path.join(root, 'LICENSE'), 'rb') as f:
                md5sum = hashlib.md5(f.read()).hexdigest()
                licenses[relative_path] = md5sum
    return licenses


def compare_vendor_licenses_and_report(old_path: str, new_path: str):
    old_licenses = build_licenses_map(old_path)
    new_licenses = build_licenses_map(new_path)

    # Report new & changed licenses
    new_license_vendors = set([])
    changed_license_vendors = set([])
    for vendor, license_md5 in new_licenses.items():
        old_license_md5 = old_licenses.get(vendor, None)
        if not old_license_md5:
            new_license_vendors.add(vendor)
        elif old_license_md5 != license_md5:
            changed_license_vendors.add(vendor)

    if len(new_license_vendors) + len(changed_license_vendors) > 0:
        print('Found following vendor licenses changed: ')
        for v in new_license_vendors:
            print('  ' + v + ' (new)')
        for v in changed_license_vendors:
            print('  ' + v + ' (modified)')
    else:
        print('Licenses of vendors not changed.')


def update_vendor_licenses(build_env: BuildEnv):
    # Run 'go mod tidy' and 'go mod vendor' to update
    # current vendors.
    golang.run_mod_tidy(build_env)
    golang.run_mod_vendor(build_env)

    vendor_path = os.path.join(build_env.root_dir, 'vendor')
    tmp_licenses_output_path = tempfile.mkdtemp(prefix='lite-vendor-licenses')

    try:
        # Walk through each directory in vendor path.
        walk_through_vendor_and_copy_licenses_to(vendor_path, tmp_licenses_output_path)

        # If currently there's an old licenses path, compare it
        # with the newly generated and report diff.
        licenses_output_path = os.path.join(build_env.root_dir, 'LICENSES/vendor')

        if os.path.exists(licenses_output_path):
            compare_vendor_licenses_and_report(licenses_output_path, tmp_licenses_output_path)
    except Exception as e:
        # Remove the temp dir if any exception happens.
        shutil.rmtree(tmp_licenses_output_path, ignore_errors=True)
        raise e

    # Move the temp directory to licenses output path.
    shutil.rmtree(licenses_output_path, ignore_errors=True)
    shutil.move(tmp_licenses_output_path, licenses_output_path)


if __name__ == '__main__':
    print('Start updating vendor licenses...')

    update_vendor_licenses(BASIC_BUILD_ENV)

    print('Vendor licenses updated!')
