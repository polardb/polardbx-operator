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

import logging
import os

import click
import jsons

from core import Context, Manager
from audit_controller import AuditController

logging.basicConfig()
logging.root.setLevel(logging.NOTSET)
logging.basicConfig(level=logging.NOTSET, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _current_path():
    # Return the symlink.
    return os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'current'))


def _write_convenient_access_script(context: Context):
    # Write myc command
    if not os.path.exists('/usr/bin/myc'):
        with open('/usr/bin/myc', 'w') as f:
            f.write('''#!/usr/bin/env bash
        mysql -h127.1 -P%d -uroot -Ac "$@"''' % context.port_access())
        os.chmod('/usr/bin/myc', 0o755)

    # Write xsl profile.d
    if not os.path.exists('/etc/profile.d/xsl.sh'):
        current_path = _current_path()
        with open('/etc/profile.d/xsl.sh', 'w') as f:
            f.write('''export PATH=%s:%s:$PATH            
if command -v xsl &> /dev/null; then
    if [ ! -f /etc/profile.d/xsl.comp ]; then
        _XSL_COMPLETE=source_bash xsl > /etc/profile.d/xsl.comp
    fi

    source /etc/profile.d/xsl.comp
fi''' % (current_path + '/venv/bin', current_path))


@click.command()
@click.option('--initialize', is_flag=True)
@click.option('--restore-prepare', is_flag=True)
@click.option('--debug', is_flag=True)
@click.option('--ignore-indicates', is_flag=True)
@click.option('--force', is_flag=True)
@click.option('--cluster-start-index', type=str)
def _start(initialize, restore_prepare, debug, ignore_indicates, cluster_start_index, force):
    # Construct a new context.
    context = Context()

    if restore_prepare:
        context.set_restore_prepare(True)

    mgr = Manager(context)

    if debug:
        logging.info('Context: %s' % jsons.dumps(context))

    if debug:
        logging.info('Indicates: %s' % jsons.dumps(context.get_controller_indicates()))

    mgr.wait_for_unblock()

    # Go bootstrap or just initialize.
    engine = mgr.engine()
    engine.try_flush_meta_when_start()

    # Handle indicates including pod block.
    if not ignore_indicates:
        mgr.handle_indicates()

    _write_convenient_access_script(context)

    if cluster_start_index is not None:
        engine.set_cluster_start_index(cluster_start_index)

    if not force:
        engine.wait_for_enable()

    if not engine.is_initialized():
        logging.info('Begin to initialize...')
        engine.initialize()
        logging.info('Initialized!')

    if restore_prepare:
        engine.set_restore_prepare(True)
        engine.clean_data_log()

    # create a process to enable audit log
    audit_controller = AuditController()
    audit_controller.start_process()

    # Bootstrap when not only initialize.
    if not initialize:
        logging.info('Bootstrapping engine %s ...' % context.engine_name())
        engine.update_config()
        # mv log file if log_data_separation config changes
        engine.try_move_log_file()
        # try to flush metadata
        engine.try_handle_indicate()
        engine.bootstrap()


if __name__ == '__main__':
    _start()
