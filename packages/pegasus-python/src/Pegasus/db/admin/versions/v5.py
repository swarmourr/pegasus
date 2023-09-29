#!/usr/bin/env python
#
#  Copyright 2017-2021 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import logging
import warnings

from sqlalchemy import *
from sqlalchemy.exc import *

from Pegasus.db.admin.versions.base_version import BaseVersion
from Pegasus.db.schema import *

DB_VERSION = 5

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        "Fixes malfunction past migrations and clean the database"
        log.info("Updating to version %s" % DB_VERSION)

        # check and fix foreign key for master_workflowstate table
        with warnings.catch_warnings():
            table_names = self.db.get_bind().table_names()
            tables_to_inquiry = ["master_workflowstate", "master_workflow", "workflow"]
            if set(tables_to_inquiry).issubset(table_names):
                meta = MetaData()
                warnings.simplefilter("ignore")
                meta.reflect(bind=self.db.get_bind(), only=tables_to_inquiry)
                mw_id = meta.tables["master_workflowstate"].c.wf_id

                # PM-1015: invalid constraint
                if not mw_id.references(
                    meta.tables["master_workflow"].c.wf_id
                ) and mw_id.references(meta.tables["workflow"].c.wf_id):
                    log.info("Updating foreign key constraint.")
                    try:
                        self.db.execute("DROP INDEX UNIQUE_MASTER_WORKFLOWSTATE")
                    except Exception:
                        pass
                    if self.db.get_bind().driver == "mysqldb":
                        self.db.execute(
                            "RENAME TABLE master_workflowstate TO master_workflowstate_v4"
                        )
                    else:
                        self.db.execute(
                            "ALTER TABLE master_workflowstate RENAME TO master_workflowstate_v4"
                        )
                    Workflowstate.__table__.create(self.db.get_bind(), checkfirst=True)
                    self.db.execute(
                        "INSERT INTO master_workflowstate(wf_id, state, timestamp, restart_count, status) SELECT m4.wf_id, m4.state, m4.timestamp, m4.restart_count, m4.status FROM master_workflowstate_v4 m4 LEFT JOIN master_workflow mw WHERE m4.wf_id=mw.wf_id"
                    )
                    self.db.commit()
                    self._drop_table("master_workflowstate_v4")

        # clean the database from past migrations
        self._drop_table("rc_lfn_new")
        self._drop_table("rc_lfn_old")

    def downgrade(self, force=False):
        "No need for downgrade"

    def _drop_table(self, table_name):
        """
        Drop a table.
        :param table_name:
        :return:
        """
        try:
            self.db.execute("DROP TABLE %s" % table_name)
        except Exception:
            pass
