import datetime
import json
import os
import pwd

import pytest
from flask import g
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import Pegasus.db.schema as schema
from Pegasus.db.ensembles import Triggers, TriggerType
from Pegasus.service.ensembles.views import to_seconds


class NoAuthFlaskTestCase:
    @pytest.fixture(autouse=True)
    def init(self, emapp):
        self.user = pwd.getpwuid(os.getuid()).pw_name

    @staticmethod
    def pre_callable():

        # create in-mem db
        engine = create_engine("sqlite://")

        # create all tables in schema
        schema.Base.metadata.create_all(engine)

        g.session = sessionmaker(bind=engine)()

        # create an ensemble entry
        g.session.add(
            schema.Ensemble(
                name="test-ensemble",
                created=datetime.datetime.now(),
                updated=datetime.datetime.now(),
                state="ACTIVE",
                max_running=1,
                max_planning=1,
                username=pwd.getpwuid(os.getuid()).pw_name,
            )
        )

        g.session.commit()

        # create a trigger entry
        g.session.add(
            schema.Trigger(
                ensemble_id=1,
                name="test-trigger",
                state="READY",
                workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                _type=TriggerType.CRON.value,
            )
        )

        g.session.commit()


class TestTriggerRoutes(NoAuthFlaskTestCase):
    @pytest.mark.parametrize(
        "ensemble,expected",
        [
            (
                "test-ensemble",
                [
                    {
                        "args": None,
                        "ensemble_id": 1,
                        "id": 1,
                        "name": "test-trigger",
                        "state": "READY",
                        "type": "CRON",
                        "workflow": {"args": ["arg1"], "script": "/wf.py"},
                    }
                ],
            ),
            ("nonexistent-ensemble", []),
        ],
    )
    def test_list_triggers(self, mocker, emapp_client, ensemble, expected):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.get_context(
            "/ensembles/{}/triggers".format(ensemble), pre_callable=self.pre_callable,
        )

        assert rv.status_code == 200
        assert rv.json == expected

    def test_get_route(self, emapp_client):
        pass

    def test_route_create_cron_trigger(self, mocker, emapp_client, emapp):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        request_data = {
            "trigger": "test-trigger2",
            "workflow_script": "/workflow.py",
            "workflow_args": json.dumps([]),
            "interval": "10s",
            "timeout": "20s",
            "type": TriggerType.CRON.value,
        }

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/triggers/cron",
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == 201
        assert rv.json == {"message": "created"}
        assert rv.location == "/ensembles/test-ensemble/triggers/test-trigger2"

        db_entry = g.session.query(schema.Trigger).filter_by(_id=2).one()
        assert Triggers.get_object(db_entry) == {
            "id": 2,
            "ensemble_id": 1,
            "name": "test-trigger2",
            "state": "READY",
            "workflow": {"script": "/workflow.py", "args": []},
            "args": {"interval": 10, "timeout": 20,},
            "type": "CRON",
        }

    @pytest.mark.parametrize(
        "ensemble,request_data,exception_msg,status_code",
        [
            (
                "nonexistent-ensemble",
                {
                    "trigger": "test-trigger2",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "No such ensemble: nonexistent-ensemble",
                404,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "trigger name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_script name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_script must be given as an absolute path",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "badjson",
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps({"a": 1}),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "0s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
                "interval must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "0s",
                    "type": TriggerType.CRON.value,
                },
                "timeout must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
        ],
    )
    def test_route_create_cron_trigger_invalid_request(
        self, mocker, emapp_client, ensemble, request_data, exception_msg, status_code
    ):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            "/ensembles/{}/triggers/cron".format(ensemble),
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == status_code
        assert exception_msg in rv.json["message"]

    def test_route_create_file_pattern_trigger(self, mocker, emapp_client, emapp):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        request_data = {
            "trigger": "test-trigger2",
            "workflow_script": "/workflow.py",
            "workflow_args": json.dumps(["arg1"]),
            "interval": "10s",
            "timeout": "20s",
            "file_patterns": json.dumps(["/*.txt", "/*.csv"]),
            "type": TriggerType.FILE_PATTERN.value,
        }

        rv = emapp_client.post_context(
            "/ensembles/test-ensemble/triggers/file_pattern",
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == 201
        assert rv.json == {"message": "created"}
        assert rv.location == "/ensembles/test-ensemble/triggers/test-trigger2"

        db_entry = g.session.query(schema.Trigger).filter_by(_id=2).one()
        assert Triggers.get_object(db_entry) == {
            "id": 2,
            "ensemble_id": 1,
            "name": "test-trigger2",
            "state": "READY",
            "workflow": {"script": "/workflow.py", "args": ["arg1"]},
            "args": {
                "interval": 10,
                "timeout": 20,
                "file_patterns": ["/*.txt", "/*.csv"],
            },
            "type": "FILE_PATTERN",
        }

    @pytest.mark.parametrize(
        "ensemble,request_data,exception_msg,status_code",
        [
            (
                "nonexistent-ensemble",
                {
                    "trigger": "test-trigger2",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "No such ensemble: nonexistent-ensemble",
                404,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "trigger name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_script name must be a non-empty string",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_script must be given as an absolute path",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "badjson",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps({"a": 1}),
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "workflow_args must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "0s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "interval must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "0s",
                    "file_patterns": json.dumps(["/*.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "timeout must be given as `<int> <s|m|h|d>` and be greater than 0 seconds",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": "abc123",
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "file_patterns must be given as a list serialized to json",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps([]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "file_patterns must contain at least one file pattern",
                500,
            ),
            (
                "test-ensemble",
                {
                    "trigger": "test-trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": "[]",
                    "interval": "10s",
                    "timeout": "20s",
                    "file_patterns": json.dumps(["not_abs_path.txt"]),
                    "type": TriggerType.FILE_PATTERN.value,
                },
                "each file pattern must be given as an absolute path",
                500,
            ),
        ],
    )
    def test_route_create_file_pattern_trigger_invalid_request(
        self, mocker, emapp_client, ensemble, request_data, exception_msg, status_code
    ):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.post_context(
            "/ensembles/{}/triggers/file_pattern".format(ensemble),
            pre_callable=self.pre_callable,
            data=request_data,
        )

        assert rv.status_code == status_code
        assert exception_msg in rv.json["message"]

    def test_route_delete_trigger(self, mocker, emapp_client):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            "/ensembles/test-ensemble/triggers/test-trigger",
            pre_callable=self.pre_callable,
        )

        # check status code and msg
        assert rv.status_code == 202
        assert (
            rv.json["message"]
            == "ensemble: test-ensemble, trigger: test-trigger marked for deletion"
        )

        # check trigger state
        trigger_to_delete = g.session.query(schema.Trigger).filter_by(_id=1).one()
        assert trigger_to_delete.state == "STOPPED"

    @pytest.mark.parametrize(
        "ensemble,trigger,exception_msg,status_code",
        [
            (
                "nonexistent-ensemble",
                "test-trigger",
                "No such ensemble: nonexistent-ensemble",
                404,
            ),
            (
                "test-ensemble",
                "nonexistent-trigger",
                "No such trigger: nonexistent-trigger assigned to ensemble",
                404,
            ),
        ],
    )
    def test_route_delete_nonexistent_trigger(
        self, mocker, emapp_client, ensemble, trigger, exception_msg, status_code
    ):
        # patch so that the in-mem db defined in pre_callable can be used
        # and also to avoid a db version check done in Pegasus.db.connection.connect
        mocker.patch("Pegasus.db.connection.connect")

        rv = emapp_client.delete_context(
            "/ensembles/{}/triggers/{}".format(ensemble, trigger),
            pre_callable=self.pre_callable,
        )

        assert rv.status_code == status_code
        assert exception_msg in rv.json["message"]


@pytest.mark.parametrize(
    "input,expected",
    [
        ("1s", 1),
        ("1 s", 1),
        ("10 m", 60 * 10),
        ("10h", 60 * 60 * 10),
        ("1d", 60 * 60 * 24),
        ("2 d", 60 * 60 * 24 * 2),
    ],
)
def test_to_seconds(input, expected):
    assert to_seconds(input) == expected


@pytest.mark.parametrize(
    "bad_input,exception_msg",
    [
        ("", "interval must be given as '<int> <s|m|h|d>'"),
        ("1", "interval must be given as '<int> <s|m|h|d>'"),
        ("1s h", "interval must be given as '<int> <s|m|h|d>'"),
        ("1x", "interval must be given as '<int> <s|m|h|d>'"),
        ("0s", "interval must be greater than 0 seconds"),
        ("0d", "interval must be greater than 0 seconds"),
    ],
)
def test_to_seconds_invalid_input(bad_input, exception_msg):
    with pytest.raises(ValueError) as e:
        to_seconds(bad_input)

    assert exception_msg in str(e)
