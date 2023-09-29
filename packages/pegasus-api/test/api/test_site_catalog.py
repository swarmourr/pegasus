import getpass
import json
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
import yaml
from jsonschema import validate

from Pegasus.api.errors import DuplicateError
from Pegasus.api.site_catalog import (
    OS,
    PEGASUS_VERSION,
    Arch,
    Directory,
    FileServer,
    Grid,
    Operation,
    Scheduler,
    Site,
    SiteCatalog,
    SupportedJobs,
)
from Pegasus.api.writable import _CustomEncoder


class TestFileServer:
    def test_valid_file_server(self):
        assert FileServer("url", Operation.PUT)

    def test_invlaid_file_server(self):
        with pytest.raises(TypeError) as e:
            FileServer("url", "put")

        assert "invalid operation_type: put" in str(e)

    def test_tojson_with_profiles(self, convert_yaml_schemas_to_json, load_schema):
        file_server = FileServer("url", Operation.PUT).add_env(SOME_ENV="1")

        result = json.loads(json.dumps(file_server, cls=_CustomEncoder))

        expected = {
            "url": "url",
            "operation": "put",
            "profiles": {"env": {"SOME_ENV": "1"}},
        }

        file_server_schema = load_schema("sc-5.0.json")["$defs"]["fileServer"]
        validate(instance=result, schema=file_server_schema)

        assert result == expected


class TestDirectory:
    def test_valid_directory(self):
        assert Directory(Directory.LOCAL_SCRATCH, "/path")
        assert Directory(
            Directory.SHARED_SCRATCH, Path("/abs/path"), shared_file_system=True
        )

    def test_invalid_directory_type(self):
        with pytest.raises(TypeError) as e:
            Directory("invalid type", "/path")

        assert "invalid directory_type: invalid type" in str(e)

    def test_invalid_directory_path(self):
        with pytest.raises(ValueError) as e:
            Directory(Directory.LOCAL_SCRATCH, Path("dir"))

        assert "invalid path: {}".format(Path("dir")) in str(e)

    def test_add_valid_file_server(self):
        d = Directory(Directory.LOCAL_SCRATCH, "/path")
        assert d.add_file_servers(FileServer("url", Operation.PUT))

    def test_add_invalid_file_server(self):
        with pytest.raises(TypeError) as e:
            d = Directory(Directory.LOCAL_SCRATCH, "/path")
            d.add_file_servers(123)

            assert "invalid file_server: 123" in str(e)

    def test_chaining(self):
        a = Directory(Directory.LOCAL_SCRATCH, "/path")
        b = a.add_file_servers(FileServer("url", Operation.PUT)).add_file_servers(
            FileServer("url", Operation.GET)
        )

        assert id(a) == id(b)

    def test_tojson(self):
        directory = Directory(Directory.LOCAL_SCRATCH, "/path", True).add_file_servers(
            FileServer("url", Operation.PUT)
        )

        result = json.loads(json.dumps(directory, cls=_CustomEncoder))

        expected = {
            "type": "localScratch",
            "path": "/path",
            "sharedFileSystem": True,
            "fileServers": [{"url": "url", "operation": "put"}],
        }

        assert result == expected


class TestGrid:
    def test_valid_grid(self):
        assert Grid(
            Grid.GT5,
            "smarty.isi.edu/jobmanager-pbs",
            Scheduler.PBS,
            SupportedJobs.AUXILLARY,
        )

    @pytest.mark.parametrize(
        "grid_type, contact, scheduler_type, job_type, invalid_var",
        [
            (
                "badgridtype",
                "contact",
                Scheduler.SLURM,
                SupportedJobs.AUXILLARY,
                "grid_type",
            ),
            (
                Grid.BATCH,
                "contact",
                "badschedulertype",
                SupportedJobs.AUXILLARY,
                "scheduler_type",
            ),
            (Grid.BATCH, "contact", Scheduler.SLURM, "badjobtype", "job_type"),
        ],
    )
    def test_invalid_grid(
        self, grid_type, contact, scheduler_type, job_type, invalid_var
    ):
        with pytest.raises(TypeError) as e:
            Grid(
                grid_type, contact, scheduler_type, job_type,
            )

        assert "invalid {invalid_var}: {value}".format(
            invalid_var=invalid_var, value=locals()[invalid_var]
        ) in str(e)

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        grid = Grid(
            Grid.GT5,
            "smarty.isi.edu/jobmanager-pbs",
            Scheduler.PBS,
            SupportedJobs.AUXILLARY,
        )

        result = json.loads(json.dumps(grid, cls=_CustomEncoder))

        expected = {
            "type": "gt5",
            "contact": "smarty.isi.edu/jobmanager-pbs",
            "scheduler": "pbs",
            "jobtype": "auxillary",
        }

        grid_schema = load_schema("sc-5.0.json")["$defs"]["grid"]
        validate(instance=result, schema=grid_schema)

        assert result == expected


class TestSite:
    @pytest.mark.parametrize(
        "arch", [(Arch.AARCH64), (Arch.X86_64),],
    )
    def test_valid_site(self, arch):
        assert Site(
            "site",
            arch=arch,
            os_type=OS.LINUX,
            os_release="release",
            os_version="1.1.1",
        )

    @pytest.mark.parametrize(
        "name, arch, os_type, invalid_var",
        [
            ("site", "badarch", OS.LINUX, "arch"),
            ("site", Arch.X86_64, "badostype", "os_type"),
            ("site", Arch.AARCH64, "randomos", "os_type"),
        ],
    )
    def test_invalid_site(self, name, arch, os_type, invalid_var):
        with pytest.raises(TypeError) as e:
            Site(name, arch=arch, os_type=os_type)

        assert "invalid {invalid_var}: {value}".format(
            invalid_var=invalid_var, value=locals()[invalid_var]
        ) in str(e)

    def test_add_valid_directory(self):
        site = Site("s")
        site.add_directories(Directory(Directory.LOCAL_SCRATCH, "/path"))
        site.add_directories(Directory(Directory.LOCAL_STORAGE, "/path"))

        assert len(site.directories) == 2

    def test_add_invalid_directory(self):
        with pytest.raises(TypeError) as e:
            site = Site("s")
            site.add_directories("baddirectory")

        assert "invalid directory: baddirectory" in str(e)

    def test_add_valid_grid(self):
        site = Site("s")
        site.add_grids(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.AUXILLARY,
            )
        )
        site.add_grids(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.COMPUTE,
            )
        )

        assert len(site.grids) == 2

    def test_add_invalid_grid(self):
        with pytest.raises(TypeError) as e:
            site = Site("s")
            site.add_grids("badgrid")

        assert "invalid grid: badgrid" in str(e)

    def test_chaining(self):
        site = Site("s")
        a = site.add_directories(Directory(Directory.LOCAL_SCRATCH, "/path"))
        b = site.add_grids(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.AUXILLARY,
            )
        )

        assert id(a) == id(b)

    def test_tojson_with_profiles(self):
        site = Site(
            "s",
            arch=Arch.AARCH64,
            os_type=OS.LINUX,
            os_release="release",
            os_version="1.2.3",
        )
        site.add_directories(
            Directory(Directory.LOCAL_SCRATCH, "/path").add_file_servers(
                FileServer("url", Operation.GET)
            )
        )
        site.add_grids(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.AUXILLARY,
            )
        )
        site.add_env(JAVA_HOME="/usr/bin/java")

        result = json.loads(json.dumps(site, cls=_CustomEncoder))

        expected = {
            "name": "s",
            "arch": "aarch64",
            "os.type": "linux",
            "os.release": "release",
            "os.version": "1.2.3",
            "directories": [
                {
                    "type": "localScratch",
                    "path": "/path",
                    "sharedFileSystem": False,
                    "fileServers": [{"url": "url", "operation": "get"}],
                }
            ],
            "grids": [
                {
                    "type": "gt5",
                    "contact": "smarty.isi.edu/jobmanager-pbs",
                    "scheduler": "pbs",
                    "jobtype": "auxillary",
                }
            ],
            "profiles": {"env": {"JAVA_HOME": "/usr/bin/java"}},
        }

        assert result == expected


@pytest.fixture
def expected_json():
    expected = {
        "pegasus": PEGASUS_VERSION,
        "sites": [
            {
                "name": "local",
                "arch": "x86_64",
                "os.type": "linux",
                "directories": [
                    {
                        "type": "sharedScratch",
                        "path": "/tmp/workflows/scratch",
                        "sharedFileSystem": False,
                        "fileServers": [
                            {
                                "url": "file:///tmp/workflows/scratch",
                                "operation": "all",
                            }
                        ],
                    },
                    {
                        "type": "localStorage",
                        "path": "/tmp/workflows/outputs",
                        "sharedFileSystem": False,
                        "fileServers": [
                            {
                                "url": "file:///tmp/workflows/outputs",
                                "operation": "all",
                            }
                        ],
                    },
                ],
            },
            {
                "name": "condor_pool",
                "arch": "x86_64",
                "os.type": "linux",
                "directories": [
                    {
                        "type": "sharedScratch",
                        "path": "/lustre",
                        "sharedFileSystem": False,
                        "fileServers": [
                            {
                                "url": "gsiftp://smarty.isi.edu/lustre",
                                "operation": "all",
                            }
                        ],
                    }
                ],
                "grids": [
                    {
                        "type": "gt5",
                        "contact": "smarty.isi.edu/jobmanager-pbs",
                        "scheduler": "pbs",
                        "jobtype": "auxillary",
                    },
                    {
                        "type": "gt5",
                        "contact": "smarty.isi.edu/jobmanager-pbs",
                        "scheduler": "pbs",
                        "jobtype": "compute",
                    },
                ],
                "profiles": {"env": {"JAVA_HOME": "/usr/bin/java"}},
            },
            {
                "name": "staging_site",
                "arch": "x86_64",
                "os.type": "linux",
                "directories": [
                    {
                        "type": "sharedScratch",
                        "path": "/data",
                        "sharedFileSystem": False,
                        "fileServers": [
                            {"url": "scp://obelix.isi.edu/data", "operation": "put",},
                            {"url": "http://obelix.isi.edu/data", "operation": "get",},
                        ],
                    }
                ],
            },
        ],
    }

    expected["sites"].sort(key=lambda s: s["name"])
    for i in range(len(expected["sites"])):
        expected["sites"][i]["directories"].sort(key=lambda d: d["path"])

        for j in range(len(expected["sites"][i]["directories"])):
            expected["sites"][i]["directories"][j]["fileServers"].sort(
                key=lambda fs: fs["url"]
            )

        if "grids" in expected["sites"][i]:
            expected["sites"][i]["grids"].sort(key=lambda g: g["jobtype"])

    return expected


class TestSiteCatalog:
    def test_add_valid_site(self):
        sc = SiteCatalog()
        assert sc.add_sites(Site("local"))

    def test_add_invalid_site(self):
        with pytest.raises(TypeError) as e:
            sc = SiteCatalog()
            sc.add_sites("badsite")

        assert "invalid site: badsite" in str(e)

    def test_add_duplicate_site(self):
        sc = SiteCatalog()
        sc.add_sites(Site("local"))
        with pytest.raises(DuplicateError):
            sc.add_sites(Site("local"))

    def test_chaining(self):
        sc = SiteCatalog()
        a = sc.add_sites(Site("local"))
        b = sc.add_sites(Site("condor_pool"))

        assert id(a) == id(b)

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema, expected_json):
        sc = SiteCatalog().add_sites(
            Site("local", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
                Directory(
                    Directory.SHARED_SCRATCH, "/tmp/workflows/scratch"
                ).add_file_servers(
                    FileServer("file:///tmp/workflows/scratch", Operation.ALL)
                ),
                Directory(
                    Directory.LOCAL_STORAGE, "/tmp/workflows/outputs"
                ).add_file_servers(
                    FileServer("file:///tmp/workflows/outputs", Operation.ALL)
                ),
            ),
            Site("condor_pool", arch=Arch.X86_64, os_type=OS.LINUX)
            .add_directories(
                Directory(Directory.SHARED_SCRATCH, "/lustre").add_file_servers(
                    FileServer("gsiftp://smarty.isi.edu/lustre", Operation.ALL)
                )
            )
            .add_grids(
                Grid(
                    Grid.GT5,
                    "smarty.isi.edu/jobmanager-pbs",
                    Scheduler.PBS,
                    job_type=SupportedJobs.AUXILLARY,
                ),
                Grid(
                    Grid.GT5,
                    "smarty.isi.edu/jobmanager-pbs",
                    Scheduler.PBS,
                    job_type=SupportedJobs.COMPUTE,
                ),
            )
            .add_env(JAVA_HOME="/usr/bin/java"),
            Site("staging_site", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
                Directory(Directory.SHARED_SCRATCH, "/data")
                .add_file_servers(
                    FileServer("scp://obelix.isi.edu/data", Operation.PUT)
                )
                .add_file_servers(
                    FileServer("http://obelix.isi.edu/data", Operation.GET)
                )
            ),
        )

        result = json.loads(json.dumps(sc, cls=_CustomEncoder))

        sc_schema = load_schema("sc-5.0.json")
        validate(instance=result, schema=sc_schema)

        result["sites"].sort(key=lambda s: s["name"])
        for i in range(len(result["sites"])):
            result["sites"][i]["directories"].sort(key=lambda d: d["path"])

            for j in range(len(result["sites"][i]["directories"])):
                result["sites"][i]["directories"][j]["fileServers"].sort(
                    key=lambda fs: fs["url"]
                )

            if "grids" in result["sites"][i]:
                result["sites"][i]["grids"].sort(key=lambda g: g["jobtype"])

        assert result == expected_json

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write(self, expected_json, _format, loader):
        sc = (
            SiteCatalog()
            .add_sites(
                Site("local", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
                    Directory(
                        Directory.SHARED_SCRATCH, "/tmp/workflows/scratch"
                    ).add_file_servers(
                        FileServer("file:///tmp/workflows/scratch", Operation.ALL)
                    ),
                    Directory(
                        Directory.LOCAL_STORAGE, "/tmp/workflows/outputs"
                    ).add_file_servers(
                        FileServer("file:///tmp/workflows/outputs", Operation.ALL)
                    ),
                )
            )
            .add_sites(
                Site("condor_pool", arch=Arch.X86_64, os_type=OS.LINUX)
                .add_directories(
                    Directory(Directory.SHARED_SCRATCH, "/lustre").add_file_servers(
                        FileServer("gsiftp://smarty.isi.edu/lustre", Operation.ALL)
                    )
                )
                .add_grids(
                    Grid(
                        Grid.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        Scheduler.PBS,
                        job_type=SupportedJobs.AUXILLARY,
                    ),
                    Grid(
                        Grid.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        Scheduler.PBS,
                        job_type=SupportedJobs.COMPUTE,
                    ),
                )
                .add_env(JAVA_HOME="/usr/bin/java"),
                Site(
                    "staging_site", arch=Arch.X86_64, os_type=OS.LINUX
                ).add_directories(
                    Directory(Directory.SHARED_SCRATCH, "/data").add_file_servers(
                        FileServer("scp://obelix.isi.edu/data", Operation.PUT),
                        FileServer("http://obelix.isi.edu/data", Operation.GET),
                    )
                ),
            )
        )

        with NamedTemporaryFile(mode="r+") as f:
            sc.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

        result["sites"].sort(key=lambda s: s["name"])
        for i in range(len(result["sites"])):
            result["sites"][i]["directories"].sort(key=lambda d: d["path"])

            for j in range(len(result["sites"][i]["directories"])):
                result["sites"][i]["directories"][j]["fileServers"].sort(
                    key=lambda fs: fs["url"]
                )

            if "grids" in result["sites"][i]:
                result["sites"][i]["grids"].sort(key=lambda g: g["jobtype"])

        assert "createdOn" in result["x-pegasus"]
        assert result["x-pegasus"]["createdBy"] == getpass.getuser()
        assert result["x-pegasus"]["apiLang"] == "python"
        del result["x-pegasus"]
        assert result == expected_json

    def test_write_default(self):
        expected_file = Path("sites.yml")
        SiteCatalog().write()

        try:
            expected_file.unlink()
        except FileNotFoundError:
            pytest.fail("could not find {}".format(expected_file))

    def test_site_catalog_key_ordering_on_yml_write(self):
        SiteCatalog().write()

        EXPECTED_FILE = Path("sites.yml")

        with EXPECTED_FILE.open() as f:
            # reading in as str so ordering of keys is not disrupted
            # when loaded into a dict
            result = f.read()

        EXPECTED_FILE.unlink()

        """
        Check that sc keys have been ordered as follows:
        - pegasus
        - sites
        """
        p = re.compile(r"x-pegasus:[\w\W]+pegasus: 5.0.4[\w\W]+sites:[\w\W]")
        assert p.match(result) is not None
