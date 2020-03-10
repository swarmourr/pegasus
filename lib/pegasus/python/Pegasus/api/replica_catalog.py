from ._utils import _chained
from .errors import DuplicateError
from .mixins import MetadataMixin
from .writable import Writable, _filter_out_nones

PEGASUS_VERSION = "5.0"

__all__ = ["File", "ReplicaCatalog"]


class File(MetadataMixin):
    """
    A workflow File. This class is used to represent 
    :py:class:`~Pegasus.api.workflow.Job` inputs and outputs.
    """

    def __init__(self, lfn):
        """
        :param lfn: a unique logical filename 
        :type lfn: str
        """
        if not isinstance(lfn, str):
            raise TypeError(
                "invalid lfn: {lfn}; lfn must be of type str".format(lfn=lfn)
            )

        self.metadata = dict()
        self.lfn = lfn

    def __str__(self):
        return self.lfn

    def __hash__(self):
        return hash(self.lfn)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.lfn == other.lfn
        return False

    def __json__(self):
        return _filter_out_nones(
            {
                "lfn": self.lfn,
                "metadata": dict(self.metadata) if len(self.metadata) > 0 else None,
            }
        )


class ReplicaCatalog(Writable):
    """Maintains a mapping of logical filenames to physical filenames
        
        .. code-block:: python

            # Example
            if1 = File("if")
            if2 = File("if2")

            (ReplicaCatalog()
                .add_replica("local", if1, "/nfs/u2/ryan/data.csv")
                .add_replica("local", "if2", "/nfs/u2/ryan/data2.csv")
                .write())
    """

    _DEFAULT_FILENAME = "replicas.yml"

    def __init__(self):
        self.replicas = set()

    @_chained
    def add_replica(self, site, lfn, pfn, regex=False):
        """Add an entry to the replica catalog
        
        :param site: site at which this file resides
        :type site: str
        :param lfn: logical filename or :py:class:`~Pegasus.api.replica_catalog.File`
        :type lfn: str or File
        :param pfn: physical file name 
        :type pfn: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises DuplicateError: an entry with the same parameters already exists in the catalog
        :raises TypeError: lfn must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :return: self
        """
        if not isinstance(lfn, File) and not isinstance(lfn, str):
            raise TypeError(
                "invalid lfn: {lfn}; lfn must be of type File or str".format(lfn=lfn)
            )

        if isinstance(lfn, File):
            lfn = lfn.lfn

        replica = (site, lfn, pfn, regex)
        if replica in self.replicas:
            raise DuplicateError(
                "entry: {replica} already exists in this ReplicaCatalog".format(
                    replica=replica
                )
            )
        else:
            self.replicas.add(replica)

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"site": r[0], "lfn": r[1], "pfn": r[2], "regex": r[3]}
                for r in self.replicas
            ],
        }
