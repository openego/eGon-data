"""
Delivered input data of the new eMobility MIT/LGV methodology.

The new methodology (issue #1460) no longer derives the EV fleet from KBA
registration statistics. Instead, one bundle per scenario is delivered by
the data providers, published on Zenodo as a single zip archive and
downloaded by the pipeline itself.

This module is deliberately free of imports from the consuming dataset
(:mod:`egon.data.datasets.emobility.motorized_individual_travel`), so that
its submodules and the validation rules can use it without an import
cycle.

Notes
-----
The bundle is *not* part of the egon-data data bundle. ``eGon2035``
keeps taking its trip tarball from the data bundle, unchanged.
"""

from pathlib import Path
import json
import os
import time
import zipfile

# The vehicle profiles and events of the legacy methodology are taken
# from the data bundle, everything else from the Zenodo archives below.
# This is the single place deciding which of the two code paths a
# scenario takes, cf. `is_legacy_scenario()`.
LEGACY_SCENARIOS = ("eGon2035",)

#: Working directory of the eMobility datasets (relative to the run's
#: CWD, per project convention -- never the repository).
WORKING_DIR = Path(".", "emobility")

#: Root the Zenodo archives are extracted into. The delivered archive
#: already uses the scenario name as its top level directory
#: (``status2024/`` in delivery v1.4), so extraction and lookup agree by
#: construction.
INPUT_DATA_DIR = WORKING_DIR / "input_data"

SIMBEV_METADATA_FILE = "metadata_simbev_run.json"
GEOLIS_METADATA_FILE = "metadata_geolis_run.json"

#: Files expected in an extracted scenario directory. Checked before the
#: first read so that a truncated archive fails with the missing names
#: instead of a `FileNotFoundError` in the middle of an import.
INPUT_FILES = {
    "ev_pool": "ev_pool.parquet",
    "ev_event": "ev_event.parquet",
    "ev_count_municipality": "ev_count_municipality.parquet",
    "ev_charging_location": "ev_charging_location.parquet",
    "ev_mapping_ev_municipality": "ev_mapping_ev_municipality.parquet",
    # One row per placement (~5e8 for status2024, ~5e9 for each reGon
    # scenario). Folded into one event-id array per charging location
    # on import, cf. `mit_import.import_location_events()`.
    "ev_mapping_event_location": "ev_mapping_event_location.parquet",
    "metadata_simbev_run": SIMBEV_METADATA_FILE,
    "metadata_geolis_run": GEOLIS_METADATA_FILE,
}

#: Columns the importers read from each delivered parquet file. Checked
#: against the parquet footers right after extraction, so a delivery
#: with a missing or renamed column fails before the first import instead
#: of hours into the run.
REQUIRED_COLUMNS = {
    "ev_pool": ["ev_id", "rs7_id", "type"],
    "ev_event": [
        "event_id",
        "ev_id",
        "charging_use_case",
        "location",
        "nominal_charging_capacity_kW",
        "soc_start",
        "soc_end",
        "chargingdemand_battery_kWh",
        "park_start_timesteps",
        "park_end_timesteps",
        "drive_start_timesteps",
        "drive_end_timesteps",
        "consumption_kWh",
    ],
    "ev_count_municipality": [
        "ags",
        "rs7_id",
        "bev_mini",
        "bev_medium",
        "bev_luxury",
        "phev_mini",
        "phev_medium",
        "phev_luxury",
        "bev_commercial",
        "phev_commercial",
        "bev_light_duty_vehicle",
    ],
    "ev_charging_location": [
        "location_id",
        "charging_points",
        "average_charging_capacity",
        "use_case",
        "candidate_uid",
        "is_synthetic_location",
        "geometry",
    ],
    "ev_mapping_ev_municipality": ["ev_id"],
    "ev_mapping_event_location": ["event_id", "location_id"],
}

#: Keys of `metadata_geolis_run.json` that identify a delivery. Recorded
#: next to every extraction, cf. :func:`delivery_identity`.
DELIVERY_IDENTITY_KEYS = (
    "consolidated_at",
    "n_locations",
    "n_event_rows",
    "n_vehicle_instances",
)

#: Whether the input data may be fetched from Zenodo. Off while the
#: current delivery (consolidated 8-15 October 2026) is not published
#: yet: the scenario directories are then provided locally, e.g. as
#: symlinks, in :data:`INPUT_DATA_DIR`.
ZENODO_DOWNLOAD_ENABLED = False

# Zenodo dataset
ZENODO_RECORD = 23162886

ZENODO_URLS = {
    "status2024": (
        f"https://zenodo.org/record/{ZENODO_RECORD}/files/status2024.zip"
    ),
    "reGon2037": (
        f"https://zenodo.org/record/{ZENODO_RECORD}/files/reGon2037.zip"
    ),
    "reGon2045": (
        f"https://zenodo.org/record/{ZENODO_RECORD}/files/reGon2045.zip"
    ),
}

#: Seconds a stale download lock is tolerated before it is broken. The
#: archives are several GB, so this has to be generous.
_LOCK_TIMEOUT = 6 * 3600
_LOCK_POLL = 10


def is_legacy_scenario(scenario_name: str) -> bool:
    """Whether a scenario uses the legacy (simBEV tarball) methodology.

    This is the single place that decides the dual code path of the MIT
    and the charging infrastructure dataset. Branch on the scenario
    name, never on the presence of an input file: the interim
    configuration of PR #1485 maps ``status2024`` and ``reGon2037`` onto
    the archived eGon2035 tarball, so a "use the new file if it is
    there" fallback would silently supply that run's technical data.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    bool
        True for scenarios on the legacy methodology
    """
    return scenario_name in LEGACY_SCENARIOS


def new_methodology_scenarios(scenario_names) -> list:
    """Subset of `scenario_names` that uses the new methodology."""
    return [_ for _ in scenario_names if not is_legacy_scenario(_)]


def legacy_scenarios(scenario_names) -> list:
    """Subset of `scenario_names` that uses the legacy methodology."""
    return [_ for _ in scenario_names if is_legacy_scenario(_)]


def zenodo_url(scenario_name: str) -> str:
    """Zenodo URL of a scenario's input data archive.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    str
        Complete URL of the zip archive
    """
    try:
        return ZENODO_URLS[scenario_name]
    except KeyError:
        raise ValueError(
            f"No Zenodo record configured for scenario '{scenario_name}'. "
            f"Known scenarios: "
            f"{sorted(ZENODO_URLS)}."
        )


def scenario_input_dir(scenario_name: str) -> Path:
    """Directory the scenario's input data is extracted to."""
    return INPUT_DATA_DIR / scenario_name


def input_file(scenario_name: str, key: str) -> Path:
    """Path of one delivered input file.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    key : str
        Key of :data:`INPUT_FILES`

    Returns
    -------
    pathlib.Path
        Path of the file in the extracted scenario directory
    """
    path = scenario_input_dir(scenario_name) / INPUT_FILES[key]
    if not path.is_file():
        raise FileNotFoundError(
            f"Input data file '{INPUT_FILES[key]}' for scenario "
            f"'{scenario_name}' not found at {path}. Run the input data "
            f"download (task `download-and-extract`) first."
        )
    return path


def missing_input_files(scenario_name: str) -> list:
    """Delivered files that are not (yet) in the scenario directory."""
    directory = scenario_input_dir(scenario_name)
    return [
        name
        for name in INPUT_FILES.values()
        if not (directory / name).is_file()
    ]


def delivery_marker(scenario_name: str) -> Path:
    """File recording which delivery a scenario directory holds.

    Kept next to the scenario directory rather than inside it, so that a
    directory symlinked to a read-only delivery folder works as well.
    """
    return INPUT_DATA_DIR / f"{scenario_name}.delivery.json"


def delivery_identity(geolis_metadata: dict) -> dict:
    """The identifying keys of a delivery's GeoLIS run metadata."""
    return {key: geolis_metadata.get(key) for key in DELIVERY_IDENTITY_KEYS}


def _archive_identity(archive: Path, scenario_name: str) -> dict:
    """Delivery identity read from the GeoLIS metadata inside an archive."""
    with zipfile.ZipFile(archive, "r") as zip_ref:
        member = next(
            (
                _
                for _ in zip_ref.namelist()
                if _.endswith(f"{scenario_name}/{GEOLIS_METADATA_FILE}")
            ),
            None,
        )
        if member is None:
            raise FileNotFoundError(
                f"{GEOLIS_METADATA_FILE} for scenario '{scenario_name}' "
                f"not found in {archive}."
            )
        return delivery_identity(json.loads(zip_ref.read(member)))


def _directory_identity(scenario_name: str) -> dict:
    """Delivery identity read from the extracted GeoLIS metadata."""
    path = scenario_input_dir(scenario_name) / GEOLIS_METADATA_FILE
    with open(path) as f:
        return delivery_identity(json.load(f))


def _recorded_identity(scenario_name: str):
    """Delivery identity recorded at the last extraction, or None."""
    marker = delivery_marker(scenario_name)
    if not marker.is_file():
        return None
    with open(marker) as f:
        return json.load(f)


def _record_identity(scenario_name: str, identity: dict) -> None:
    """Record which delivery the scenario directory holds."""
    delivery_marker(scenario_name).parent.mkdir(parents=True, exist_ok=True)
    with open(delivery_marker(scenario_name), "w") as f:
        json.dump(identity, f, indent=2)


def is_extracted(scenario_name: str, identity: dict = None) -> bool:
    """Whether the scenario's delivery is completely extracted.

    Complete file names alone are not enough: a left-over extraction of
    an earlier delivery has the same names. With `identity` given, the
    recorded delivery must match it too.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    identity : dict, optional
        Delivery identity the extraction has to match, cf.
        :func:`delivery_identity`
    """
    if missing_input_files(scenario_name):
        return False
    if identity is None:
        return True
    return _recorded_identity(scenario_name) == identity


def missing_columns(scenario_name: str) -> dict:
    """Columns the importers need that a delivered file does not have.

    Reads the parquet footers only, no data.

    Returns
    -------
    dict
        File key -> list of missing column names, only for files that
        miss any
    """
    # Imported here so that consumers needing only the constants do not
    # pull in pyarrow.
    import pyarrow.parquet as pq

    missing = {}
    for key, columns in REQUIRED_COLUMNS.items():
        schema = pq.read_schema(
            scenario_input_dir(scenario_name) / INPUT_FILES[key]
        )
        absent = [_ for _ in columns if _ not in schema.names]
        if absent:
            missing[INPUT_FILES[key]] = absent
    return missing


def verify_extracted_files(scenario_name: str) -> None:
    """Check that the extracted delivery is complete and readable.

    Fails with everything that is missing at once -- file names first,
    then the columns every importer reads -- rather than letting the
    first read fail somewhere in the middle of an import. Three
    deliveries so far shipped a missing or renamed column.
    """
    missing = missing_input_files(scenario_name)
    if missing:
        raise FileNotFoundError(
            f"Input data for scenario '{scenario_name}' is incomplete: "
            f"{missing} missing in {scenario_input_dir(scenario_name)}."
        )

    absent = missing_columns(scenario_name)
    if absent:
        raise ValueError(
            f"Input data for scenario '{scenario_name}' lacks columns "
            f"the import needs: {absent}. The delivery does not match "
            f"the agreed format."
        )


class _DownloadLock:
    """Crude cross-process lock around the shared download directory.

    Retried or concurrently started tasks may fetch and extract the same
    multi-GB archive at the same time. Without mutual exclusion one would
    read a half-written file.
    """

    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                self.acquired = True
                return self
            except FileExistsError:
                try:
                    age = time.time() - self.path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age > _LOCK_TIMEOUT:
                    print(
                        f"Breaking stale download lock {self.path} "
                        f"({age / 3600:.1f} h old)."
                    )
                    self.path.unlink(missing_ok=True)
                    continue
                time.sleep(_LOCK_POLL)

    def __exit__(self, *exc):
        if self.acquired:
            self.path.unlink(missing_ok=True)
        return False


def _use_local_delivery(scenario_name: str) -> Path:
    """Use a scenario directory provided locally instead of downloaded.

    The delivery identity is read from the directory itself. A change
    against the recorded one is logged, so it is visible in the task log
    which delivery a run used.
    """
    directory = scenario_input_dir(scenario_name)
    if missing_input_files(scenario_name):
        raise FileNotFoundError(
            f"The Zenodo download is disabled "
            f"(`ZENODO_DOWNLOAD_ENABLED`), so the input data for "
            f"scenario '{scenario_name}' has to be provided in "
            f"{directory.resolve()}, e.g. as a symlink to the delivered "
            f"folder. Missing: {missing_input_files(scenario_name)}."
        )

    identity = _directory_identity(scenario_name)
    recorded = _recorded_identity(scenario_name)
    if recorded is None:
        print(f"  Using delivery {identity} from {directory.resolve()}.")
    elif recorded != identity:
        print(
            f"  WARNING: the delivery in {directory.resolve()} changed "
            f"since the last run: {recorded} -> {identity}."
        )
    else:
        print(f"  Using delivery {identity} (unchanged).")
    _record_identity(scenario_name, identity)
    return directory


def download_and_extract_scenario(scenario_name: str) -> Path:
    """Download and extract the input data archive of one scenario.

    Both steps are idempotent: an existing zip is not fetched again and
    an extraction of the *same* delivery is not repeated. A
    full-scenario archive is several GB, so a re-run of the task must
    not re-fetch it. An extraction of a different delivery -- same file
    names, different content -- is replaced, cf. :func:`is_extracted`.

    While :data:`ZENODO_DOWNLOAD_ENABLED` is off, the scenario directory
    has to be provided locally and is only checked.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    pathlib.Path
        The extracted scenario directory
    """
    if not ZENODO_DOWNLOAD_ENABLED:
        directory = _use_local_delivery(scenario_name)
        verify_extracted_files(scenario_name)
        return directory

    # Imported here so that module import does not pull in urllib for
    # consumers that only need the constants.
    from urllib.request import urlretrieve

    url = zenodo_url(scenario_name)
    INPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    archive = INPUT_DATA_DIR / f"{scenario_name}.zip"
    directory = scenario_input_dir(scenario_name)

    with _DownloadLock(INPUT_DATA_DIR / f"{scenario_name}.lock"):
        if archive.is_file():
            print(f"Archive {archive} already present, skipping download.")
        else:
            print(f"Downloading {url} to {archive}...")
            # `urllib.request.urlretrieve`, as every other Zenodo
            # download in this repository does. Do not use
            # `requests.get(url, stream=True)`: it returns 403 from
            # Zenodo under Python 3.10 / urllib3 2.5 and writes the
            # error page without checking the status code, which
            # surfaces much later as a corrupt zip.
            #
            # Download to a temporary name and rename only once it is
            # complete, so an interrupted download is not mistaken for
            # a usable archive on the next run.
            partial = archive.with_suffix(".zip.part")
            urlretrieve(url, partial)
            partial.replace(archive)

        identity = _archive_identity(archive, scenario_name)
        if is_extracted(scenario_name, identity):
            print(
                f"Delivery {identity} of scenario '{scenario_name}' "
                f"already extracted to {directory}, skipping extraction."
            )
        else:
            recorded = _recorded_identity(scenario_name)
            if recorded is not None and recorded != identity:
                print(
                    f"Replacing the extraction of delivery {recorded} "
                    f"by {identity}."
                )
            print(f"Extracting {archive} to {INPUT_DATA_DIR}...")
            with zipfile.ZipFile(archive, "r") as zip_ref:
                zip_ref.extractall(INPUT_DATA_DIR)
            _record_identity(scenario_name, identity)

        verify_extracted_files(scenario_name)

    return directory
