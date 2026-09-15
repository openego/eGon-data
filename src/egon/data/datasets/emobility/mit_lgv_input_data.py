"""
Delivered input data of the new eMobility MIT/LGV methodology.

The new methodology (issue #1460) no longer derives the EV fleet from KBA
registration statistics. Instead, one bundle per scenario is delivered by
the data providers, published on Zenodo as a single zip archive and
downloaded by the pipeline itself.

This module is deliberately free of imports from the two consuming
datasets
(:mod:`egon.data.datasets.emobility.motorized_individual_travel` and
:mod:`egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure`)
so that both can use it without an import cycle.

Notes
-----
The bundle is *not* part of the egon-data data bundle. ``eGon2035``
keeps taking its trip tarball from the data bundle, unchanged.
"""

from pathlib import Path
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
    # Not imported into the database (D1): ~5e8 rows for status2024 and
    # an estimated ~8e9 for reGon2045. Consumers read the parquet file
    # from the extracted archive directly.
    "ev_mapping_event_location": "ev_mapping_event_location.parquet",
    "metadata_simbev_run": SIMBEV_METADATA_FILE,
    "metadata_geolis_run": GEOLIS_METADATA_FILE,
}

# Zenodo testing and production are separate deployments with separate
# record ids, so the switch is not a host substitution -- the full URL
# differs per scenario per environment. Flip ZENODO_ENVIRONMENT to
# "zenodo" for production; that is the one edit needed.
#
# TODO(#1460): the records do not exist yet. Replace the PLACEHOLDER
# record ids with the real ones; `grep -rn PLACEHOLDER src/` must come
# back empty before this is released.
ZENODO_ENVIRONMENT = "zenodo_sandbox"

ZENODO_URLS = {
    "zenodo_sandbox": {
        "status2024": (
            "https://sandbox.zenodo.org/record/PLACEHOLDER/files/"
            "status2024.zip"
        ),
        "reGon2037": (
            "https://sandbox.zenodo.org/record/PLACEHOLDER/files/"
            "reGon2037.zip"
        ),
        "reGon2045": (
            "https://sandbox.zenodo.org/record/PLACEHOLDER/files/"
            "reGon2045.zip"
        ),
    },
    "zenodo": {
        "status2024": (
            "https://zenodo.org/record/PLACEHOLDER/files/status2024.zip"
        ),
        "reGon2037": (
            "https://zenodo.org/record/PLACEHOLDER/files/reGon2037.zip"
        ),
        "reGon2045": (
            "https://zenodo.org/record/PLACEHOLDER/files/reGon2045.zip"
        ),
    },
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
        return ZENODO_URLS[ZENODO_ENVIRONMENT][scenario_name]
    except KeyError:
        raise ValueError(
            f"No Zenodo record configured for scenario "
            f"'{scenario_name}' in environment '{ZENODO_ENVIRONMENT}'. "
            f"Known scenarios: "
            f"{sorted(ZENODO_URLS[ZENODO_ENVIRONMENT])}."
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


def is_extracted(scenario_name: str) -> bool:
    """Whether the scenario's archive has already been extracted."""
    return not missing_input_files(scenario_name)


def verify_extracted_files(scenario_name: str) -> None:
    """Check that the extracted archive is complete.

    Raises with all missing names at once rather than letting the first
    read fail somewhere in the middle of an import.
    """
    missing = missing_input_files(scenario_name)
    if missing:
        raise FileNotFoundError(
            f"Input data for scenario '{scenario_name}' is incomplete: "
            f"{missing} missing in {scenario_input_dir(scenario_name)}."
        )


class _DownloadLock:
    """Crude cross-process lock around the shared download directory.

    The MIT and the charging infrastructure dataset are independent (D3)
    and may therefore fetch and extract the same multi-GB archive
    concurrently. Without mutual exclusion one would read a half-written
    file.
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


def download_and_extract_scenario(scenario_name: str) -> Path:
    """Download and extract the input data archive of one scenario.

    Both steps are idempotent: an existing zip is not fetched again and
    a complete extraction is not repeated. A full-scenario archive is
    several GB, so a re-run of the task must not re-fetch it.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    pathlib.Path
        The extracted scenario directory
    """
    # Imported here so that module import does not pull in urllib for
    # consumers that only need the constants.
    from urllib.request import urlretrieve

    url = zenodo_url(scenario_name)
    INPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    archive = INPUT_DATA_DIR / f"{scenario_name}.zip"
    directory = scenario_input_dir(scenario_name)

    # Nothing to do at all: check before taking the lock, so a run that
    # only reads existing data never waits behind a download.
    if is_extracted(scenario_name):
        print(
            f"Input data for scenario '{scenario_name}' already "
            f"extracted to {directory}, nothing to do."
        )
        return directory

    with _DownloadLock(INPUT_DATA_DIR / f"{scenario_name}.lock"):
        # The two steps are decided independently, and both are
        # re-checked here: while this process waited for the lock,
        # another one may have downloaded the archive, extracted it, or
        # both.
        if is_extracted(scenario_name):
            print(
                f"Input data for scenario '{scenario_name}' already "
                f"extracted to {directory}, skipping extraction."
            )
            return directory

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

        print(
            f"Extracting {archive} to {INPUT_DATA_DIR} "
            f"({len(missing_input_files(scenario_name))} of "
            f"{len(INPUT_FILES)} files missing)..."
        )
        with zipfile.ZipFile(archive, "r") as zip_ref:
            zip_ref.extractall(INPUT_DATA_DIR)

        verify_extracted_files(scenario_name)

    return directory
