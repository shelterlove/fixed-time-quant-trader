from dataclasses import replace
from datetime import UTC, datetime
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

import pytest

from fixed_time import cli
from fixed_time.state import Store
from test_state_and_engine import live_config


def test_live_check_does_not_create_or_migrate_database(tmp_path,monkeypatch,capsys):
    config = live_config(tmp_path)
    monkeypatch.setattr(cli,"load_live_config",lambda _: config)
    monkeypatch.setattr(cli.Engine,"check_exchange",lambda _: {"equity":"100","positions":[]})
    assert cli.main(["live-check","--root",str(tmp_path)]) == 0
    assert not config.database_path.exists()
    assert not config.database_path.with_suffix(".db.lock").exists()


def test_backup_contains_uncheckpointed_wal_data(tmp_path,monkeypatch,capsys):
    config = live_config(tmp_path)
    store = Store(config.database_path)
    store.event("INFO","BACKUP_TEST","preserve me")
    monkeypatch.setattr(cli,"load_live_config",lambda _: config)
    assert cli.main(["live-backup","--root",str(tmp_path)]) == 0
    backup = next((tmp_path/"backups").glob("*.sqlite3"))
    with sqlite3.connect(backup) as db:
        assert db.execute("SELECT detail FROM v2_events WHERE code='BACKUP_TEST'").fetchone()[0] == "preserve me"
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    store.close()


def shell():
    if os.name == "nt":
        git_bash=Path(r"C:\Program Files\Git\bin\bash.exe")
        if git_bash.exists():
            return str(git_bash)
    return shutil.which("sh")


@pytest.mark.parametrize("failure",["","tests","preflight_after_stop","backup","startup"])
def test_deploy_orders_validation_backup_restart_and_failure_recovery(tmp_path,failure):
    executable=shell()
    if not executable:
        pytest.skip("POSIX shell unavailable")
    root=Path(__file__).resolve().parents[1]
    (tmp_path/"deploy.sh").write_bytes((root/"deploy.sh").read_bytes())
    (tmp_path/".env").write_text("TRADING_ENABLED=true\n")
    bin_dir=tmp_path/"bin"
    bin_dir.mkdir()
    (bin_dir/"git").write_text('#!/bin/sh\ncase "$*" in "rev-parse HEAD") echo abc123;; esac\n')
    (bin_dir/"docker").write_text('''#!/bin/sh
echo "$*" >> calls
case "$*" in
  "run --rm --network none fixed-time-deploy-tests") [ "$FAILURE" != tests ] || exit 1;;
  "compose ps --status running --services") echo trader;;
  "compose stop -t 60 trader") touch stopped;;
  *live-deploy-check*) if [ -f stopped ] && [ "$FAILURE" = preflight_after_stop ]; then exit 1; fi;;
  *live-backup*) [ "$FAILURE" != backup ] || exit 1;;
  "compose up -d --wait --wait-timeout 180") [ "$FAILURE" != startup ] || exit 1;;
esac
exit 0
''')
    for file in bin_dir.iterdir():
        file.chmod(0o755)
    env=dict(os.environ,PATH=str(bin_dir)+os.pathsep+os.environ["PATH"],FAILURE=failure)
    result=subprocess.run([executable,"-c",'export PATH="$PWD/bin:$PATH"; sh ./deploy.sh'],cwd=tmp_path,env=env,text=True,capture_output=True,timeout=30)
    calls=(tmp_path/"calls").read_text()
    assert (result.returncode == 0) == (failure == ""),result.stderr
    if failure == "tests":
        assert "compose stop" not in calls
    else:
        assert calls.index("run --rm --network none") < calls.index("compose stop")
        if failure != "preflight_after_stop":
            assert calls.index("compose stop") < calls.index("live-backup")
    if failure in {"backup","preflight_after_stop"}:
        assert "compose start trader" in calls
        assert "compose up" not in calls
    if failure == "startup":
        assert "compose start trader" not in calls
    if failure == "":
        assert calls.index("live-backup") < calls.index("compose up")
