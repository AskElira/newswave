# NewsWave on the Windows VPS

**Never run the Mac copy and the VPS copy at the same time with the same Alpaca keys** (two daemons = duplicate
streams, double orders, fighting over the same paper account). Stop one before starting the other.

All commands are PowerShell. `$R` = where you unzip, e.g. `C:\newswave`.

1. **Python 3.12 + uv.** `winget install Python.Python.3.12` and `winget install astral-sh.uv` (open a new shell after).
2. **Unzip the bundle** (`newswave-windows.zip`, built on the Mac from committed code, no secrets):
   `Expand-Archive newswave-windows.zip C:\` then `cd C:\newswave` (the zip's top folder is `newswave`).
3. **venv + install:**
   ```
   uv venv --python 3.12 .venv
   uv pip install --python .venv\Scripts\python.exe -e ".[dev]"
   ```
4. **Claude Code, native installer** (not npm: npm makes a `.cmd` shim, which NewsWave refuses for safety):
   `irm https://claude.ai/install.ps1 | iex`, then run `claude` once **as the user that will run the task** and log in.
   If `claude` is not on PATH for that user, set `CLAUDE_BIN=C:\Users\<you>\.local\bin\claude.exe` in `.env`.
5. **`.env`** (never commit, never zip): `copy .env.example .env`, then fill the Alpaca **PAPER** keys. Keep
   `EXECUTION_MODE=OBSERVE`. Set `PYTHONUTF8=1` for any manual run: `$env:PYTHONUTF8=1` (the task sets it itself).
6. **Check:** `.venv\Scripts\python.exe -m newswave check` (paper guard OK, keys set).
7. **Arm:** `.venv\Scripts\python.exe -m newswave arm` runs the full test suite on this machine; it must pass
   before paper execution can ever be enabled. Red = nothing written, tell the developer.
8. **Install the always-on task** (elevated PowerShell; asks for your Windows password so it can run logged off):
   `powershell -ExecutionPolicy Bypass -File deploy\windows\install-newswave-task.ps1`, then
   `Start-ScheduledTask -TaskName NewsWave`. It runs `run` only (OBSERVE). Paper orders are a separate, deliberate
   step: add `--execute` to the task's argument after `arm` (re-run `arm` after ANY code or parameter change).
9. **Dashboard:** over RDP on the VPS, `.venv\Scripts\python.exe -m newswave dashboard` then open
   http://127.0.0.1:8765 . Logs: `data\logs\newswave.log`.

## Stop
- Graceful: `New-Item data\STOP -ItemType File`. The daemon stops within ~1 s and deletes the file.
  (A leftover STOP file stops the next start immediately: delete it.) Positions are not flattened; broker stops protect them.
- Task: `Stop-ScheduledTask -TaskName NewsWave` (hard stop) or `uninstall-newswave-task.ps1` to remove it.
- Ctrl+C / Ctrl+Break in a console run also stops gracefully.

## Update
Stop it (STOP file), unzip the new bundle over the folder (keeps `.env`, `data`, `.venv`), re-run step 3's install
line if dependencies changed, `arm` again if you use `--execute`, then `Start-ScheduledTask -TaskName NewsWave`.
