"""Exposes the patch-stack workflow in PlatformIO's "Project Tasks" panel.

PlatformIO has no project-level custom task list - custom targets belong to an
environment and show up under its "Custom" group. So platformio.ini declares a
[env:stack] that builds nothing and exists only to carry these, and because the
root platformio.ini is parsed before its extra_configs, it lands at the top of
the environment list rather than buried among the 500+ board environments.

Everything here shells out to tools/restack.sh, which stays the single source of
truth. See MAINTAINING.md.
"""

import os
import shutil
import subprocess
import sys

Import("env")  # noqa: F821  (injected by PlatformIO)

PROJECT_DIR = env.subst("$PROJECT_DIR")  # noqa: F821

# Environments the integration is proven against. Keep in step with the
# build-integration matrix in .github/workflows/stack-check.yml.
INTEGRATION_ENVS = [
    "waveshare_rp2040_lora_repeater",
    "waveshare_rp2040_lora_repeater_lowpower",
    "waveshare_rp2040_lora_repeater_ota_esp32",
    "Heltec_v3_repeater",
    "Heltec_v3_companion_radio_usb",
]


def _bash():
    """Git Bash on Windows, whatever is on PATH elsewhere."""
    if os.name == "nt":
        for candidate in (
            os.path.join(os.environ.get("ProgramFiles", r"C:\Program Files"), "Git", "bin", "bash.exe"),
            os.path.join(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"), "Git", "bin", "bash.exe"),
        ):
            if os.path.isfile(candidate):
                return candidate
    found = shutil.which("bash")
    if found:
        return found
    sys.stderr.write(
        "bash not found. On Windows it ships with Git for Windows; "
        "install it or run tools/restack.sh by hand.\n"
    )
    env.Exit(1)  # noqa: F821


def _run(argv):
    """Run argv in the project directory, failing the target on a non-zero exit."""
    code = subprocess.call(argv, cwd=PROJECT_DIR)
    if code != 0:
        env.Exit(code)  # noqa: F821


def _restack(subcommand):
    def action(*_args, **_kwargs):
        _run([_bash(), "tools/restack.sh", subcommand])
    return action


def _build_integration(*_args, **_kwargs):
    argv = [sys.executable, "-m", "platformio", "run"]
    for name in INTEGRATION_ENVS:
        argv += ["-e", name]
    _run(argv)


def _system_python():
    """A Python outside PlatformIO's own venv.

    sys.executable here is the interpreter PlatformIO runs itself in. The bot's
    dependencies (pycryptodome, or cryptography as the test's fallback) belong in
    your own environment, not in PlatformIO's - so run the tests with that one.
    """
    pio_home = os.path.realpath(os.path.expanduser("~/.platformio"))
    for name in ("python3", "python"):
        found = shutil.which(name)
        if found and not os.path.realpath(found).startswith(pio_home):
            return found
    return sys.executable


def _run_tools_tests(*_args, **_kwargs):
    python = _system_python()
    for start in ("tools", "tools/telegram_bot"):
        code = subprocess.call(
            [python, "-m", "unittest", "discover",
             "-s", start, "-p", "test_*.py", "-t", start, "-v"],
            cwd=PROJECT_DIR,
        )
        if code != 0:
            sys.stderr.write(
                "\nIf this is a missing module, the bot's dependencies are not "
                "installed for %s:\n    %s -m pip install -r tools/telegram_bot/requirements.txt\n"
                % (python, python)
            )
            env.Exit(code)  # noqa: F821


TASKS = [
    ("stack-check", _restack("check"),
     "1. Check (read-only)",
     "How far behind upstream, and whether the seven branches still merge. "
     "Touches nothing."),

    ("stack-rebase", _restack("rebase"),
     "2. Rebase onto upstream",
     "Restacks all seven branches onto their parents. REWRITES your branches. "
     "All-or-nothing: never rebase one by hand first."),

    ("stack-integrate", _restack("integrate"),
     "3. Regenerate patch_public",
     "Rebuilds the integration branch from the stack. Asks before overwriting, "
     "and leaves patch_public untouched if a merge fails."),

    ("stack-build", _build_integration,
     "4. Build integration",
     "Builds the environments the integration is proven against. A clean merge "
     "proves nothing - rerere replays wrong resolutions as happily as right ones."),

    ("stack-push", _restack("push"),
     "5. Push to origin",
     "Force-pushes the seven branches and patch_public. Shows what moves and asks "
     "first. It cannot tell whether you ran step 4 - if you did not, say no."),

    ("stack-tests", _run_tools_tests,
     "Run tools unit tests",
     "The Python tests carried by tools/companion-tools."),
]

for name, action, title, description in TASKS:
    env.AddCustomTarget(  # noqa: F821
        name=name,
        dependencies=None,
        actions=action,
        title=title,
        description=description,
        always_build=True,
    )
