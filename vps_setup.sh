#!/usr/bin/env bash
# AUTO DONGHUA — VPS worker bootstrap.
# Usage: bash vps_setup.sh <koyeb-base-url> <secret>
# Checks every dependency the worker needs and installs whatever is
# missing (python3 >= 3.7, pip, telethon; Pillow/hachoir optional for
# thumbs & real video metadata). Old Debians without a usable
# python3 are reported honestly instead of half-installing.
set -u
BASE="${1:-}"
SECRET="${2:-}"
if [ -z "$BASE" ] || [ -z "$SECRET" ]; then
  echo "usage: bash vps_setup.sh <koyeb-base-url> <secret>"; exit 1
fi
RAW="https://raw.githubusercontent.com/AnimeDonghuo/autodonghua/arena/25bec0bc-autodonghua"
SUDO=""; [ "$(id -u)" != "0" ] && command -v sudo >/dev/null && SUDO="sudo"

py_ok() {
  command -v python3 >/dev/null || return 1
  python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 7) else 1)'
}

if ! py_ok; then
  echo "• python3 >= 3.7 missing — trying apt…"
  if command -v apt-get >/dev/null; then
    $SUDO apt-get update -y && $SUDO apt-get install -y python3 python3-pip
  fi
fi
if ! py_ok; then
  echo "❌ This system has no Python 3.7+ and apt could not add one."
  echo "   The worker needs Debian 10 (buster) or newer."
  exit 1
fi
echo "• python: $(python3 -V 2>&1)"

if ! python3 -c 'import telethon' 2>/dev/null; then
  echo "• installing telethon…"
  python3 -m pip install --user telethon 2>/dev/null \
    || python3 -m pip install --break-system-packages --user telethon \
    || $SUDO python3 -m pip install telethon
fi
python3 -c 'import telethon' || { echo "❌ telethon install failed"; exit 1; }
echo "• telethon: $(python3 -c 'import telethon;print(telethon.__version__)')"

for opt in Pillow hachoir; do
  python3 -c "import $opt" 2>/dev/null || {
    echo "• optional: installing $opt (thumbs / real video metadata)…"
    python3 -m pip install --user "$opt" 2>/dev/null \
      || python3 -m pip install --break-system-packages --user "$opt" 2>/dev/null
  }
done

echo "• fetching worker…"
curl -sL "$RAW/vps_worker.py" -o vps_worker.py || { echo "❌ fetch failed"; exit 1; }
printf '{"base": "%s", "secret": "%s"}\n' "${BASE%/}" "$SECRET" > vps_worker.json
chmod 600 vps_worker.json

pkill -f vps_worker.py 2>/dev/null
# procps-free kill scan: minimal images lack pkill, and a missed
# old worker next to a new one = duplicate jobs + wiped downloads
for d in /proc/[0-9]*; do
  a=$(tr '\0' '\n' < "$d/cmdline" 2>/dev/null)
  [ -z "$a" ] && continue
  case "$(echo "$a" | head -1)" in *python*)
    echo "$a" | grep -qx -e vps_worker.py -e '.*/vps_worker.py' \
      && kill "${d#/proc/}" 2>/dev/null;;
  esac
done
sleep 1
nohup python3 vps_worker.py > vps_worker.log 2>&1 &
sleep 3
if grep -q "worker starting" vps_worker.log 2>/dev/null; then
  echo "✅ worker running (pid $(pgrep -f vps_worker.py 2>/dev/null | head -1)) — log: vps_worker.log"
  echo "   Now flip it on in Telegram: /vps on"
else
  echo "❌ worker did not start — tail vps_worker.log:"; tail -5 vps_worker.log
  exit 1
fi
