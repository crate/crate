import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta


TODAY = datetime.today()


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def parse_version(version):
    """Parse semantic version into tuple."""
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)

    if not match:
        raise ValueError(f"Invalid version: {version}")

    return tuple(map(int, match.groups()))



def major_minor(version):
    major, minor, _ = parse_version(version)
    return f"{major}.{minor}"



def load_versions_from_stdin():
    """
    Read version lines from stdin.

    Expected format:
        YYYY-MM-DD VERSION

    Example:
        2025-01-21 5.10.0
    """

    releases = []

    for raw_line in sys.stdin:
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        parts = line.split()

        if len(parts) != 2:
            raise ValueError(f"Invalid line: {line}")

        date_str, version = parts

        release_date = datetime.strptime(date_str, "%Y-%m-%d")

        releases.append(
            {
                "date": release_date,
                "version": version,
                "major_minor": major_minor(version),
                "parsed": parse_version(version),
            }
        )

    releases.sort(key=lambda x: x["date"])

    return releases



def compute_status_ranges(releases):
    """
    Compute lifecycle ranges for each minor release.
    """

    grouped = defaultdict(list)

    for r in releases:
        grouped[r["major_minor"]].append(r)

    minor_releases = []

    for mm, items in grouped.items():
        first = min(x["date"] for x in items)
        parsed = items[0]["parsed"]

        minor_releases.append(
            {
                "major_minor": mm,
                "date": first,
                "parsed": parsed,
            }
        )

    minor_releases.sort(key=lambda x: x["date"])

    result = []

    for i, current in enumerate(minor_releases):
        major, minor, _ = current["parsed"]

        start = current["date"]

        next_same_major = None
        next_major = None

        for future in minor_releases[i + 1 :]:
            f_major, _, _ = future["parsed"]

            if f_major == major and next_same_major is None:
                next_same_major = future

            if f_major > major and next_major is None:
                next_major = future

        if major < 5:
            end = next_major["date"] if next_major else TODAY

            result.append(
                {
                    "label": current["major_minor"],
                    "status": "deprecated",
                    "start": start,
                    "end": end,
                }
            )
            continue

        is_latest_minor = True

        for future in minor_releases:
            f_major, f_minor, _ = future["parsed"]

            if f_major == major and f_minor > minor:
                is_latest_minor = False
                break

        if is_latest_minor:
            maintained_end = TODAY

            if next_major:
                maintained_end = next_major["date"] + timedelta(days=180)

            result.append(
                {
                    "label": current["major_minor"],
                    "status": "maintained",
                    "start": start,
                    "end": maintained_end,
                }
            )

        else:
            maintained_end = next_same_major["date"]
            supported_end = TODAY

            result.append(
                {
                    "label": current["major_minor"],
                    "status": "maintained",
                    "start": start,
                    "end": maintained_end,
                }
            )

            result.append(
                {
                    "label": current["major_minor"],
                    "status": "supported",
                    "start": maintained_end,
                    "end": supported_end,
                }
            )

    return result



def generate_mermaid_gantt(ranges):
    """Generate Mermaid Gantt chart to stdout."""

    print("gantt")
    print("    title CrateDB Version Lifecycle")
    print("    dateFormat YYYY-MM-DD")
    print("    axisFormat %Y-%m")
    print()

    grouped = defaultdict(list)

    for item in ranges:
        major = item["label"].split(".")[0]
        grouped[major].append(item)

    for major in sorted(grouped.keys(), key=int):
        print(f"    section {major}.x")

        for item in sorted(grouped[major], key=lambda x: x["start"]):
            label = item["label"]
            status = item["status"]
            start = item["start"].strftime("%Y-%m-%d")
            end = item["end"].strftime("%Y-%m-%d")

            if status == "maintained":
                style = "crit"
            elif status == "supported":
                style = "active"
            else:
                style = "done"

            print(
                f"    {label} {status} :{style}, {start}, {end}"
            )

        print()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    releases = load_versions_from_stdin()
    ranges = compute_status_ranges(releases)
    generate_mermaid_gantt(ranges)


if __name__ == "__main__":
    main()
