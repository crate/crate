#!/usr/bin/env python3

from subprocess import check_call, check_output, DEVNULL
from dataclasses import dataclass
from datetime import date
from functools import partial
import glob
import argparse
import os


@dataclass
class Target:
    os: str
    arch: str
    classifier: str
    ext: str = "tar.gz"

    @property
    def short_arch(self):
        return self.arch.split("_")[0]


def main():
    parser = argparse.ArgumentParser("tarballs")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--gpg-key", type=str)
    parser.add_argument("--releases-basepath", type=str, default="/var/data/www/cdn.crate.io/downloads/releases/")
    parser.add_argument("--repo-path", type=str, default=os.getcwd())
    args = parser.parse_args()

    targets = [
        Target("linux", "x86_64", "linux-x86_64"),
        Target("linux", "aarch64", "linux-aarch_64"),
        Target("mac", "x86_64", "osx-x86_64"),
        Target("mac", "aarch64", "osx-aarch_64"),
        Target("windows", "x86_64", "windows-x86_64", ext="zip"),
    ]
    repo_path = os.path.expanduser(args.repo_path)
    timestamp = date.today().strftime("%Y-%m-%d-%H-%M")
    jdk_version = check_output(
        ["./mvnw", "help:evaluate", "-Dexpression=versions.jdk", "-q", "-DforceStdout"],
        cwd=repo_path,
        text=True,
    )
    jdk_path = glob.glob(os.path.expanduser(f"~/.m2/jdks/jdk-{jdk_version}*"))[0]
    env = {
        "JAVA_HOME": jdk_path
    }
    if args.verbose:
        call = partial(check_call, cwd=repo_path, env=env)
    else:
        call = partial(check_call, cwd=repo_path, env=env, stdout=DEVNULL)

    for target in targets:
        print(f"Building artifact for {target.os} {target.arch}")
        build_cmd = [
            "./mvnw",
            f"-Dtimestamp={timestamp}",
            "-P", "release",
            "-P", "jmods",
            f"-Djmods.arch={target.arch}",
            f"-Djmods.os={target.os}",
            f"-Djmods.ext={target.ext}",
            f"-Dos.classifier={target.classifier}",
            "-T", "1C",
            "clean", "package",
            "-DskipTests=true",
            "-Dcheckstyle.skip",
            "-Djacoco.skip=true",
        ]
        call(build_cmd)
        artifacts = glob.glob(f"app/target/crate-*.{target.ext}", root_dir=repo_path)
        assert artifacts, "Must have build artifact after build call"
        tarball = artifacts[0]
        sign_cmd = [
            "gpg",
        ]
        if args.gpg_key:
            sign_cmd.append("--local-user")
            sign_cmd.append(args.gpg_key)
        sign_cmd += [
            "--armor",
            "--detach-sig",
            "--batch",
            tarball
        ]
        call(sign_cmd)

        if args.upload:
            target_path = f"{args.releases_basepath}cratedb/{target.short_arch}_{target.os}/"
            print(f"Uploading {tarball} to {target_path}")
            call([
                "scp",
                tarball,
                f"web-int.vdc.cr8.net:{target_path}"
            ])
            call([
                "scp",
                f"{tarball}.asc",
                f"web-int.vdc.cr8.net:{target_path}"
            ])


if __name__ == "__main__":
    main()
