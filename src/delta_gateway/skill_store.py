"""Bounded text skill packages with operator and reviewed agent management."""

import json
import os
import re
import shutil
import time
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import mkdtemp
from uuid import uuid4

import yaml

from delta_gateway.ownership import SessionLock

MAX_FILE = 256_000
MAX_PACKAGE = 512_000
NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
NOTES = ("PREFERENCES.md", "KNOWLEDGE.md")


def slug(name):
    if not isinstance(name, str) or not NAME.fullmatch(name):
        raise ValueError("Use a skill name of 1–64 lowercase letters, digits or hyphens")
    return name


def safe_file(root, relative):
    if not isinstance(relative, str) or not relative or "\\" in relative or ":" in relative:
        raise ValueError("Use a relative file path within the skill")
    parts = PurePosixPath(relative)
    if (
        relative != parts.as_posix()
        or parts.is_absolute()
        or any(p in {".", ".."} or p.startswith(".") or p.rstrip(" .") != p for p in parts.parts)
    ):
        raise ValueError("File paths must stay inside the skill")
    root = Path(root)
    if any(p.is_symlink() or p.is_junction() for p in (root, *root.parents)):
        raise ValueError("Linked skill directories are not supported")
    path = root
    for part in parts.parts:
        path = path / part
        if path.is_symlink() or path.is_junction():
            raise ValueError("Linked skill files are not supported")
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("File paths must stay inside the skill")
    return path


def text_file(path):
    if path.stat().st_size > MAX_FILE:
        raise ValueError("Skill file exceeds the 256 KB text limit")
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError:
        raise ValueError("Skill files must contain UTF-8 text") from None
    if "\0" in text:
        raise ValueError("Binary skill files are not supported")
    return text


def validate(name, files):
    slug(name)
    if not isinstance(files, dict) or not 1 <= len(files) <= 32 or "SKILL.md" not in files:
        raise ValueError("Supply SKILL.md and at most 31 supporting text files")
    total = 0
    seen = set()
    for relative, text in files.items():
        if not isinstance(relative, str) or relative.casefold() in seen:
            raise ValueError("Skill file paths must be unique, including case")
        seen.add(relative.casefold())
        safe_file(Path.cwd() / "skill-validation", relative)
        if not isinstance(text, str) or "\0" in text:
            raise ValueError("Skill files must contain text")
        size = len(text.encode("utf-8"))
        if size > MAX_FILE:
            raise ValueError("A skill file exceeds 256 KB")
        total += size
    if total > MAX_PACKAGE:
        raise ValueError("Skill package exceeds 512 KB")
    main = files["SKILL.md"]
    match = re.match(r"\A---\s*\n(.*?)\n---(?:\n|$)", main, re.S)
    if not match:
        raise ValueError("SKILL.md needs YAML name and description metadata")
    try:
        meta = yaml.safe_load(match[1])
    except yaml.YAMLError:
        raise ValueError("Invalid skill metadata") from None
    if not isinstance(meta, dict) or meta.get("name") != name:
        raise ValueError("The skill metadata name must match its folder name")
    if not isinstance(meta.get("description"), str) or not meta["description"].strip():
        raise ValueError("A skill description is required")


def replace_directory(source, target):
    # Windows scanners can briefly retain a newly written directory handle.
    for attempt in range(6):
        try:
            os.replace(source, target)
            return
        except PermissionError as error:
            if os.name != "nt" or error.winerror not in {5, 32, 33} or attempt == 5:
                raise
            time.sleep(0.02 * 2**attempt)


class SkillStore:
    def __init__(self, root):
        self.root = Path(root)

    def lock(self, name):
        key = sha256(os.path.normcase(str(self.root.resolve())).encode()).hexdigest()[:16]
        return SessionLock(self.root.parent / "skill-locks", key + "-" + slug(name))

    def package(self, name):
        return safe_file(self.root, slug(name))

    def read(self, name):
        folder = self.package(name)
        files = {}
        for path in folder.rglob("*"):
            relative = path.relative_to(folder).as_posix()
            safe_file(folder, relative)
            if path.is_file():
                files[relative] = text_file(path)
                if len(files) > 32 or sum(len(t.encode()) for t in files.values()) > MAX_PACKAGE:
                    raise ValueError("Skill package exceeds the supported size")
        validate(name, files)
        return files

    def write(self, name, files):
        validate(name, files)
        target = self.package(name)
        self.root.mkdir(parents=True, exist_ok=True)
        owner = self.lock(name)
        stage = Path(mkdtemp(prefix=".skill-", dir=self.root))
        backup = self.root / (".previous-" + name + "-" + uuid4().hex)
        committed = False
        try:
            for relative, text in files.items():
                path = safe_file(stage, relative)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            if target.exists():
                replace_directory(target, backup)
            try:
                replace_directory(stage, target)
                committed = True
            except BaseException:
                if backup.exists():
                    replace_directory(backup, target)
                raise
        finally:
            # These names are created under the checked, operator-owned skill directory.
            for path in (stage, backup) if committed else (stage,):
                if path.exists() and path.resolve().parent == self.root.resolve():
                    shutil.rmtree(path)
            owner.close()

    def add(self, folder):
        folder = Path(folder).resolve()
        name = slug(folder.name)
        source = SkillStore(folder.parent)
        self.write(name, source.read(name))
        return name

    def remove(self, name):
        target = self.package(name)
        owner = self.lock(name)
        try:
            if not target.is_dir():
                raise ValueError("No managed skill with that name")
            if target.resolve().parent != self.root.resolve():
                raise ValueError("Skill directory is outside the managed store")
            shutil.rmtree(target)
        finally:
            owner.close()


def page(skill, relative="SKILL.md", offset=1, limit=200):
    if type(offset) is not int or offset < 1 or type(limit) is not int or not 1 <= limit <= 400:
        raise ValueError("Use a positive line offset and a limit of 1–400 lines")
    root = skill.source.parent
    if relative == "SKILL.md":
        text = skill.body
    else:
        text = text_file(safe_file(root, relative))
    lines = text.splitlines()
    chosen = []
    size = 0
    for line in lines[offset - 1 : offset - 1 + limit]:
        if size + len(line) > 16000:
            if not chosen:
                raise ValueError("A line exceeds the page limit; shorten this skill file")
            break
        chosen.append(line)
        size += len(line) + 1
    end = offset - 1 + len(chosen)
    files = []
    for file in root.rglob("*"):
        relative_file = file.relative_to(root).as_posix()
        if file.is_file() and not file.is_symlink():
            safe_file(root, relative_file)
            files.append(relative_file)
            if len(files) >= 32:
                break
    return json.dumps(
        dict(
            name=skill.name,
            file=relative,
            offset=offset,
            text="\n".join(chosen),
            nextOffset=end + 1 if end < len(lines) else None,
            files=sorted(files),
        )
    )


def write_note(root, filename, text):
    if filename not in NOTES or not isinstance(text, str) or len(text.encode()) > 64_000:
        raise ValueError("Choose a supported note file and at most 64 KB of text")
    target = safe_file(root, filename)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = target.with_name(".note-" + uuid4().hex)
    try:
        stage.write_text(text, encoding="utf-8")
        replace_directory(stage, target)
    finally:
        stage.unlink(missing_ok=True)
