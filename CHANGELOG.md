# Changelog

All notable changes to Astrum will be documented in this file.

Astrum is currently in its early `0.1.x` stage. The core execution model is usable, but public APIs may still receive small refinements before `0.2.0`.

The format is inspired by Keep a Changelog, and this project follows semantic versioning as closely as possible during the early development phase.

## [0.1.1]

This release focuses on improving project readiness after the first public PyPI release.

### Changed

- Updated project status after the first public PyPI release.
- Improved README wording to better explain Astrum's scope and current maturity.
- Clarified that Astrum is an in-process async DAG orchestrator, not a distributed workflow platform.
- Clarified suitable and unsuitable use cases.
- Improved documentation consistency around installation, quick start, and examples.
- Cleaned up packaging and source comments left from the initial release preparation phase.

### Added

- Added this changelog.
- Added contribution guidelines.
- Added clearer community feedback requests.
- Added early project stability notes.
- Added guidance for reporting bugs, API design feedback, documentation issues, and real-world use cases.

### Notes

Version `0.1.0` was the first public release on PyPI.

Version `0.1.1` is intended to be the recommended version for early community testing and feedback.

## [0.1.0] - 2026-06-05

First public PyPI release of Astrum.

### Added

- Added the core task declaration API.
- Added task dependency declaration.
- Added async and sync task execution support.
- Added DAG-based execution planning.
- Added automatic concurrent execution for independent branches.
- Added upstream result injection.
- Added retry support.
- Added structured execution reports.
- Added basic documentation and examples.
- Added PyPI packaging.

### Notes

This was the initial public release. Some project metadata and documentation wording were still in a preparation state. Users are encouraged to use `0.1.1` or newer for early testing.
