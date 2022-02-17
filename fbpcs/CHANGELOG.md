# Change log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
Types of changes
  - `Added` for new features.
  - `Changed` for changes in existing functionality.
  - `Deprecated` for soon-to-be removed features.
  - `Removed` for now removed features.
  - `Fixed` for any bug fixes.
  - `Security` in case
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
  - Added optional new argument --log_cost to private decoupled attribution, private decoupled aggregation, private shard aggregator and data processing stages
  - In logging the cost for MPC runs, added few more fields in CostEstimation.cpp
  - Added log_cost argument to MPC stage services
### Changed

### Removed

## [1.3.0] - 2022-02-14
### Added
  - Added version 1 of input data validation stage.
  - Adding additional tests for decoupled_aggregation.

### Changed
  - Changes for stream log implementation.

### Removed
  - Remove directories as buck sources in fbcode/fbpcs/emp_games/TARGETS.

## [1.2.0] - 2022-02-03
### Added
  - Added version argument to run_fbpcs.sh
  - Added pid metric export stage
  - Added results visibility to private computation instance
  - Check for TODOs in config.yml parsing

### Changed
- Add Ad Id compression logic to decoupled aggregation game.

### Removed
  - Removed pl_coordinator.py and pa_coordinator.py which already only had deprecation messages and no logic
  - Removed instruction on using new commands from run_fbpcs.sh
  - Removed legacy attribution code.

## [1.1.0] - 2022-01-05
### Added
  - Add class variable `cloud_provider` to `PCEConfig`
  - Added PC instance, PID instance, and MPC instance serde versioning tests
  - Added support for decoupled attribution stage cancellation
  - Validate that config.yml doesn't contain TOODs

### Changed
  - Cleanup and move sharding/sharding_cpp to service/sharing_service
  - Refactor ShardingService to reuse RunBinaryBaseService
  - Consolidate wait_for_containers functions
  - Move CloudProvider enum class
  - Reformatted CostEstimation.h into .h/.cpp files.
  - Upgraded version of EMP libraries (emp-tool 0.2.3, emp-ot 0.2.2, emp-sh2pc 0.2.2)
  - Changed CostEstimation S3 bucket

### Removed
  - Removed Timestamp.h class

## [1.0.0] - 2021-12-09
### Added
  - Created official changelog

### Changed
  - Centralized default stage flow selection.
  - Finished PIDService container timeout implementation.
  - Upgraded GraphAPI version to v12.0.
  - Removed CostEstimation.h library from attribution directory and put in fbpcs.

### Removed
  - Removed run_post_processing_handlers PC-CLI endpoint.
  - Removed CloudCredentialService (formerly used by PIDService).
