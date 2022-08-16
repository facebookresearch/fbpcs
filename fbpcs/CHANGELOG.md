# Change log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
Types of changes

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case This project adheres to
  [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed
  - Mark validate_container_definition as deprecated in PCSContainerService since it is no longer a public method in ContainerService in fbpcp
### Removed

## [1.10.0] - 2022-08-12
### Added
  - Created interface for logging debug metrics for counter-based alerting
  - Created interface for writing checkpoints to track computation progress

### Changed

### Removed

## [1.9.0] - 2022-07-29

## [1.8.0] - 2022-07-22

### Added
- Added optional new argument --padding_size which is passed as multi_conversion_limit to the lift id spine combiner and as num_conversions_per_user to the pcf2 private lift stage

### Changed
- change StageSelector to select PIDShardStageService, PIDPrepareStageService and PIDRunProtocolStageService for PID_SHARD, PID_PREPARE and ID_MATCH stages respectively

### Removed
- Delete PIDStageService, PIDService because they are not used any more after we use the new PID stage services.
- Delete PIDDispatcher and PIDStageMapper
- Delete PIDStage(PIDShardStage/PIDPrepareStage/PIDProtocolRunStage) because they are not used any more after we use the new PID stage services.
- Delete PIDInstanceRepository

## [1.7.0] - 2022-06-23

### Changed
- Use C++20 CMake options for C++ compiler.
- set result visibility one docker arg
- allow configuration of result visibility on PC-CLI create_instance endpoint

## [1.6.0] - 2022-06-02

### Changed

- Changes since last release: [Commits](https://github.com/facebookresearch/fbpcs/compare/v1.5.0...91b1b5821feb7b8b4d40203bce866aad65f9c422)

## [1.5.0] - 2022-04-06

### Added

- Validation stage with input validation and binary file validation
- Integration of PID binaries into build and release scripts
- Split PREPARE stage into id spine combiner stage and resharder stage
- Introduction of StageState, general state storage entity
- Validation / transformation of input path (PC-CLI)
- Validation of valid statuses in one command runner
- use-row-numbers for PID
- Track start and end times on private computation instance
- PC-Service support custom onedocker binary repository
- Creation of PCSContainerInstance and PCSContainerService
- Container definition validation in OneDockerService
- Added pcf2.0 based attribution and aggregation games to fbpcs folder.
- Added new flow for PCF2.0 to pcs.

## [1.4.0] - 2022-03-07

### Added

- Added optional new argument --log_cost to private decoupled attribution,
  private decoupled aggregation, private shard aggregator and data processing
  stages
- In logging the cost for MPC runs, added few more fields in CostEstimation.cpp
- Added log_cost argument to MPC stage services

### Changed

- Changed LogRetriever to cover the publisher side use case.
- Refactor the logic of update MPC repository

### Removed

- Removed the usages of the `fail_fast` and `partial_container_retry_enabled`
  fields of PrivateComputationInstance in order to cleanup partially implemented
  "partial container failure recovery" feature

### Fixed

- Fixed bug when shard data is empty by adding dummy data.

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

- Removed pl_coordinator.py and pa_coordinator.py which already only had
  deprecation messages and no logic
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
- Upgraded version of EMP libraries (emp-tool 0.2.3, emp-ot 0.2.2, emp-sh2pc
  0.2.2)
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
