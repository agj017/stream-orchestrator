# Stream State Model

## Stream Lifecycle

`stream` is the primary resource lifecycle tracked by the orchestrator.

### States

- `PENDING`: accepted by API, persisted, and waiting for orchestration.
- `PROVISIONING`: controller is applying desired state to Kubernetes/MediaMTX.
- `RUNNING`: stream is active and serving traffic.
- `FAILED`: provisioning or reconciliation failed after retries.
- `STOPPING`: stop request accepted and termination is in progress.
- `STOPPED`: stream is terminated successfully.

### Allowed Transitions

- `PENDING -> PROVISIONING`
- `PROVISIONING -> RUNNING`
- `PROVISIONING -> FAILED`
- `FAILED -> PROVISIONING` (retry)
- `RUNNING -> STOPPING`
- `STOPPING -> STOPPED`
- `STOPPING -> FAILED`

### Transition Notes

- API creates streams in `PENDING`.
- Controller owns transitions from `PROVISIONING` onward.
- Every transition should update `updated_at`.
- `FAILED` must include an error reason for operations visibility.

