package runtime

// File system path of a Unix Domain Socket used by workers to expose their interfaces
const ControlPlaneUDSPath = "unix:///tmp/heddle-cp.sock"
const WorkerUDSPath = "unix:///tmp/heddle-worker.sock"
