# Tasks and immutable objects

Loom represents a finite computation as a Task. A client submits its input and receives a handle to the work. A worker claims the task, executes it, and commits an immutable output reference.

ObjectRef identifies data by its content digest. Workers can read an entire object or an exact byte range. Applications must choose boundaries that preserve records and UTF-8 characters.

A worker can fail before committing its result. Loom can retry the computation, so applications must distinguish pure computation from external effects. A completed model request might be billed twice if its first result was never committed.
