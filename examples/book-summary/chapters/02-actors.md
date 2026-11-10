# Durable actors

An Actor has a stable identity and durable application state. Loom processes one message at a time for each actor. Different actors can execute concurrently across worker slots.

An actor turn returns its new state together with outgoing message and task intents. The backend commits these together with the message receipt. A crash cannot publish only half of this transition.

The book-summary actor remembers which chapters are complete. It submits a bounded number of chapter tasks and creates the final synthesis task only after every chapter has a successful result. It does not hold a database transaction open while waiting for a model.
