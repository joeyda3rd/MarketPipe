# Chat Date Context Rule

When a chat session starts, the assistant **MUST** run a shell command (e.g. `date`) to capture the current date and keep that value in its private context for the entire session. If the conversation context is cleared or re-initialized, the assistant **MUST** execute the same command again to refresh the stored date before proceeding.

This rule guarantees that time-sensitive reasoning remains accurate even when the context window is rebuilt or truncated.
