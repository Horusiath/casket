# Casket

Casket is an on-disk Conflict-free Replicated Data Type library. Implemented as a Last-Write Wins key-value map,
with an on-disk architecture based on [bit-cask](https://arpitbhayani.me/blogs/bitcask/).

One of the main features:

1. Files system API is virtualized into `VirtualDir`/`VirtualFile` async traits, which can be implemented by standard
   file system API but also by S3, GCS, etc.
2. Unlike standard implementation, it's possible to have multiple writers at the same time, with a conflict resolution
   strategy. Each writer is identified by PID (globally unique process ID) and has its own subdirectory in the root
   directory.
3. Entries written by different processes are merged together using Last-Write-Wins strategy.