# Obstore stream-zip example

This example demonstrates how to create a zip archive from files in one store and upload it to another store using the [`stream_zip`](https://github.com/uktrade/stream-zip) library.

This never stores any entire source file or the target zip file in memory, so you can zip large files with low memory overhead.
