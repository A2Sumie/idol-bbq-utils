#!/bin/sh
set -e

# Ensure database file exists (Prisma might complain if the file is missing, but sqlite provider usually creates it)
# However, since we mount it, it might be created by docker as a directory if not exists.
# The user mapped ./assets/refactor.db:/app/data.db.

export PATH="/app/tools/bin:$PATH"

echo "Refreshing media tools..."
if ! /app/tools/update-media-tools.sh; then
    echo "Media tools refresh failed, continuing with bundled versions." >&2
fi

echo "Migrating database..."
# Use the installed prisma CLI
bunx prisma migrate deploy

echo "Starting application..."
exec bun /app/bin.js
