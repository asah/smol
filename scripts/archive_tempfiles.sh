#!/bin/bash
# Archive temporary files created by Claude after committing
# This script moves all untracked files to archive/ directories

set -e

echo "=== Archiving untracked files ==="
echo

# Get list of untracked files from git status
untracked_files=$(git status --porcelain | grep '^??' | awk '{print $2}')

if [ -z "$untracked_files" ]; then
    echo "No untracked files to archive."
    exit 0
fi

# Process each untracked file
while IFS= read -r file; do
    if [ -z "$file" ]; then
        continue
    fi

    # Determine the archive directory based on file location
    if [[ "$file" == sql/* ]]; then
        archive_dir="sql/archive"
        archive_path="$archive_dir/$(basename "$file")"
    elif [[ "$file" == bench/* ]]; then
        archive_dir="bench/archive"
        archive_path="$archive_dir/$(basename "$file")"
    elif [[ "$file" == expected/* ]]; then
        archive_dir="expected/archive"
        archive_path="$archive_dir/$(basename "$file")"
    elif [[ "$file" == scripts/* ]]; then
        archive_dir="scripts/archive"
        archive_path="$archive_dir/$(basename "$file")"
    else
        # Root level files go to archive/
        archive_dir="archive"
        archive_path="$archive_dir/$(basename "$file")"
    fi

    # Create archive directory if it doesn't exist
    if [ ! -d "$archive_dir" ]; then
        echo "Creating directory: $archive_dir"
        mkdir -p "$archive_dir"
    fi

    # Move the file
    echo "Moving: $file -> $archive_path"
    mv "$file" "$archive_path"

done <<< "$untracked_files"

echo
echo "=== Archive complete ==="
echo "Run 'git status' to verify all files have been archived."
