#!/usr/bin/env python3
import argparse
import glob
import os
import subprocess
import sys
from pathlib import Path

# Import PYCOF's config system
try:
    import pycof as pc
except ImportError:
    print("Error: Could not import pycof. Make sure it's installed or run from the project directory.")
    sys.exit(1)

# Define
library = "PYCOF"


# Collect arguments
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--version", default=None, help="New version to load")
parser.add_argument("-t", "--test", action="store_true", help="Publish to PyPi test")
parser.add_argument("-p", "--publish", action="store_true", help="Publish to Git")
parser.add_argument("-m", "--message", default="", help="Message for the Git commit")

args = parser.parse_args()

lib_path = os.path.join(Path(__file__).parent.resolve())

# Set up working directory
os.chdir(lib_path)

# Define new version number if not provided in arguments
if args.version is None:
    # Get the current version from git tags (since we're using poetry-dynamic-versioning)
    try:
        result = subprocess.run(["git", "describe", "--tags", "--abbrev=0"], capture_output=True, text=True, check=True)
        current_tag = result.stdout.strip()
        current_version = current_tag.lstrip("v")  # Remove 'v' prefix if present
        print(f"Current version found is {current_version}.")

        # Parse version and increment patch version
        version_parts = current_version.split(".")
        version_parts[-1] = str(int(version_parts[-1]) + 1)
        new_version = ".".join(version_parts)
        print(f"New version number will be {new_version}.")
    except subprocess.CalledProcessError as e:
        print(f"Error getting current version from git tags: {e}")
        print("Make sure you have at least one git tag in the format v1.0.0")
        sys.exit(1)
else:
    new_version = args.version

print(f"Target version: {new_version}")

# Configure Poetry with PyPI tokens from pycof config
try:
    config = pc.get_config()
    pypi_token = config.get("PYPI_TOKEN")
    test_pypi_token = config.get("TEST_PYPI_TOKEN")  # Optional separate token for test PyPI

    if pypi_token:
        subprocess.run(["poetry", "config", "pypi-token.pypi", pypi_token], check=True)
        print("✓ PyPI token configured")
    else:
        print("Warning: PYPI_TOKEN not found in config. Make sure Poetry is configured with PyPI credentials.")

    if test_pypi_token:
        subprocess.run(["poetry", "config", "pypi-token.testpypi", test_pypi_token], check=True)
        print("✓ Test PyPI token configured")
    elif args.test:
        print("Warning: TEST_PYPI_TOKEN not found in config. Using main PYPI_TOKEN for test PyPI.")
        if pypi_token:
            subprocess.run(["poetry", "config", "pypi-token.testpypi", pypi_token], check=True)

except Exception as e:
    print(f"Warning: Could not configure PyPI tokens: {e}")
    print("Make sure Poetry is manually configured with tokens or credentials are available.")


# Create git tag for the new version (required for poetry-dynamic-versioning)
# Note: poetry-dynamic-versioning will automatically read this tag during build
# and inject the version into the package, overriding any version in pyproject.toml
try:
    subprocess.run(["git", "tag", "-a", f"v{new_version}", "-m", f"Version {new_version}"], check=True)
    print(f"Created git tag v{new_version}")
except subprocess.CalledProcessError as e:
    print(f"Error creating git tag: {e}")
    print("Tag might already exist. Continuing...")

# Clean and build with Poetry
try:
    # Clean dist directory
    subprocess.run(["rm", "-rf", "dist/*"], shell=True, check=False)

    # Build with Poetry (dynamic versioning will read the version from the git tag)
    print("Building package with Poetry (using automatic versioning from git tags)...")
    subprocess.run(["poetry", "build"], check=True)
    print("Package built successfully with Poetry")

    # Show what version was actually built
    import glob

    wheel_files = glob.glob("dist/*.whl")
    if wheel_files:
        wheel_name = os.path.basename(wheel_files[0])
        print(f"Built package: {wheel_name}")

except subprocess.CalledProcessError as e:
    print(f"Error building package: {e}")
    sys.exit(1)

# Publish with Poetry
try:
    if args.test:
        # Publish to test PyPI
        subprocess.run(["poetry", "publish", "--repository", "testpypi"], check=True)
        print("Package published to test PyPI successfully")
    else:
        # Publish to PyPI
        subprocess.run(["poetry", "publish"], check=True)
        print("Package published to PyPI successfully")
except subprocess.CalledProcessError as e:
    print(f"Error publishing package: {e}")
    sys.exit(1)


# Commit to git and push
if args.publish:
    try:
        subprocess.run(["git", "add", "--all"], check=True)
        subprocess.run(
            ["git", "commit", "-a", "-m", f"Upload version {new_version} to pypi. {args.message}"], check=True
        )
        # Tag was already created above, so just push it
        subprocess.run(["git", "push", "origin", "--tags"], check=True)
        subprocess.run(["git", "push"], check=True)
        git_update = "and changes pushed to git"
        print("Git commit and push completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error with git operations: {e}")
        git_update = "but git operations failed"
else:
    git_update = ""
    print("Note: Git tag was created but not pushed. Use -p flag to push to git.")

print(f"\n\n New version {new_version} loaded on PyPi {git_update}")
