import subprocess
import sys
import os
import shutil

# Get the current user's username from the environment variables
# It tries to get 'USERNAME' first, otherwise 'USER'
user_name = os.environ.get("USERNAME") or os.environ.get("USER")

# Define a dictionary to store user-specific paths
# Each user has 'box_path' and 'github_path'
user_paths = {
    "Nikil": {
        "box_path": "C:/Users/Admin/Box/2. Projects/9. Migration",
        "github_path": "C:/Users/Admin/OneDrive - University of Chicago IIC/Desktop/gramodaya",
    },
}

# Check if the current user is defined in the user_paths dictionary
# Raise an error if the user is not found
if user_name not in user_paths:
    raise ValueError(
        f"User '{user_name}' not found in user_paths. Please add your paths."
    )

# Extract the user's specific paths based on the username
box_path = user_paths[user_name]["box_path"]
github_path = user_paths[user_name]["github_path"]

# Define a dictionary to store various paths for data and output
# Paths are constructed using the user's specific paths
paths = {
    "box_path": box_path,
    "github_path": github_path,
    "data_path": os.path.join(box_path, "Data"),
    "output_path": os.path.join(box_path, "Output"),
}

# Create directories if they don't exist
for path in paths.values():
    if not os.path.exists(path):
        os.makedirs(path)

# Define a dictionary of packages where keys are package names
# and values are their common aliases
packages = {
    "numpy": "np",
    "pandas": "pd",
    "matplotlib": "mpl",
    "seaborn": "sns",
    "pyarrow": None,
    "polars": "pl",
    "ydata_profiling": "ydata_profiling",
}


def run_command(command):
    """Run a shell command and handle errors."""
    # Print the command that will be executed
    print(f"> Running: {' '.join(command)}")
    # Execute the command and capture the output and errors
    result = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    # Check if the command was successful
    if result.returncode != 0:
        # Print the error message if the command failed
        print(f"Error running: {' '.join(command)}\n{result.stderr}")
        # Raise an exception with the error details
        raise RuntimeError(f"Command failed: {' '.join(command)}")


def ensure_piptools():
    """Ensure that pip-tools is installed."""
    try:
        import piptools  # noqa: F401
    except ImportError:
        print("> pip-tools not found. Installing...")
        run_command([sys.executable, "-m", "pip", "install", "--upgrade", "pip-tools"])


def clear_cache():
    """Clear cached files and pip cache."""
    print("> Clearing cache...")

    # Remove requirements files if they exist
    for file in ["requirements.in", "requirements.txt"]:
        try:
            os.remove(os.path.join(github_path, file))
            print(f"Deleted: {os.path.join(github_path, file)}")
        except FileNotFoundError:
            pass

    # Clear pip cache
    run_command([sys.executable, "-m", "pip", "cache", "purge"])


def uninstall_packages():
    """Uninstall all packages listed in packages."""
    # Indicate the start of the uninstall process
    print("> Uninstalling packages...")
    # Iterate over each package name in the packages dictionary
    for package in packages.keys():
        # Uninstall the package using pip
        run_command([sys.executable, "-m", "pip", "uninstall", "-y", package])


def generate_requirements():
    """Generate and install dependencies from requirements.in."""
    # Indicate the start of generating the requirements file
    print("> Generating requirements file...")

    # Ensure pip-tools is installed before using it
    ensure_piptools()

    with open(os.path.join(github_path, "requirements.in"), "w") as file:
        # Write each package name to the file
        file.write("\n".join(packages.keys()) + "\n")

    # Use piptools to compile the requirements.in file into requirements.txt
    run_command(
        [
            sys.executable,
            "-m",
            "piptools",
            "compile",
            "--output-file",
            os.path.join(github_path, "requirements.txt"),
            os.path.join(github_path, "requirements.in"),
        ]
    )
    # Install the packages listed in requirements.txt
    run_command(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            os.path.join(github_path, "requirements.txt"),
            "--no-cache-dir",
            "--prefer-binary",
            "--upgrade-strategy",
            "only-if-needed",
        ]
    )


if __name__ == "__main__":
    # Clear cache and uninstall packages (optional)
    clear_cache()
    # Uncomment the following line if you want to uninstall packages
    # uninstall_packages()

    # Generate a fresh requirements file and install dependencies
    generate_requirements()
    # Indicate that all packages have been installed and imported successfully
    print("> All packages installed and imported successfully!")
