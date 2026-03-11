import sys
import pkg_resources
import argparse

def check_requirements(requirements_file):
    print(f"🔍 Checking dependencies against {requirements_file}...")
    try:
        with open(requirements_file, 'r') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        # Filter for package names only if needed, but pkg_resources.require handles versions
        pkg_resources.require(requirements)
        print("✅ All dependencies met!")
        return True
    except pkg_resources.DistributionNotFound as e:
        print(f"❌ Missing dependency: {e.req}")
        return False
    except pkg_resources.VersionConflict as e:
        print(f"❌ Version conflict: {e.dist} (required: {e.req})")
        return False
    except Exception as e:
        print(f"⚠️ Error checking requirements: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify Python dependencies.")
    parser.add_argument("--requirements", default="/app/requirements.txt", help="Path to requirements.txt")
    args = parser.parse_args()
    
    if not check_requirements(args.requirements):
        sys.exit(1)
