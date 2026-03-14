import os
import argparse
import yaml

def lint_directory(config_dir):
    print(f"🔍 Linting YAML files in '{config_dir}/'...\n")
    
    error_count = 0
    file_count = 0

    # 1. Crawl: Walk through the given directory finding all files
    for root, _, files in os.walk(config_dir):
        for file in files:
            # Only check YAML files
            if file.endswith((".yaml", ".yml")):
                file_count += 1
                filepath = os.path.join(root, file)
                
                # 2. Read: Open the file
                with open(filepath, 'r', encoding='utf-8') as f:
                    try:
                        # 3. Parse: Try to load the YAML
                        yaml.safe_load(f)
                    except yaml.YAMLError as exc:
                        # 4. Report: Catch the error and print an actionable hint
                        error_count += 1
                        print(f"❌ Error found in: {filepath}")
                        
                        # PyYAML errors usually tell us the exact line number!
                        if hasattr(exc, 'problem_mark'):
                            mark = exc.problem_mark
                            print(f"   Hint: Check line {mark.line + 1}, column {mark.column + 1}.")
                            print(f"   Details: {exc.problem}\n")
                        else:
                            print(f"   Hint: {exc}\n")

    # Final summary
    if error_count == 0:
        print(f"✅ Success! Checked {file_count} files and found no errors.")
    else:
        print(f"🚨 Failed: Found {error_count} broken config file(s).")
        # Exit with code 1 so automated systems know the command failed
        exit(1)

if __name__ == "__main__":
    # Set up the CLI command using standard Python
    parser = argparse.ArgumentParser(description="Lint YAML configuration files for syntax errors.")
    parser.add_argument(
        "--path", 
        type=str, 
        default="config", 
        help="Path to the config directory (default is 'config')"
    )
    
    args = parser.parse_args()
    lint_directory(args.path)