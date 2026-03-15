import os
import argparse
import yaml

def lint_directory(config_dir):
    # --- FIX 1: Path Validation ---
    if not os.path.isdir(config_dir):
        print(f"🚨 Error: The path '{config_dir}' does not exist or is not a directory.")
        exit(1)

    print(f"🔍 Linting YAML files in '{config_dir}/'...\n")
    
    error_count = 0
    file_count = 0

    for root, _, files in os.walk(config_dir):
        for file in files:
            if file.endswith((".yaml", ".yml")):
                file_count += 1
                filepath = os.path.join(root, file)
                
                # --- FIX 2: File Read Robustness ---
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        yaml.safe_load(f)
                except OSError as e:
                    error_count += 1
                    print(f"❌ IO Error in: {filepath}\n   Details: {e}\n")
                except yaml.YAMLError as exc:
                    error_count += 1
                    print(f"❌ Syntax Error in: {filepath}")
                    if hasattr(exc, 'problem_mark'):
                        mark = exc.problem_mark
                        print(f"   Hint: Check line {mark.line + 1}, column {mark.column + 1}.\n")
                    else:
                        print(f"   Details: {exc}\n")

    if error_count == 0:
        print(f"✅ Success! Checked {file_count} files and found no errors.")
    else:
        print(f"🚨 Failed: Found {error_count} error(s).")
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lint YAML configuration files.")
    parser.add_argument("--path", type=str, default="config", help="Path to config directory")
    args = parser.parse_args()
    lint_directory(args.path)