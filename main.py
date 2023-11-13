import subprocess
import time

def run_scripts(script1_path, script2_path, script3_path):
    while True:  # This loop will keep running indefinitely
        try:
            # Get the current time
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

            # Run the first script
            result1 = subprocess.run(["python", script1_path], check=True, capture_output=True, text=True)
            records1 = result1.stdout.strip()
            # Print the stdout and stderr outputs
            print(result1.stdout)
            print(result1.stderr)


            # Run the second script and capture its output
            result2 = subprocess.run(["python", script2_path], check=True, capture_output=True, text=True)
            output2 = result2.stdout.strip()
            records2 = ""  # Default value

            # Check the output of the second script
            if output2 != "No new posts":
                # Run the third script if the output of the second script is not "no new posts"
                result3 = subprocess.run(["python", script3_path], check=True, capture_output=True, text=True)
                records3 = result3.stdout.strip()
            else:
                print(f"{current_time}: No new posts detected")
                records3 = "0"  # No records extracted from the third script

            # Print the time and number of records extracted only if there are new posts
            if records3 != "0":
                print(f"{current_time}: {records1}, {records3}")

            # Wait for 30 minutes before running the scripts again
            time.sleep(1800)

        except subprocess.CalledProcessError as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    script1_path = "extract_posts.py"
    script2_path = "extract_addresses.py"
    script3_path = "alert.py"

    run_scripts(script1_path, script2_path, script3_path)

