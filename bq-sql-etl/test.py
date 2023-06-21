import os

jdbc_driver_jar = "./credentials/mssql-jdbc-12.2.0.jre11 (1).jar"

# Get the absolute path
absolute_path = os.path.abspath(jdbc_driver_jar)

# Check if the file exists
if os.path.exists(absolute_path):
    print("The file exists.")

    # Check if the file is a regular file (not a directory)
    if os.path.isfile(absolute_path):
        print("The path points to a file.")
        
        # Check the file permissions
        if os.access(absolute_path, os.R_OK):
            print("The file is readable.")
        else:
            print("The file is not readable.")
    else:
        print("The path does not point to a file.")
else:
    print("The file does not exist or the path is incorrect.")
